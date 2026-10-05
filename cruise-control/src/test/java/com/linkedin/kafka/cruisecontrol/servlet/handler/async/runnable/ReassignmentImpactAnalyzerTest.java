/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.GoalImpact;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.GoalImpactStatus;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ImpactAnalysisStatus;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentImpact;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ReassignmentImpactAnalyzerTest {
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  private static final double DELTA = 1e-6;
  // Replicas of the small cluster model: T1-0 [0, 2], T1-1 [1, 0], T2-0 [1, 2], T2-1 [0, 2], T2-2 [0, 1] -- the leader is first.
  private static final TopicPartition T1P0 = new TopicPartition(T1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(T1, 1);
  private static final TopicPartition T2P0 = new TopicPartition(T2, 0);
  private static final TopicPartition T2P1 = new TopicPartition(T2, 1);
  private static final TopicPartition T2P2 = new TopicPartition(T2, 2);
  private static final String RACK_AWARE_GOAL = "RackAwareGoal";
  private static final String REPLICA_DISTRIBUTION_GOAL = "ReplicaDistributionGoal";

  private static ClusterModel smallClusterModel() {
    return DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
  }

  /**
   * @return A cluster model with brokers 0 and 1 (on separate racks), each with log directories {@link TestConstants#LOGDIR0} and
   * {@link TestConstants#LOGDIR1}, and partition T1-0 with replicas [0, 1] on {@link TestConstants#LOGDIR0}.
   */
  private static ClusterModel jbodClusterModel() {
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(Map.of(0, 0, 1, 1), TestConstants.BROKER_CAPACITY,
                                                                           TestConstants.DISK_CAPACITY);
    clusterModel.createReplica("0", 0, T1P0, 0, true, false, LOGDIR0, false);
    clusterModel.createReplica("1", 1, T1P0, 1, false, false, LOGDIR0, false);
    List<Long> windows = Collections.singletonList(1L);
    clusterModel.setReplicaLoad("0", 0, T1P0, KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(10.0, 20.0, 30.0, 40.0), windows);
    clusterModel.setReplicaLoad("1", 1, T1P0, KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(5.0, 20.0, 0.0, 40.0), windows);
    return clusterModel;
  }

  private static ReplicaPlacementInfo rpi(int broker) {
    return new ReplicaPlacementInfo(broker);
  }

  /**
   * @param tp Topic partition.
   * @param oldReplicas Current replicas -- the leader is first.
   * @param newReplicas New replicas -- the leader is first.
   * @return A proposal to change the replicas of the given partition from the given old replicas to the given new replicas, all on
   * unknown log directories.
   */
  private static ExecutionProposal proposal(TopicPartition tp, List<Integer> oldReplicas, List<Integer> newReplicas) {
    List<ReplicaPlacementInfo> oldPlacements = oldReplicas.stream().map(ReassignmentImpactAnalyzerTest::rpi).collect(Collectors.toList());
    return new ExecutionProposal(tp, 0L, oldPlacements.get(0), oldPlacements,
                                 newReplicas.stream().map(ReassignmentImpactAnalyzerTest::rpi).collect(Collectors.toList()));
  }

  private static double utilization(ClusterModel clusterModel, int broker, Resource resource) {
    return clusterModel.broker(broker).load().expectedUtilizationFor(resource);
  }

  private static List<Integer> replicas(ClusterModel clusterModel, TopicPartition tp) {
    return clusterModel.partition(tp).replicas().stream().map(r -> r.broker().id()).collect(Collectors.toList());
  }

  private static int leader(ClusterModel clusterModel, TopicPartition tp) {
    return clusterModel.partition(tp).leader().broker().id();
  }

  @Test
  public void testApplyLeadershipMovement() {
    ClusterModel clusterModel = smallClusterModel();
    assertEquals(295.0, utilization(clusterModel, 0, Resource.NW_OUT), DELTA);
    assertEquals(0.0, utilization(clusterModel, 2, Resource.NW_OUT), DELTA);

    List<TopicPartition> notModeled = ReassignmentImpactAnalyzer.applyPlan(clusterModel, List.of(proposal(T1P0, List.of(0, 2), List.of(2, 0))),
                                                                           false);
    assertTrue(notModeled.isEmpty());
    assertEquals(2, leader(clusterModel, T1P0));
    // The outbound network load of the leader moves to the new leader.
    assertEquals(165.0, utilization(clusterModel, 0, Resource.NW_OUT), DELTA);
    assertEquals(130.0, utilization(clusterModel, 2, Resource.NW_OUT), DELTA);
  }

  @Test
  public void testApplyReplicaMovement() {
    ClusterModel clusterModel = smallClusterModel();
    assertEquals(280.0, utilization(clusterModel, 0, Resource.DISK), DELTA);
    assertEquals(135.0, utilization(clusterModel, 2, Resource.DISK), DELTA);

    // Move the follower of T2-0 from broker 2 to broker 0, and the leader of T1-1 from broker 1 to broker 2.
    List<TopicPartition> notModeled = ReassignmentImpactAnalyzer.applyPlan(clusterModel,
                                                                           List.of(proposal(T2P0, List.of(1, 2), List.of(1, 0)),
                                                                                   proposal(T1P1, List.of(1, 0), List.of(2, 0))),
                                                                           false);
    assertTrue(notModeled.isEmpty());
    assertEquals(Set.of(1, 0), Set.copyOf(replicas(clusterModel, T2P0)));
    assertEquals(1, leader(clusterModel, T2P0));
    assertEquals(Set.of(2, 0), Set.copyOf(replicas(clusterModel, T1P1)));
    assertEquals(2, leader(clusterModel, T1P1));
    assertEquals(285.0, utilization(clusterModel, 0, Resource.DISK), DELTA);
    assertEquals(185.0, utilization(clusterModel, 2, Resource.DISK), DELTA);
    assertEquals(100.0, utilization(clusterModel, 1, Resource.DISK), DELTA);
    assertEquals(110.0, utilization(clusterModel, 2, Resource.NW_OUT), DELTA);
  }

  @Test
  public void testApplyReplicationFactorChanges() {
    ClusterModel clusterModel = smallClusterModel();
    List<TopicPartition> notModeled = ReassignmentImpactAnalyzer.applyPlan(clusterModel,
                                                                           List.of(proposal(T1P1, List.of(1, 0), List.of(1, 0, 2)),
                                                                                   proposal(T2P2, List.of(0, 1), List.of(0)),
                                                                                   proposal(T2P1, List.of(0, 2), List.of(2))),
                                                                           false);
    assertTrue(notModeled.isEmpty());
    // A replica is added with the load of a follower.
    assertEquals(List.of(1, 0, 2), replicas(clusterModel, T1P1));
    assertEquals(1, leader(clusterModel, T1P1));
    assertEquals(55.0, clusterModel.broker(2).replica(T1P1).load().expectedUtilizationFor(Resource.DISK), DELTA);
    assertEquals(0.0, clusterModel.broker(2).replica(T1P1).load().expectedUtilizationFor(Resource.NW_OUT), DELTA);
    // Replicas are removed, after moving the leadership if needed.
    assertEquals(List.of(0), replicas(clusterModel, T2P2));
    assertEquals(List.of(2), replicas(clusterModel, T2P1));
    assertEquals(2, leader(clusterModel, T2P1));
    assertEquals(60.0, utilization(clusterModel, 1, Resource.DISK), DELTA);
    assertEquals(225.0, utilization(clusterModel, 0, Resource.DISK), DELTA);
    assertEquals(190.0, utilization(clusterModel, 2, Resource.DISK), DELTA);
    assertEquals(3, clusterModel.maxReplicationFactor());
  }

  @Test
  public void testPartitionsNotModeled() {
    ClusterModel clusterModel = smallClusterModel();
    BrokerStats loadBefore = clusterModel.brokerStats(CONFIG);
    TopicPartition unknownPartition = new TopicPartition("T3", 0);
    List<TopicPartition> notModeled = ReassignmentImpactAnalyzer.applyPlan(clusterModel,
                                                                           // The replicas in the model differ.
                                                                           List.of(proposal(T1P0, List.of(0, 1), List.of(1, 0)),
                                                                                   // The new broker is not in the model.
                                                                                   proposal(T2P0, List.of(1, 2), List.of(1, 7)),
                                                                                   // The partition is not in the model.
                                                                                   proposal(unknownPartition, List.of(0, 1), List.of(1, 0))),
                                                                           false);
    assertEquals(List.of(T1P0, T2P0, unknownPartition), notModeled);
    assertEquals(loadBefore.getJsonStructure(), clusterModel.brokerStats(CONFIG).getJsonStructure());
  }

  @Test
  public void testApplyIntraBrokerReplicaMovement() {
    ExecutionProposal diskMove = new ExecutionProposal(T1P0, 0L, new ReplicaPlacementInfo(0, LOGDIR0),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR0), new ReplicaPlacementInfo(1, LOGDIR0)),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR1), new ReplicaPlacementInfo(1, LOGDIR0)));
    // A broker-level analysis does not move replicas between log directories.
    ClusterModel clusterModel = jbodClusterModel();
    assertTrue(ReassignmentImpactAnalyzer.applyPlan(clusterModel, List.of(diskMove), false).isEmpty());
    assertEquals(LOGDIR0, clusterModel.broker(0).replica(T1P0).disk().logDir());

    clusterModel = jbodClusterModel();
    assertTrue(ReassignmentImpactAnalyzer.applyPlan(clusterModel, List.of(diskMove), true).isEmpty());
    assertEquals(LOGDIR1, clusterModel.broker(0).replica(T1P0).disk().logDir());
    assertEquals(0.0, clusterModel.broker(0).disk(LOGDIR0).utilization(), DELTA);
    assertEquals(40.0, clusterModel.broker(0).disk(LOGDIR1).utilization(), DELTA);
    assertEquals(40.0, clusterModel.broker(1).disk(LOGDIR0).utilization(), DELTA);

    // A log directory that is not in the model.
    ExecutionProposal unknownLogDir = new ExecutionProposal(T1P0, 0L, new ReplicaPlacementInfo(0, LOGDIR0),
                                                            List.of(new ReplicaPlacementInfo(0, LOGDIR0), new ReplicaPlacementInfo(1, LOGDIR0)),
                                                            List.of(new ReplicaPlacementInfo(0, "/mnt/i02"), new ReplicaPlacementInfo(1, LOGDIR0)));
    clusterModel = jbodClusterModel();
    assertEquals(List.of(T1P0), ReassignmentImpactAnalyzer.applyPlan(clusterModel, List.of(unknownLogDir), true));
    assertEquals(LOGDIR0, clusterModel.broker(0).replica(T1P0).disk().logDir());
  }

  @Test
  public void testAnalyzeNothingToChange() throws Exception {
    KafkaCruiseControl kafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.replay(kafkaCruiseControl);
    ReassignmentImpact impact = new ReassignmentImpactAnalyzer(kafkaCruiseControl, new OperationProgress(), true)
        .analyze(Collections.emptyList(), false, Collections.emptyList());
    assertEquals(ImpactAnalysisStatus.NOT_NEEDED, impact.status());
    EasyMock.verify(kafkaCruiseControl);
  }

  @Test
  public void testAnalyzeIsUnavailableWhileLoadMonitorIsLoading() throws Exception {
    KafkaCruiseControl kafkaCruiseControl = mockKafkaCruiseControl(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING, false,
                                                                   Collections.emptySet(), Collections.emptySet());
    ReassignmentImpact impact = new ReassignmentImpactAnalyzer(kafkaCruiseControl, new OperationProgress(), true)
        .analyze(List.of(proposal(T1P0, List.of(0, 2), List.of(2, 0))), false, List.of(RACK_AWARE_GOAL));
    assertEquals(ImpactAnalysisStatus.UNAVAILABLE, impact.status());
    assertTrue(impact.reason(), impact.reason().contains("LOADING"));
    assertTrue(impact.goalSummary().isEmpty());
    assertTrue(impact.introducedHardGoalViolations().isEmpty());
  }

  @Test
  public void testAnalyzeGoalSummary() throws Exception {
    KafkaCruiseControl kafkaCruiseControl = mockKafkaCruiseControl(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING, false,
                                                                   Set.of(REPLICA_DISTRIBUTION_GOAL), Set.of(RACK_AWARE_GOAL));
    ReassignmentImpact impact = new ReassignmentImpactAnalyzer(kafkaCruiseControl, new OperationProgress(), true)
        .analyze(List.of(proposal(T1P0, List.of(0, 2), List.of(2, 0))), false, List.of(RACK_AWARE_GOAL, REPLICA_DISTRIBUTION_GOAL));

    assertEquals(impact.reason(), ImpactAnalysisStatus.COMPLETED, impact.status());
    assertTrue(impact.notes().isEmpty());
    assertTrue(impact.partitionsNotModeled().isEmpty());
    List<GoalImpact> goalSummary = impact.goalSummary();
    assertEquals(2, goalSummary.size());
    assertEquals(RACK_AWARE_GOAL, goalSummary.get(0).goal());
    assertTrue(goalSummary.get(0).hardGoal());
    assertEquals(GoalImpactStatus.VIOLATION_INTRODUCED, goalSummary.get(0).status());
    assertEquals(REPLICA_DISTRIBUTION_GOAL, goalSummary.get(1).goal());
    assertFalse(goalSummary.get(1).hardGoal());
    assertEquals(GoalImpactStatus.VIOLATION_FIXED, goalSummary.get(1).status());
    assertEquals(List.of(RACK_AWARE_GOAL), impact.introducedHardGoalViolations());

    // The leadership of T1-0 moves from broker 0 to broker 2.
    assertEquals(295.0, nwOutRate(impact.loadBeforeReassignment(), 0), DELTA);
    assertEquals(165.0, nwOutRate(impact.loadAfterReassignment(), 0), DELTA);
    assertEquals(130.0, nwOutRate(impact.loadAfterReassignment(), 2), DELTA);
    Map<String, Object> json = impact.getJsonStructure();
    assertEquals(List.of(RACK_AWARE_GOAL, REPLICA_DISTRIBUTION_GOAL), json.get("goals"));
    assertEquals(90.0, (Double) json.get("onDemandBalancednessScoreBefore"), DELTA);
    assertEquals(80.0, (Double) json.get("onDemandBalancednessScoreAfter"), DELTA);
  }

  @Test
  public void testAnalyzeFallsBackToBrokerLevelModel() throws Exception {
    // The model has no capacity per log directory -- i.e. a disk-level model cannot be built.
    KafkaCruiseControl kafkaCruiseControl = mockKafkaCruiseControl(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING, true,
                                                                   Collections.emptySet(), Collections.emptySet());
    ExecutionProposal diskMove = new ExecutionProposal(T1P0, 0L, new ReplicaPlacementInfo(0, LOGDIR0),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR0), new ReplicaPlacementInfo(2, LOGDIR0)),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR1), new ReplicaPlacementInfo(2, LOGDIR0)));
    ReassignmentImpact impact = new ReassignmentImpactAnalyzer(kafkaCruiseControl, new OperationProgress(), true)
        .analyze(List.of(diskMove), true, Collections.emptyList());

    assertEquals(impact.reason(), ImpactAnalysisStatus.COMPLETED, impact.status());
    assertEquals(2, impact.notes().size());
    assertTrue(impact.notes().get(0), impact.notes().get(0).startsWith("Disk-level analysis is unavailable (no disk capacity)"));
    assertEquals("The placement of replicas on log directories is not modeled at broker level.", impact.notes().get(1));
    // The broker-level analysis uses the default goals.
    assertEquals(TestConstants.DEFAULT_GOALS_VALUES.split(",").length, impact.goalSummary().size());
    assertTrue(impact.goalSummary().stream().allMatch(goalImpact -> goalImpact.status() == GoalImpactStatus.OK));
  }

  @SuppressWarnings("unchecked")
  private static double nwOutRate(BrokerStats brokerStats, int broker) {
    List<Map<String, Object>> brokers = (List<Map<String, Object>>) brokerStats.getJsonStructure().get("brokers");
    return brokers.stream().filter(stats -> stats.get("Broker").equals(broker)).map(stats -> (Double) stats.get("NwOutRate"))
                  .findFirst().orElseThrow();
  }

  /**
   * @param loadMonitorState The state of the load monitor.
   * @param noDiskCapacity {@code true} if a disk-level model cannot be built, {@code false} otherwise.
   * @param violatedGoalsBefore Goals that the optimizer reports as violated before the reassignment.
   * @param violatedGoalsAfter Goals that the optimizer reports as violated after the reassignment.
   * @return A Kafka Cruise Control instance that serves the small cluster model and the given optimization results.
   */
  private static KafkaCruiseControl mockKafkaCruiseControl(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState loadMonitorState,
                                                           boolean noDiskCapacity,
                                                           Set<String> violatedGoalsBefore,
                                                           Set<String> violatedGoalsAfter) throws Exception {
    KafkaCruiseControl kafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    EasyMock.expect(kafkaCruiseControl.config()).andStubReturn(CONFIG);
    EasyMock.expect(kafkaCruiseControl.timeMs()).andStubReturn(1L);
    EasyMock.expect(kafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject()))
            .andStubReturn(new ModelCompletenessRequirements(1, 0.0, false));
    EasyMock.expect(kafkaCruiseControl.getLoadMonitorTaskRunnerState()).andStubReturn(loadMonitorState);
    EasyMock.expect(kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject()))
            .andStubReturn(EasyMock.createNiceMock(LoadMonitor.AutoCloseableSemaphore.class));
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.anyLong(), EasyMock.anyLong(), EasyMock.anyObject(), EasyMock.anyBoolean(),
                                                    EasyMock.anyBoolean(), EasyMock.anyObject())).andStubAnswer(() -> {
      if (noDiskCapacity && (Boolean) EasyMock.getCurrentArguments()[3]) {
        throw new IllegalStateException("no disk capacity");
      }
      return smallClusterModel();
    });
    EasyMock.expect(kafkaCruiseControl.executorState()).andStubReturn(ExecutorState.noTaskInProgress(Collections.emptySet(),
                                                                                                      Collections.emptySet()));
    EasyMock.expect(kafkaCruiseControl.excludedTopics(EasyMock.anyObject(), EasyMock.isNull())).andStubReturn(Collections.emptySet());
    // The impact analysis optimizes the model after the reassignment first.
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.isNull(),
                                                     EasyMock.anyObject(OptimizationOptions.class)))
            .andReturn(optimizerResult(violatedGoalsAfter, 80.0)).andReturn(optimizerResult(violatedGoalsBefore, 90.0));
    EasyMock.replay(kafkaCruiseControl);
    return kafkaCruiseControl;
  }

  private static OptimizerResult optimizerResult(Set<String> violatedGoalsBeforeOptimization, double onDemandBalancednessScoreBefore) {
    OptimizerResult optimizerResult = EasyMock.createNiceMock(OptimizerResult.class);
    EasyMock.expect(optimizerResult.violatedGoalsBeforeOptimization()).andStubReturn(violatedGoalsBeforeOptimization);
    EasyMock.expect(optimizerResult.onDemandBalancednessScoreBefore()).andStubReturn(onDemandBalancednessScoreBefore);
    EasyMock.replay(optimizerResult);
    return optimizerResult;
  }
}
