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

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.ClusterProperty;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.JbodReplicaRelocationTest;
import com.linkedin.kafka.cruisecontrol.model.RandomCluster;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.GOAL_VIOLATION;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.REGRESSION;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for optimizing inter-broker goals and intra-broker goals in a single optimization -- i.e. balancing both the
 * brokers and the disks of each broker.
 */
public class DiskAwareRebalanceTest {
  private static final Map<Integer, Integer> RACK_BY_BROKER = Map.of(0, 0, 1, 1, 2, 2);
  private static final List<String> INTER_BROKER_GOALS = List.of(DiskCapacityGoal.class.getSimpleName(),
                                                                 DiskUsageDistributionGoal.class.getSimpleName());
  private static final List<String> INTRA_BROKER_GOALS = List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName(),
                                                                 IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());

  private static KafkaCruiseControlConfig config() {
    return new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  }

  private static GoalOptimizer goalOptimizer(KafkaCruiseControlConfig config) {
    return new GoalOptimizer(config, null, Time.SYSTEM, new MetricRegistry(), EasyMock.mock(Executor.class),
                             EasyMock.mock(AdminClient.class));
  }

  private static List<String> interAndIntraBrokerGoals() {
    List<String> goals = new ArrayList<>(INTER_BROKER_GOALS);
    goals.addAll(INTRA_BROKER_GOALS);
    return goals;
  }

  private static void createReplica(ClusterModel cluster, int brokerId, TopicPartition tp, int index, String logdir) {
    String rackId = RACK_BY_BROKER.get(brokerId).toString();
    cluster.createReplica(rackId, brokerId, tp, index, index == 0, false, logdir, false);
    cluster.setReplicaLoad(rackId, brokerId, tp, getAggregatedMetricValues(1.0, 100.0, index == 0 ? 100.0 : 0.0, 10000.0),
                           Collections.singletonList(1L));
  }

  /**
   * Three brokers with two disks each (see {@link TestConstants#DISK_CAPACITY}), and twelve single-replica partitions
   * with the same size. All replicas reside in {@link TestConstants#LOGDIR0} of their brokers:
   * <ul>
   *   <li>Broker 0: 8 replicas -- i.e. the broker is overloaded.</li>
   *   <li>Broker 1: 2 replicas.</li>
   *   <li>Broker 2: 2 replicas.</li>
   * </ul>
   *
   * @return Cluster model for the tests.
   */
  private static ClusterModel unbalancedJbodCluster() {
    ClusterModel cluster = DeterministicCluster.getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY,
                                                                      TestConstants.DISK_CAPACITY);
    for (int partition = 0; partition < 12; partition++) {
      int brokerId = partition < 8 ? 0 : partition < 10 ? 1 : 2;
      createReplica(cluster, brokerId, new TopicPartition(T1, partition), 0, LOGDIR0);
    }
    return cluster;
  }

  private static Map<Integer, Set<TopicPartition>> partitionsByBroker(ClusterModel cluster) {
    Map<Integer, Set<TopicPartition>> partitionsByBroker = new HashMap<>();
    for (Broker broker : cluster.brokers()) {
      partitionsByBroker.put(broker.id(), broker.replicas().stream().map(Replica::topicPartition).collect(Collectors.toSet()));
    }
    return partitionsByBroker;
  }

  @Test
  public void testInterAndIntraBrokerGoalsInSingleOptimization() throws Exception {
    KafkaCruiseControlConfig config = config();
    ClusterModel clusterModel = unbalancedJbodCluster();
    List<Goal> goals = KafkaCruiseControlUtils.goalsByPriority(interAndIntraBrokerGoals(), config);

    OptimizerResult result = goalOptimizer(config).optimizations(clusterModel, goals, new OperationProgress());

    // Both the brokers and the disks of each broker are balanced.
    assertTrue("Violated goals: " + result.violatedGoalsAfterOptimization(), result.violatedGoalsAfterOptimization().isEmpty());
    JbodReplicaRelocationTest.verifyDiskConsistency(clusterModel);
    // Intra-broker goals do not change the distribution of replicas across brokers set by the inter-broker goals.
    ClusterModel clusterModelWithInterBrokerGoalsOnly = unbalancedJbodCluster();
    goalOptimizer(config).optimizations(clusterModelWithInterBrokerGoalsOnly, KafkaCruiseControlUtils.goalsByPriority(INTER_BROKER_GOALS, config),
                                        new OperationProgress());
    assertEquals(partitionsByBroker(clusterModelWithInterBrokerGoalsOnly), partitionsByBroker(clusterModel));

    // The proposals move replicas both across brokers and across disks of the same broker.
    Set<ExecutionProposal> proposals = result.goalProposals();
    List<ExecutionProposal> interBrokerReplicaMovements = proposals.stream().filter(p -> !p.replicasToAdd().isEmpty())
                                                                   .collect(Collectors.toList());
    List<ExecutionProposal> intraBrokerReplicaMovements = proposals.stream().filter(p -> !p.replicasToMoveBetweenDisksByBroker().isEmpty())
                                                                   .collect(Collectors.toList());
    assertFalse(interBrokerReplicaMovements.isEmpty());
    assertFalse(intraBrokerReplicaMovements.isEmpty());
    // Each replica to add specifies a logdir of its destination broker to be created in.
    for (ExecutionProposal proposal : interBrokerReplicaMovements) {
      for (ReplicaPlacementInfo replicaToAdd : proposal.replicasToAdd()) {
        assertNotNull(proposal.toString(), replicaToAdd.logdir());
        assertNotNull(proposal.toString(), clusterModel.broker(replicaToAdd.brokerId()).disk(replicaToAdd.logdir()));
      }
    }
    Map<String, Object> proposalSummary = result.getProposalSummaryForJson();
    assertEquals(interBrokerReplicaMovements.size(), proposalSummary.get("numReplicaMovements"));
    assertEquals(intraBrokerReplicaMovements.stream().mapToInt(p -> p.replicasToMoveBetweenDisksByBroker().size()).sum(),
                 proposalSummary.get("numIntraBrokerReplicaMovements"));
  }

  @Test
  public void testProposalWithInterBrokerAndIntraBrokerReplicaMovements() {
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY,
                                                                           TestConstants.DISK_CAPACITY);
    // The partition has replicas on LOGDIR0 of broker 0 (i.e. the leader) and broker 1.
    TopicPartition tp = new TopicPartition(T2, 0);
    createReplica(clusterModel, 0, tp, 0, LOGDIR0);
    createReplica(clusterModel, 1, tp, 1, LOGDIR0);
    Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution = clusterModel.getReplicaDistribution();
    Map<TopicPartition, ReplicaPlacementInfo> initialLeaderDistribution = clusterModel.getLeaderDistribution();

    // Move the follower to broker 2 (i.e. to its least utilized disk, LOGDIR0) and the leader to LOGDIR1 of broker 0.
    clusterModel.relocateReplica(tp, 1, 2);
    clusterModel.relocateReplica(tp, 0, LOGDIR1);

    Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(initialReplicaDistribution, initialLeaderDistribution, clusterModel);
    assertEquals(1, proposals.size());
    ExecutionProposal proposal = proposals.iterator().next();
    assertEquals(Set.of(new ReplicaPlacementInfo(2, LOGDIR0)), proposal.replicasToAdd());
    assertEquals(Set.of(new ReplicaPlacementInfo(1, LOGDIR0)), proposal.replicasToRemove());
    assertEquals(Map.of(0, new ReplicaPlacementInfo(0, LOGDIR1)), proposal.replicasToMoveBetweenDisksByBroker());
    JbodReplicaRelocationTest.verifyDiskConsistency(clusterModel);
  }

  @Test
  public void testInterBrokerGoalCannotBePrioritizedAfterIntraBrokerGoal() {
    KafkaCruiseControlConfig config = config();
    List<Goal> goals = KafkaCruiseControlUtils.goalsByPriority(List.of(IntraBrokerDiskUsageDistributionGoal.class.getSimpleName(),
                                                                       DiskUsageDistributionGoal.class.getSimpleName()), config);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
                     () -> goalOptimizer(config).optimizations(unbalancedJbodCluster(), goals, new OperationProgress()));
    assertTrue(exception.getMessage(), exception.getMessage().contains(DiskUsageDistributionGoal.class.getSimpleName()));
    assertTrue(exception.getMessage(), exception.getMessage().contains(IntraBrokerDiskUsageDistributionGoal.class.getSimpleName()));
  }

  @Test
  public void testInterAndIntraBrokerGoalsOnRandomClusters() throws Exception {
    List<String> goalNameByPriority = List.of(ReplicaCapacityGoal.class.getName(),
                                              DiskCapacityGoal.class.getName(),
                                              ReplicaDistributionGoal.class.getName(),
                                              DiskUsageDistributionGoal.class.getName(),
                                              IntraBrokerDiskCapacityGoal.class.getName(),
                                              IntraBrokerDiskUsageDistributionGoal.class.getName());
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(2000L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    balancingConstraint.setResourceBalancePercentage(TestConstants.LOW_BALANCE_PERCENTAGE);
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);

    Map<ClusterProperty, Number> jbodCluster = Map.of(ClusterProperty.POPULATE_REPLICA_PLACEMENT_INFO, 1);
    // Logdirs differ across brokers in these clusters (see TestConstants#JBOD_BROKER_CAPACITY_CONFIG_FILE).
    verifyInterAndIntraBrokerGoalsOnRandomCluster(jbodCluster, Collections.emptySet(), goalNameByPriority, balancingConstraint);
    verifyInterAndIntraBrokerGoalsOnRandomCluster(jbodCluster, Set.of(T1, T2), goalNameByPriority, balancingConstraint);
  }

  private static void verifyInterAndIntraBrokerGoalsOnRandomCluster(Map<ClusterProperty, Number> modifiedProperties,
                                                                    Set<String> excludedTopics,
                                                                    List<String> goalNameByPriority,
                                                                    BalancingConstraint balancingConstraint) throws Exception {
    Map<ClusterProperty, Number> clusterProperties = new HashMap<>(TestConstants.BASE_PROPERTIES);
    clusterProperties.putAll(modifiedProperties);
    ClusterModel clusterModel = RandomCluster.generate(clusterProperties);
    RandomCluster.populate(clusterModel, clusterProperties, TestConstants.Distribution.UNIFORM, true, true, excludedTopics);

    assertTrue("Goals failed to optimize cluster with properties " + modifiedProperties,
               OptimizationVerifier.executeGoalsFor(balancingConstraint, clusterModel, goalNameByPriority, excludedTopics,
                                                    List.of(GOAL_VIOLATION, REGRESSION)));
    JbodReplicaRelocationTest.verifyDiskConsistency(clusterModel);
  }
}
