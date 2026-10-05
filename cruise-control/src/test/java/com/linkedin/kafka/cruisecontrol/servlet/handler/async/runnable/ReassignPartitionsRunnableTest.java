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
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner.LoadMonitorTaskRunnerState;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReassignPartitionsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RequestedPartitionReassignment;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ImpactAnalysisStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Tests the partition reassignment runnable against Kafka metadata and a load model that mirror
 * {@link DeterministicCluster#smallClusterModel(Map)}: T1-0 [0, 2], T1-1 [1, 0], T2-0 [1, 2], T2-1 [0, 2], T2-2 [0, 1] -- the leader
 * is first. Each broker has log directories /a, which hosts its replicas, and /b.
 */
public class ReassignPartitionsRunnableTest {
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  private static final String UUID = "4b5a2b8e-5a1e-4d3e-9a5c-2d7a3f4b1c6d";
  private static final List<Node> NODES = List.of(new Node(0, "host0", 9092, "0"), new Node(1, "host1", 9092, "0"),
                                                  new Node(2, "host2", 9092, "1"));
  private static final TopicPartition T1P0 = new TopicPartition(T1, 0);
  private static final TopicPartition T2P0 = new TopicPartition(T2, 0);
  private static final Map<TopicPartition, List<Integer>> REPLICAS = Map.of(T1P0, List.of(0, 2),
                                                                            new TopicPartition(T1, 1), List.of(1, 0),
                                                                            T2P0, List.of(1, 2),
                                                                            new TopicPartition(T2, 1), List.of(0, 2),
                                                                            new TopicPartition(T2, 2), List.of(0, 1));
  // Promote the follower of T1-0, and move the follower of T2-0 from broker 2 to log directory /b of broker 0.
  private static final List<RequestedPartitionReassignment> REQUESTED =
      List.of(RequestedPartitionReassignment.withLeader(T1P0, 2),
              RequestedPartitionReassignment.withReplicas(T2P0, List.of(1, 0), List.of("any", "/b")));
  private static final String RACK_AWARE_GOAL = "RackAwareGoal";

  private final List<String> _events = new ArrayList<>();
  private final Capture<Set<ExecutionProposal>> _executedProposals = EasyMock.newCapture();
  private final Capture<Map<TopicPartitionReplica, String>> _requestedLogDirs = EasyMock.newCapture();

  @Test
  public void testDryRunDoesNotExecute() throws Exception {
    ReassignPartitionsResult result = runnable(parameters(true, false, REQUESTED), LoadMonitorTaskRunnerState.LOADING,
                                               Collections.emptySet(), () -> KafkaFuture.completedFuture(null)).getResult();

    assertTrue(result.dryRun());
    assertFalse(result.executionStarted());
    assertEquals(2, result.partitions().size());
    assertEquals(ImpactAnalysisStatus.UNAVAILABLE, result.impactAnalysis().status());
    assertEquals(1, result.summary().numLeaderMovements());
    assertEquals(1, result.summary().numInterBrokerReplicaMovements());
    // Neither the executor is reserved, nor the log directories of replicas to add are requested.
    assertTrue(_events.toString(), _events.isEmpty());
    assertFalse(_requestedLogDirs.hasCaptured());
    assertFalse(_executedProposals.hasCaptured());
  }

  @Test
  public void testRefuseExecutionWithUnverifiedImpact() throws Exception {
    ReassignPartitionsRunnable runnable = runnable(parameters(false, false, REQUESTED), LoadMonitorTaskRunnerState.LOADING,
                                                   Collections.emptySet(), () -> KafkaFuture.completedFuture(null));
    String message = assertThrows(UserRequestException.class, runnable::getResult).getMessage();

    assertTrue(message, message.startsWith("Cannot verify the impact of the partition reassignment on hard goals"));
    assertTrue(message, message.contains("skip_hard_goal_check=true"));
    assertEquals(List.of("setGeneratingProposalsForExecution", "failGeneratingProposalsForExecution"), _events);
    assertFalse(_executedProposals.hasCaptured());
  }

  @Test
  public void testRefuseExecutionThatViolatesHardGoals() throws Exception {
    ReassignPartitionsRunnable runnable = runnable(parameters(false, false, REQUESTED), LoadMonitorTaskRunnerState.RUNNING,
                                                   Set.of(RACK_AWARE_GOAL), () -> KafkaFuture.completedFuture(null));
    String message = assertThrows(UserRequestException.class, runnable::getResult).getMessage();

    assertTrue(message, message.startsWith("The partition reassignment would violate hard goals [RackAwareGoal]."));
    assertEquals(List.of("setGeneratingProposalsForExecution", "failGeneratingProposalsForExecution"), _events);
    assertFalse(_executedProposals.hasCaptured());
  }

  @Test
  public void testExecuteSkippingHardGoalCheck() throws Exception {
    // A replica that does not exist yet is expected to be reported as not available.
    ReassignPartitionsResult result = runnable(parameters(false, true, REQUESTED), LoadMonitorTaskRunnerState.RUNNING,
                                               Set.of(RACK_AWARE_GOAL), () -> failedFuture(new ReplicaNotAvailableException("not yet")))
        .getResult();

    assertFalse(result.dryRun());
    assertTrue(result.executionStarted());
    assertEquals(ImpactAnalysisStatus.COMPLETED, result.impactAnalysis().status());
    assertEquals(List.of(RACK_AWARE_GOAL), result.impactAnalysis().introducedHardGoalViolations());
    // The log directories of replicas to add are requested before the execution starts.
    assertEquals(List.of("setGeneratingProposalsForExecution", "alterReplicaLogDirs", "executeProposals"), _events);
    assertEquals(Map.of(new TopicPartitionReplica(T2, 0, 0), "/b"), _requestedLogDirs.getValue());
    Map<TopicPartition, List<Integer>> newReplicas = new HashMap<>();
    _executedProposals.getValue().forEach(p -> newReplicas.put(p.topicPartition(), p.newReplicas().stream().map(r -> r.brokerId())
                                                                                     .collect(Collectors.toList())));
    assertEquals(Map.of(T1P0, List.of(2, 0), T2P0, List.of(1, 0)), newReplicas);
  }

  @Test
  public void testFailToRequestLogDirsOfReplicasToAdd() throws Exception {
    ReassignPartitionsRunnable runnable = runnable(parameters(false, true, REQUESTED), LoadMonitorTaskRunnerState.RUNNING,
                                                   Collections.emptySet(), () -> failedFuture(new KafkaStorageException("disk failure")));
    String message = assertThrows(IllegalStateException.class, runnable::getResult).getMessage();

    assertTrue(message, message.startsWith("Failed to request the log directories of replicas to add"));
    assertTrue(message, message.contains("disk failure"));
    assertEquals(List.of("setGeneratingProposalsForExecution", "alterReplicaLogDirs", "failGeneratingProposalsForExecution"), _events);
    assertFalse(_executedProposals.hasCaptured());
  }

  @Test
  public void testNothingToExecute() throws Exception {
    // T1-0 already has the requested leader.
    ReassignPartitionsResult result = runnable(parameters(false, false, List.of(RequestedPartitionReassignment.withLeader(T1P0, 0))),
                                               LoadMonitorTaskRunnerState.RUNNING, Collections.emptySet(),
                                               () -> KafkaFuture.completedFuture(null)).getResult();

    assertFalse(result.executionStarted());
    assertEquals(ImpactAnalysisStatus.NOT_NEEDED, result.impactAnalysis().status());
    assertEquals(1, result.summary().numPartitionsUnchanged());
    assertEquals(List.of("setGeneratingProposalsForExecution", "failGeneratingProposalsForExecution"), _events);
    assertFalse(_executedProposals.hasCaptured());
  }

  @Test
  public void testInvalidRequests() throws Exception {
    // A request without body may only poll an existing user task -- a new task requires the body.
    ReassignPartitionsRunnable runnable = runnable(parameters(true, false, null), LoadMonitorTaskRunnerState.RUNNING,
                                                   Collections.emptySet(), () -> KafkaFuture.completedFuture(null));
    assertEquals("Missing partition reassignment in the request body.",
                 assertThrows(UserRequestException.class, runnable::getResult).getMessage());

    // Unknown goals are rejected even if the hard goal check is skipped.
    ReassignPartitionsParameters parameters = parameters(false, true, REQUESTED, List.of("NoSuchGoal"));
    runnable = runnable(parameters, LoadMonitorTaskRunnerState.RUNNING, Collections.emptySet(), () -> KafkaFuture.completedFuture(null));
    String message = assertThrows(UserRequestException.class, runnable::getResult).getMessage();
    assertTrue(message, message.startsWith("Goals [NoSuchGoal] are not supported."));

    // Requested goals must include the hard goals unless the hard goal check is skipped.
    parameters = parameters(true, false, REQUESTED, List.of(RACK_AWARE_GOAL));
    runnable = runnable(parameters, LoadMonitorTaskRunnerState.RUNNING, Collections.emptySet(), () -> KafkaFuture.completedFuture(null));
    message = assertThrows(UserRequestException.class, runnable::getResult).getMessage();
    assertTrue(message, message.startsWith("Missing hard goals"));

    // Problems with the requested reassignment are reported as a user error.
    parameters = parameters(true, false, List.of(RequestedPartitionReassignment.withLeader(new TopicPartition("T3", 0), 1)));
    runnable = runnable(parameters, LoadMonitorTaskRunnerState.RUNNING, Collections.emptySet(), () -> KafkaFuture.completedFuture(null));
    message = assertThrows(UserRequestException.class, runnable::getResult).getMessage();
    assertEquals("Cannot reassign partitions (1 problem):\n- T3-0: topic 'T3' does not exist.", message);
    assertTrue(_events.toString(), _events.isEmpty());
  }

  @Test
  public void testFreshClusterOmitsLeadersThatAreNotAlive() throws Exception {
    AdminClient adminClient = adminClient(() -> KafkaFuture.completedFuture(null), List.of(NODES.get(0), NODES.get(1)));
    // T1-0 [0, 2] and T2-0 [1, 2] are led by brokers 0 and 1. Broker 2 is not alive.
    assertEquals(0, ReassignPartitionsRunnable.freshCluster(adminClient, Set.of(T1, T2), 1000L).leaderFor(T1P0).id());
    adminClient = adminClient(() -> KafkaFuture.completedFuture(null), List.of(NODES.get(1), NODES.get(2)));
    assertNull(ReassignPartitionsRunnable.freshCluster(adminClient, Set.of(T1, T2), 1000L).partition(T1P0).leader());
  }

  private Object record(String event) {
    _events.add(event);
    return null;
  }

  private static <T> KafkaFuture<T> failedFuture(Exception exception) {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.completeExceptionally(exception);
    return future;
  }

  private static ReassignPartitionsParameters parameters(boolean dryRun,
                                                         boolean skipHardGoalCheck,
                                                         List<RequestedPartitionReassignment> requested) {
    return parameters(dryRun, skipHardGoalCheck, requested, Collections.emptyList());
  }

  private static ReassignPartitionsParameters parameters(boolean dryRun,
                                                         boolean skipHardGoalCheck,
                                                         List<RequestedPartitionReassignment> requested,
                                                         List<String> goals) {
    ReassignPartitionsParameters parameters = EasyMock.createNiceMock(ReassignPartitionsParameters.class);
    EasyMock.expect(parameters.dryRun()).andStubReturn(dryRun);
    EasyMock.expect(parameters.skipHardGoalCheck()).andStubReturn(skipHardGoalCheck);
    EasyMock.expect(parameters.requestedReassignments()).andStubReturn(requested);
    EasyMock.expect(parameters.goals()).andStubReturn(goals);
    EasyMock.expect(parameters.allowCapacityEstimation()).andStubReturn(true);
    EasyMock.expect(parameters.reason()).andStubReturn("test");
    EasyMock.replay(parameters);
    return parameters;
  }

  /**
   * @param logDirRequestResult The result of requesting the log directory of a replica to add.
   * @param aliveBrokers Alive brokers.
   * @return An admin client that serves the metadata, log directories and (no) ongoing reassignments of the cluster.
   */
  @SuppressWarnings("unchecked")
  private AdminClient adminClient(Supplier<KafkaFuture<Void>> logDirRequestResult, List<Node> aliveBrokers) {
    Map<Integer, Node> nodeById = NODES.stream().collect(Collectors.toMap(Node::id, node -> node));
    Map<String, List<TopicPartitionInfo>> partitionsByTopic = new HashMap<>();
    Map<Integer, Map<TopicPartition, ReplicaInfo>> replicasByBroker = new HashMap<>();
    REPLICAS.entrySet().stream().sorted(Map.Entry.comparingByKey((a, b) -> a.toString().compareTo(b.toString()))).forEach(e -> {
      List<Node> replicas = e.getValue().stream().map(nodeById::get).collect(Collectors.toList());
      partitionsByTopic.computeIfAbsent(e.getKey().topic(), t -> new ArrayList<>())
                       .add(new TopicPartitionInfo(e.getKey().partition(), replicas.get(0), replicas, replicas));
      e.getValue().forEach(b -> replicasByBroker.computeIfAbsent(b, k -> new HashMap<>()).put(e.getKey(), new ReplicaInfo(1024L, 0L, false)));
    });
    Map<String, TopicDescription> topicDescriptions = new HashMap<>();
    partitionsByTopic.forEach((topic, partitions) -> topicDescriptions.put(topic, new TopicDescription(topic, false, partitions)));

    AdminClient adminClient = EasyMock.createMock(AdminClient.class);
    ListTopicsResult listTopicsResult = EasyMock.createNiceMock(ListTopicsResult.class);
    EasyMock.expect(listTopicsResult.names()).andStubReturn(KafkaFuture.completedFuture(Set.of(T1, T2)));
    EasyMock.expect(adminClient.listTopics(EasyMock.anyObject(ListTopicsOptions.class))).andStubReturn(listTopicsResult);
    DescribeClusterResult describeClusterResult = EasyMock.createNiceMock(DescribeClusterResult.class);
    EasyMock.expect(describeClusterResult.nodes()).andStubReturn(KafkaFuture.completedFuture(aliveBrokers));
    EasyMock.expect(describeClusterResult.clusterId()).andStubReturn(KafkaFuture.completedFuture("cluster"));
    EasyMock.expect(adminClient.describeCluster()).andStubReturn(describeClusterResult);
    DescribeTopicsResult describeTopicsResult = EasyMock.createNiceMock(DescribeTopicsResult.class);
    EasyMock.expect(describeTopicsResult.allTopicNames()).andStubReturn(KafkaFuture.completedFuture(topicDescriptions));
    EasyMock.expect(adminClient.describeTopics(EasyMock.<Collection<String>>anyObject())).andStubReturn(describeTopicsResult);
    ListPartitionReassignmentsResult listPartitionReassignmentsResult = EasyMock.createNiceMock(ListPartitionReassignmentsResult.class);
    EasyMock.expect(listPartitionReassignmentsResult.reassignments()).andStubReturn(KafkaFuture.completedFuture(Collections.emptyMap()));
    EasyMock.expect(adminClient.listPartitionReassignments(EasyMock.<Set<TopicPartition>>anyObject()))
            .andStubReturn(listPartitionReassignmentsResult);
    EasyMock.expect(adminClient.describeLogDirs(EasyMock.<Collection<Integer>>anyObject())).andStubAnswer(() -> {
      Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = new HashMap<>();
      for (Integer broker : (Collection<Integer>) EasyMock.getCurrentArguments()[0]) {
        descriptions.put(broker, KafkaFuture.completedFuture(
            Map.of("/a", new LogDirDescription(null, replicasByBroker.getOrDefault(broker, Map.of()), 1L << 40, 1L << 40),
                   "/b", new LogDirDescription(null, Map.of(), 1L << 40, 1L << 40))));
      }
      DescribeLogDirsResult describeLogDirsResult = EasyMock.createNiceMock(DescribeLogDirsResult.class);
      EasyMock.expect(describeLogDirsResult.descriptions()).andStubReturn(descriptions);
      EasyMock.replay(describeLogDirsResult);
      return describeLogDirsResult;
    });
    EasyMock.expect(adminClient.alterReplicaLogDirs(EasyMock.capture(_requestedLogDirs))).andStubAnswer(() -> {
      _events.add("alterReplicaLogDirs");
      Map<TopicPartitionReplica, KafkaFuture<Void>> values = new HashMap<>();
      _requestedLogDirs.getValue().keySet().forEach(replica -> values.put(replica, logDirRequestResult.get()));
      AlterReplicaLogDirsResult alterReplicaLogDirsResult = EasyMock.createNiceMock(AlterReplicaLogDirsResult.class);
      EasyMock.expect(alterReplicaLogDirsResult.values()).andStubReturn(values);
      EasyMock.replay(alterReplicaLogDirsResult);
      return alterReplicaLogDirsResult;
    });
    EasyMock.replay(adminClient, listTopicsResult, describeClusterResult, describeTopicsResult, listPartitionReassignmentsResult);
    return adminClient;
  }

  /**
   * @param parameters Parameters of the request.
   * @param loadMonitorState The state of the load monitor -- the impact cannot be analyzed while it is loading.
   * @param violatedGoalsAfter Goals that the optimizer reports as violated after the reassignment -- none before.
   * @param logDirRequestResult The result of requesting the log directory of a replica to add.
   * @return A partition reassignment runnable that records the calls to reserve and execute the executor in {@link #_events}.
   */
  private ReassignPartitionsRunnable runnable(ReassignPartitionsParameters parameters,
                                              LoadMonitorTaskRunnerState loadMonitorState,
                                              Set<String> violatedGoalsAfter,
                                              Supplier<KafkaFuture<Void>> logDirRequestResult) throws Exception {
    KafkaCruiseControl kafkaCruiseControl = EasyMock.createNiceMock(KafkaCruiseControl.class);
    EasyMock.expect(kafkaCruiseControl.config()).andStubReturn(CONFIG);
    EasyMock.expect(kafkaCruiseControl.adminClient()).andStubReturn(adminClient(logDirRequestResult, NODES));
    EasyMock.expect(kafkaCruiseControl.timeMs()).andStubReturn(1L);
    // The load model.
    EasyMock.expect(kafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject()))
            .andStubReturn(new ModelCompletenessRequirements(1, 0.0, false));
    EasyMock.expect(kafkaCruiseControl.getLoadMonitorTaskRunnerState()).andStubReturn(loadMonitorState);
    EasyMock.expect(kafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject()))
            .andStubReturn(EasyMock.createNiceMock(LoadMonitor.AutoCloseableSemaphore.class));
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.anyLong(), EasyMock.anyLong(), EasyMock.anyObject(), EasyMock.anyBoolean(),
                                                    EasyMock.anyBoolean(), EasyMock.anyObject()))
            .andStubAnswer(() -> DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY));
    EasyMock.expect(kafkaCruiseControl.executorState()).andStubReturn(ExecutorState.noTaskInProgress(Collections.emptySet(),
                                                                                                      Collections.emptySet()));
    EasyMock.expect(kafkaCruiseControl.excludedTopics(EasyMock.anyObject(), EasyMock.isNull())).andStubReturn(Collections.emptySet());
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.isNull(),
                                                     EasyMock.anyObject(OptimizationOptions.class)))
            .andReturn(optimizerResult(violatedGoalsAfter)).andReturn(optimizerResult(Collections.emptySet()));
    // The executor.
    kafkaCruiseControl.setGeneratingProposalsForExecution(EasyMock.eq(UUID), EasyMock.anyObject(), EasyMock.eq(true));
    EasyMock.expectLastCall().andAnswer(() -> record("setGeneratingProposalsForExecution")).anyTimes();
    kafkaCruiseControl.failGeneratingProposalsForExecution(UUID);
    EasyMock.expectLastCall().andAnswer(() -> record("failGeneratingProposalsForExecution")).anyTimes();
    kafkaCruiseControl.executeProposals(EasyMock.capture(_executedProposals), EasyMock.eq(Collections.emptySet()), EasyMock.eq(false),
                                        EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(),
                                        EasyMock.isNull(), EasyMock.isNull(), EasyMock.isNull(), EasyMock.eq(true), EasyMock.eq(UUID),
                                        EasyMock.eq(true));
    EasyMock.expectLastCall().andAnswer(() -> record("executeProposals")).anyTimes();
    EasyMock.replay(kafkaCruiseControl);
    return new ReassignPartitionsRunnable(kafkaCruiseControl, new OperationFuture("Reassign partitions"), UUID, parameters);
  }

  private static OptimizerResult optimizerResult(Set<String> violatedGoalsBeforeOptimization) {
    OptimizerResult optimizerResult = EasyMock.createNiceMock(OptimizerResult.class);
    EasyMock.expect(optimizerResult.violatedGoalsBeforeOptimization()).andStubReturn(violatedGoalsBeforeOptimization);
    EasyMock.replay(optimizerResult);
    return optimizerResult;
  }
}
