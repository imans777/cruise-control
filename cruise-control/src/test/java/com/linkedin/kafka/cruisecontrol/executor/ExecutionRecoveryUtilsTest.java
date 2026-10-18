/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionRecoveryUtils.RecoveryAction;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionRecoveryUtils.RecoveryPlan;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionState;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedTask;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ExecutionRecoveryUtilsTest {
  private static final List<Node> NODES = Arrays.asList(new Node(0, "h0", 9092), new Node(1, "h1", 9092),
                                                        new Node(2, "h2", 9092), new Node(3, "h3", 9092));
  private static final TopicPartition TP = new TopicPartition("topic", 0);
  private static final String UUID = "uuid";

  // Moves the replica on broker 1 to broker 2 -- i.e. [0, 1] -> [0, 2].
  private static ExecutionProposal interBrokerProposal() {
    return new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0),
                                 Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                 Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
  }

  // Moves the leadership from broker 0 to broker 1 -- i.e. [0, 1] -> [1, 0].
  private static ExecutionProposal leadershipProposal() {
    return new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0),
                                 Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                 Arrays.asList(new ReplicaPlacementInfo(1), new ReplicaPlacementInfo(0)));
  }

  // Moves the replica on broker 1 from /d1 to /d2.
  private static ExecutionProposal intraBrokerProposal() {
    return new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, "/d1"),
                                 Arrays.asList(new ReplicaPlacementInfo(0, "/d1"), new ReplicaPlacementInfo(1, "/d1")),
                                 Arrays.asList(new ReplicaPlacementInfo(0, "/d1"), new ReplicaPlacementInfo(1, "/d2")));
  }

  private static PersistedExecutionState state(ExecutionProposal proposal,
                                               ExecutorState.State executorState,
                                               PersistedTask... tasks) {
    return new PersistedExecutionState.Builder(UUID, PersistedExecutionState.Operation.EXECUTE_PROPOSALS)
        .executorState(executorState)
        .proposals(Collections.singletonList(proposal))
        .tasks(Arrays.asList(tasks))
        .build();
  }

  private static PersistedTask task(ExecutionTask.TaskType type, ExecutionTaskState state) {
    return new PersistedTask(type, TP, type == ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION ? 1 : PersistedTask.NO_BROKER_ID,
                             state, -1L, -1L);
  }

  private static Cluster cluster(Integer... replicas) {
    Node[] replicaNodes = Arrays.stream(replicas).map(NODES::get).toArray(Node[]::new);
    PartitionInfo partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), replicaNodes[0], replicaNodes, replicaNodes);
    return new Cluster("cluster", NODES, Collections.singleton(partitionInfo), Collections.emptySet(), Collections.emptySet());
  }

  private static Cluster emptyCluster() {
    return new Cluster("cluster", NODES, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
  }

  private static Map<TopicPartition, PartitionReassignment> reassigning(TopicPartition tp, List<Integer> targetReplicas,
                                                                        List<Integer> removingReplicas) {
    List<Integer> replicas = new ArrayList<>(targetReplicas);
    removingReplicas.stream().filter(r -> !replicas.contains(r)).forEach(replicas::add);
    List<Integer> adding = new ArrayList<>(targetReplicas);
    adding.removeAll(removingReplicas);
    return Collections.singletonMap(tp, new PartitionReassignment(replicas, adding, removingReplicas));
  }

  private static RecoveryAction interBrokerAction(ExecutionTaskState persistedState,
                                                  Cluster cluster,
                                                  Map<TopicPartition, PartitionReassignment> ongoing) {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, persistedState);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster, ongoing, Collections.emptyMap());
    return plan.actionByTask().get(task);
  }

  @Test
  public void testInterBrokerTaskInProgressAndStillOngoingIsAdopted() {
    // The partition is being reassigned towards the new replicas [0, 2] -- i.e. adding 2, removing 1.
    Map<TopicPartition, PartitionReassignment> ongoing = reassigning(TP, Arrays.asList(0, 2), Collections.singletonList(1));
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 2, 1), ongoing,
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.ADOPT, plan.actionByTask().get(task));
    assertEquals(Set.of(TP), plan.adoptedInterBrokerPartitions());
    assertEquals(Set.of(TP), plan.inFlightInterBrokerPartitions());
    assertTrue(plan.unexpectedOngoingReassignments().isEmpty());
    assertEquals(1, plan.proposalsToResume().size());
    assertNull(plan.reasonToNotResume());
    // A task that was persisted as pending right before being submitted is also adopted.
    assertEquals(RecoveryAction.ADOPT, interBrokerAction(ExecutionTaskState.PENDING, cluster(0, 2, 1), ongoing));
  }

  @Test
  public void testInterBrokerTaskInProgressAndDoneIsCompleted() {
    assertEquals(RecoveryAction.COMPLETED, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(0, 2), Collections.emptyMap()));
    assertEquals(RecoveryAction.COMPLETED, interBrokerAction(ExecutionTaskState.COMPLETED, cluster(0, 2), Collections.emptyMap()));
  }

  @Test
  public void testInterBrokerTaskInProgressButNotStartedIsPending() {
    // E.g. the reassignment was never accepted by Kafka or was rolled back.
    assertEquals(RecoveryAction.PENDING, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(0, 1), Collections.emptyMap()));
    assertEquals(RecoveryAction.PENDING, interBrokerAction(ExecutionTaskState.PENDING, cluster(0, 1), Collections.emptyMap()));
    // Only the replica order remains to be changed.
    assertEquals(RecoveryAction.PENDING, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(2, 0), Collections.emptyMap()));
  }

  @Test
  public void testInterBrokerTaskOfModifiedOrDeletedPartitionIsDropped() {
    assertEquals(RecoveryAction.DROPPED, interBrokerAction(ExecutionTaskState.IN_PROGRESS, cluster(0, 3), Collections.emptyMap()));
    assertEquals(RecoveryAction.DROPPED, interBrokerAction(ExecutionTaskState.PENDING, emptyCluster(), Collections.emptyMap()));
    // Reassigned by someone else towards other replicas.
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 3, 1),
                                                         reassigning(TP, Arrays.asList(0, 3), Collections.singletonList(1)),
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.DROPPED, plan.actionByTask().get(task));
    assertEquals(Set.of(TP), plan.unexpectedOngoingReassignments());
    assertNotNull(plan.reasonToNotResume());
  }

  @Test
  public void testInterBrokerTaskBeingRolledBack() {
    Map<TopicPartition, PartitionReassignment> ongoing = reassigning(TP, Arrays.asList(0, 1), Collections.singletonList(2));
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.DEAD);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 1, 2), ongoing,
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.ROLLBACK_IN_FLIGHT, plan.actionByTask().get(task));
    assertEquals(Set.of(TP), plan.inFlightInterBrokerPartitions());
    assertTrue(plan.unexpectedOngoingReassignments().isEmpty());
    assertNotNull(plan.reasonToNotResume());
    assertEquals(RecoveryAction.FINISHED_WITH_ERROR, interBrokerAction(ExecutionTaskState.ABORTED, cluster(0, 1), Collections.emptyMap()));
  }

  @Test
  public void testUnexpectedOngoingReassignmentPreventsResume() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    TopicPartition otherTp = new TopicPartition("other", 0);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 1),
                                                         reassigning(otherTp, Arrays.asList(0, 1), Collections.emptyList()),
                                                         Collections.emptyMap());
    assertEquals(RecoveryAction.PENDING, plan.actionByTask().get(task));
    assertEquals(Set.of(otherTp), plan.unexpectedOngoingReassignments());
    assertNotNull(plan.reasonToNotResume());
  }

  @Test
  public void testStoppingExecutionIsNotResumed() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.PENDING);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), ExecutorState.State.STOPPING_EXECUTION, task),
                                                         cluster(0, 1), Collections.emptyMap(), Collections.emptyMap());
    assertEquals(RecoveryAction.PENDING, plan.actionByTask().get(task));
    assertNotNull(plan.reasonToNotResume());
  }

  @Test
  public void testNothingToResume() {
    PersistedTask task = task(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, ExecutionTaskState.IN_PROGRESS);
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(interBrokerProposal(), null, task), cluster(0, 2),
                                                         Collections.emptyMap(), Collections.emptyMap());
    assertTrue(plan.proposalsToResume().isEmpty());
    assertNotNull(plan.reasonToNotResume());
    assertEquals(Integer.valueOf(1), plan.numTasksByAction().get(RecoveryAction.COMPLETED));
  }

  @Test
  public void testLeadershipTask() {
    PersistedTask task = task(ExecutionTask.TaskType.LEADER_ACTION, ExecutionTaskState.IN_PROGRESS);
    PersistedExecutionState state = state(leadershipProposal(), null, task);
    // Leadership moved.
    assertEquals(RecoveryAction.COMPLETED, ExecutionRecoveryUtils.reconcile(state, cluster(1, 0), Collections.emptyMap(),
                                                                            Collections.emptyMap()).actionByTask().get(task));
    // Leadership not moved -- election is retried.
    assertEquals(RecoveryAction.PENDING, ExecutionRecoveryUtils.reconcile(state, cluster(0, 1), Collections.emptyMap(),
                                                                          Collections.emptyMap()).actionByTask().get(task));
    assertEquals(RecoveryAction.DROPPED, ExecutionRecoveryUtils.reconcile(state, emptyCluster(), Collections.emptyMap(),
                                                                          Collections.emptyMap()).actionByTask().get(task));
  }

  private static ReplicaLogDirInfo logDirInfo(String current, String future) {
    ReplicaLogDirInfo info = EasyMock.mock(ReplicaLogDirInfo.class);
    EasyMock.expect(info.getCurrentReplicaLogDir()).andReturn(current).anyTimes();
    EasyMock.expect(info.getFutureReplicaLogDir()).andReturn(future).anyTimes();
    EasyMock.replay(info);
    return info;
  }

  private static RecoveryAction intraBrokerAction(ExecutionTaskState persistedState, ReplicaLogDirInfo info) {
    PersistedTask task = task(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, persistedState);
    Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfo = new HashMap<>();
    if (info != null) {
      logDirInfo.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 1), info);
    }
    RecoveryPlan plan = ExecutionRecoveryUtils.reconcile(state(intraBrokerProposal(), null, task), cluster(0, 1), Collections.emptyMap(),
                                                         logDirInfo);
    if (plan.actionByTask().get(task) == RecoveryAction.ADOPT) {
      assertEquals(Set.of(new TopicPartitionReplica(TP.topic(), TP.partition(), 1)), plan.adoptedIntraBrokerReplicas());
    }
    return plan.actionByTask().get(task);
  }

  @Test
  public void testIntraBrokerTask() {
    assertEquals(RecoveryAction.ADOPT, intraBrokerAction(ExecutionTaskState.IN_PROGRESS, logDirInfo("/d1", "/d2")));
    assertEquals(RecoveryAction.COMPLETED, intraBrokerAction(ExecutionTaskState.IN_PROGRESS, logDirInfo("/d2", null)));
    assertEquals(RecoveryAction.PENDING, intraBrokerAction(ExecutionTaskState.IN_PROGRESS, logDirInfo("/d1", null)));
    assertEquals(RecoveryAction.PENDING, intraBrokerAction(ExecutionTaskState.PENDING, null));
    assertEquals(RecoveryAction.FINISHED_WITH_ERROR, intraBrokerAction(ExecutionTaskState.DEAD, logDirInfo("/d1", null)));
  }

  @Test
  public void testReplicaMovementStrategy() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                      PostponeUrpReplicaMovementStrategy.class.getName() + "," + PrioritizeLargeReplicaMovementStrategy.class.getName());
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    assertNull(ExecutionRecoveryUtils.replicaMovementStrategy(null, config));
    assertNull(ExecutionRecoveryUtils.replicaMovementStrategy("UnknownStrategy", config));
    ReplicaMovementStrategy strategy =
        ExecutionRecoveryUtils.replicaMovementStrategy("PostponeUrpReplicaMovementStrategy,PrioritizeLargeReplicaMovementStrategy", config);
    assertEquals("PostponeUrpReplicaMovementStrategy,PrioritizeLargeReplicaMovementStrategy,BaseReplicaMovementStrategy", strategy.name());
    ReplicaMovementStrategy chainedName = ExecutionRecoveryUtils.replicaMovementStrategy(strategy.name(), config);
    assertEquals(strategy.name(), chainedName.name());
  }

  @Test
  public void testResumedReason() {
    String resumedReason = ExecutionRecoveryUtils.resumedReason("rebalance");
    assertEquals(ExecutionRecoveryUtils.RESUMED_REASON_PREFIX + "rebalance", resumedReason);
    assertEquals(resumedReason, ExecutionRecoveryUtils.resumedReason(resumedReason));
  }
}
