/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedExecutionState;
import com.linkedin.kafka.cruisecontrol.executor.persistence.PersistedTask;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;


/**
 * Utilities to recover an execution that was interrupted by a Cruise Control restart.
 *
 * The persisted execution state tells what Cruise Control <i>was doing</i> when it stopped, and the Kafka cluster tells
 * what is <i>actually happening now</i>. {@link #reconcile(PersistedExecutionState, Cluster, Map, Map)} combines both to
 * decide what to do with each persisted task (see {@link RecoveryAction}).
 */
final class ExecutionRecoveryUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionRecoveryUtils.class);
  static final String RESUMED_REASON_PREFIX = "Resumed after Cruise Control restart: ";

  private ExecutionRecoveryUtils() {
  }

  /**
   * What to do with a persisted task upon recovery.
   */
  enum RecoveryAction {
    /** The task was already done before the restart, or got done in the meantime -- nothing to do. */
    COMPLETED,
    /** The task was submitted to Kafka before the restart and Kafka is still executing it -- only monitor it, do not resubmit. */
    ADOPT,
    /** The task has not been started (or its effect is not visible in the cluster) -- execute it if the execution is resumed. */
    PENDING,
    /** The task was being rolled back before the restart and Kafka is still executing the rollback -- wait for it. */
    ROLLBACK_IN_FLIGHT,
    /** The task was aborted or dead before the restart -- it will not be retried. */
    FINISHED_WITH_ERROR,
    /** The partition was deleted, or modified by someone else since the task was generated -- the task is dropped. */
    DROPPED
  }

  /**
   * The result of reconciling a persisted execution state with the current state of the cluster.
   */
  static final class RecoveryPlan {
    private final PersistedExecutionState _state;
    private final Map<PersistedTask, RecoveryAction> _actionByTask;
    private final Set<TopicPartition> _unexpectedOngoingReassignments;

    RecoveryPlan(PersistedExecutionState state,
                 Map<PersistedTask, RecoveryAction> actionByTask,
                 Set<TopicPartition> unexpectedOngoingReassignments) {
      _state = state;
      _actionByTask = Collections.unmodifiableMap(actionByTask);
      _unexpectedOngoingReassignments = Collections.unmodifiableSet(unexpectedOngoingReassignments);
    }

    PersistedExecutionState state() {
      return _state;
    }

    Map<PersistedTask, RecoveryAction> actionByTask() {
      return _actionByTask;
    }

    /**
     * @return Ongoing partition reassignments in the cluster that are not part of the persisted execution.
     */
    Set<TopicPartition> unexpectedOngoingReassignments() {
      return _unexpectedOngoingReassignments;
    }

    private Set<PersistedTask> tasksWith(ExecutionTask.TaskType type, RecoveryAction action) {
      return _actionByTask.entrySet().stream()
                          .filter(e -> e.getKey().type() == type && e.getValue() == action)
                          .map(Map.Entry::getKey)
                          .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return Partitions whose inter-broker replica reassignment was submitted before the restart and is still ongoing.
     */
    Set<TopicPartition> adoptedInterBrokerPartitions() {
      return tasksWith(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, RecoveryAction.ADOPT)
          .stream().map(PersistedTask::topicPartition).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return Replicas whose intra-broker movement was submitted before the restart and is still ongoing.
     */
    Set<TopicPartitionReplica> adoptedIntraBrokerReplicas() {
      return tasksWith(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, RecoveryAction.ADOPT)
          .stream().map(t -> new TopicPartitionReplica(t.topicPartition().topic(), t.topicPartition().partition(), t.brokerId()))
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return Partitions of the persisted execution with an ongoing partition reassignment (i.e. adopted or being rolled back).
     */
    Set<TopicPartition> inFlightInterBrokerPartitions() {
      Set<TopicPartition> inFlight = adoptedInterBrokerPartitions();
      tasksWith(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, RecoveryAction.ROLLBACK_IN_FLIGHT)
          .forEach(t -> inFlight.add(t.topicPartition()));
      return inFlight;
    }

    /**
     * @return Proposals with at least one task to be executed or monitored if the execution is resumed.
     */
    List<ExecutionProposal> proposalsToResume() {
      Set<TopicPartition> partitions = new LinkedHashSet<>();
      _actionByTask.forEach((task, action) -> {
        if (action == RecoveryAction.PENDING || action == RecoveryAction.ADOPT) {
          partitions.add(task.topicPartition());
        }
      });
      return partitions.stream().map(tp -> _state.proposals().get(tp)).collect(Collectors.toList());
    }

    /**
     * @return {@code null} if the execution can be resumed, or the reason why it cannot be resumed otherwise.
     */
    String reasonToNotResume() {
      if (_state.executorState() == ExecutorState.State.STOPPING_EXECUTION) {
        return "the execution was being stopped when Cruise Control stopped";
      }
      if (!_unexpectedOngoingReassignments.isEmpty()) {
        return "there are ongoing partition reassignments that are not part of the execution: " + _unexpectedOngoingReassignments;
      }
      if (_actionByTask.containsValue(RecoveryAction.ROLLBACK_IN_FLIGHT)) {
        return "there are ongoing rollbacks of tasks of the execution";
      }
      if (proposalsToResume().isEmpty()) {
        return "there are no remaining tasks to execute";
      }
      return null;
    }

    /**
     * @return Number of persisted tasks by their recovery action.
     */
    Map<RecoveryAction, Integer> numTasksByAction() {
      Map<RecoveryAction, Integer> numTasksByAction = new EnumMap<>(RecoveryAction.class);
      for (RecoveryAction action : RecoveryAction.values()) {
        numTasksByAction.put(action, 0);
      }
      _actionByTask.values().forEach(action -> numTasksByAction.merge(action, 1, Integer::sum));
      return numTasksByAction;
    }
  }

  /**
   * Decide what to do with each task of the given persisted execution state based on the current state of the cluster.
   *
   * <ul>
   *   <li>Inter-broker replica tasks: A task whose partition has an ongoing reassignment towards the new replicas of the
   *   proposal is {@link RecoveryAction#ADOPT adopted}; a task whose partition has the new replicas (in order) is
   *   {@link RecoveryAction#COMPLETED}; a task whose partition still has the old replicas (or the new replicas in a
   *   different order) is {@link RecoveryAction#PENDING}.</li>
   *   <li>Intra-broker replica tasks: A task whose replica has a future replica on the target logdir is adopted; a task whose
   *   replica is on the target logdir is completed; otherwise, it is pending.</li>
   *   <li>Leadership tasks: A task whose partition has the new leader is completed; otherwise, it is pending.</li>
   * </ul>
   * Any other case indicates that the partition was modified by someone else since the task was generated, so the task
   * is {@link RecoveryAction#DROPPED}. Tasks that were aborted or dead before the restart are never retried.
   *
   * @param state Persisted execution state.
   * @param cluster Current cluster metadata.
   * @param ongoingReassignments Ongoing partition reassignments in the cluster.
   * @param logDirInfoByReplica Current log directory information of the replicas of intra-broker replica tasks (may be
   *                            partial -- a task with unknown log directory information is considered as pending).
   * @return The recovery plan.
   */
  static RecoveryPlan reconcile(PersistedExecutionState state,
                                Cluster cluster,
                                Map<TopicPartition, PartitionReassignment> ongoingReassignments,
                                Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfoByReplica) {
    Map<PersistedTask, RecoveryAction> actionByTask = new LinkedHashMap<>();
    Set<TopicPartition> expectedOngoingReassignments = new HashSet<>();
    for (PersistedTask task : state.tasks()) {
      ExecutionProposal proposal = state.proposals().get(task.topicPartition());
      RecoveryAction action;
      switch (task.type()) {
        case INTER_BROKER_REPLICA_ACTION:
          action = reconcileInterBrokerReplicaTask(task, proposal, cluster, ongoingReassignments.get(task.topicPartition()));
          if (action == RecoveryAction.ADOPT || action == RecoveryAction.ROLLBACK_IN_FLIGHT) {
            expectedOngoingReassignments.add(task.topicPartition());
          }
          break;
        case INTRA_BROKER_REPLICA_ACTION:
          action = reconcileIntraBrokerReplicaTask(task, proposal, cluster, logDirInfoByReplica);
          break;
        case LEADER_ACTION:
          action = reconcileLeadershipTask(task, proposal, cluster);
          break;
        default:
          throw new IllegalStateException("Unsupported task type " + task.type());
      }
      LOG.debug("Recovery action for persisted task {} is {}.", task, action);
      actionByTask.put(task, action);
    }
    Set<TopicPartition> unexpectedOngoingReassignments = new HashSet<>(ongoingReassignments.keySet());
    unexpectedOngoingReassignments.removeAll(expectedOngoingReassignments);
    return new RecoveryPlan(state, actionByTask, unexpectedOngoingReassignments);
  }

  private static boolean isTerminalWithError(ExecutionTaskState state) {
    return state == ExecutionTaskState.ABORTING || state == ExecutionTaskState.ABORTED || state == ExecutionTaskState.DEAD;
  }

  private static RecoveryAction reconcileInterBrokerReplicaTask(PersistedTask task,
                                                                ExecutionProposal proposal,
                                                                Cluster cluster,
                                                                PartitionReassignment ongoingReassignment) {
    PartitionInfo partitionInfo = cluster.partition(task.topicPartition());
    if (partitionInfo == null) {
      return RecoveryAction.DROPPED;
    }
    if (ongoingReassignment != null) {
      List<Integer> targetReplicas = new ArrayList<>(ongoingReassignment.replicas());
      targetReplicas.removeAll(ongoingReassignment.removingReplicas());
      if (isTerminalWithError(task.state())) {
        // The cancellation (i.e. rollback) of the task is still ongoing.
        return RecoveryAction.ROLLBACK_IN_FLIGHT;
      }
      if (task.state() != ExecutionTaskState.COMPLETED
          && new HashSet<>(targetReplicas).equals(brokerIds(proposal.newReplicas()))) {
        return RecoveryAction.ADOPT;
      }
      // An ongoing reassignment of the partition towards some other replicas -- i.e. not started by this execution.
      return RecoveryAction.DROPPED;
    }
    if (isTerminalWithError(task.state())) {
      return RecoveryAction.FINISHED_WITH_ERROR;
    }
    List<Integer> currentReplicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
    if (currentReplicas.equals(orderedBrokerIds(proposal.newReplicas()))) {
      return RecoveryAction.COMPLETED;
    }
    Set<Integer> currentReplicaSet = new HashSet<>(currentReplicas);
    if (currentReplicaSet.equals(brokerIds(proposal.oldReplicas())) || currentReplicaSet.equals(brokerIds(proposal.newReplicas()))) {
      // Either the reassignment has not started (or was reverted), or only the replica order remains to be changed.
      return RecoveryAction.PENDING;
    }
    return RecoveryAction.DROPPED;
  }

  private static RecoveryAction reconcileIntraBrokerReplicaTask(PersistedTask task,
                                                                ExecutionProposal proposal,
                                                                Cluster cluster,
                                                                Map<TopicPartitionReplica, ReplicaLogDirInfo> logDirInfoByReplica) {
    if (cluster.partition(task.topicPartition()) == null) {
      return RecoveryAction.DROPPED;
    }
    if (isTerminalWithError(task.state())) {
      return RecoveryAction.FINISHED_WITH_ERROR;
    }
    ReplicaPlacementInfo target = proposal.replicasToMoveBetweenDisksByBroker().get(task.brokerId());
    if (target == null) {
      return RecoveryAction.DROPPED;
    }
    ReplicaLogDirInfo info = logDirInfoByReplica.get(
        new TopicPartitionReplica(task.topicPartition().topic(), task.topicPartition().partition(), task.brokerId()));
    if (info == null || info.getCurrentReplicaLogDir() == null) {
      // Unknown log directory -- let the executor figure it out.
      return task.state() == ExecutionTaskState.COMPLETED ? RecoveryAction.COMPLETED : RecoveryAction.PENDING;
    }
    if (target.logdir().equals(info.getCurrentReplicaLogDir()) && info.getFutureReplicaLogDir() == null) {
      return RecoveryAction.COMPLETED;
    }
    if (target.logdir().equals(info.getFutureReplicaLogDir()) && task.state() != ExecutionTaskState.COMPLETED) {
      return RecoveryAction.ADOPT;
    }
    return RecoveryAction.PENDING;
  }

  private static RecoveryAction reconcileLeadershipTask(PersistedTask task, ExecutionProposal proposal, Cluster cluster) {
    PartitionInfo partitionInfo = cluster.partition(task.topicPartition());
    if (partitionInfo == null) {
      return RecoveryAction.DROPPED;
    }
    if (isTerminalWithError(task.state())) {
      return RecoveryAction.FINISHED_WITH_ERROR;
    }
    Node leader = partitionInfo.leader();
    if (leader != null && leader.id() == proposal.newLeader().brokerId()) {
      return RecoveryAction.COMPLETED;
    }
    // Leader election is idempotent and cheap -- simply retry it.
    return RecoveryAction.PENDING;
  }

  private static Set<Integer> brokerIds(List<ReplicaPlacementInfo> replicas) {
    return replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet());
  }

  private static List<Integer> orderedBrokerIds(List<ReplicaPlacementInfo> replicas) {
    return replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList());
  }

  /**
   * Rebuild the replica movement strategy with the given name (see {@link ReplicaMovementStrategy#name()}) from the
   * strategies in {@link ExecutorConfig#REPLICA_MOVEMENT_STRATEGIES_CONFIG}.
   *
   * @param name Name of the (possibly chained) replica movement strategy, or {@code null}.
   * @param config Cruise Control config.
   * @return The replica movement strategy, or {@code null} (i.e. use the default strategy) if the given name is {@code null}
   * or refers to an unsupported strategy.
   */
  static ReplicaMovementStrategy replicaMovementStrategy(String name, KafkaCruiseControlConfig config) {
    if (name == null || name.isBlank()) {
      return null;
    }
    Map<String, ReplicaMovementStrategy> supportedStrategiesByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    ReplicaMovementStrategy baseStrategy = new BaseReplicaMovementStrategy();
    supportedStrategiesByName.put(baseStrategy.name(), baseStrategy);
    for (ReplicaMovementStrategy strategy : config.getConfiguredInstances(ExecutorConfig.REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                                                                          ReplicaMovementStrategy.class)) {
      supportedStrategiesByName.put(strategy.name(), strategy);
    }
    ReplicaMovementStrategy strategy = null;
    for (String strategyName : name.split(",")) {
      ReplicaMovementStrategy supportedStrategy = supportedStrategiesByName.get(strategyName.trim());
      if (supportedStrategy == null) {
        LOG.warn("Replica movement strategy {} of the interrupted execution is not supported (supported: {}). Using the default"
                 + " replica movement strategy instead.", strategyName, supportedStrategiesByName.keySet());
        return null;
      }
      strategy = strategy == null ? supportedStrategy : strategy.chain(supportedStrategy);
    }
    return strategy.chainBaseReplicaMovementStrategyIfAbsent();
  }

  /**
   * @param reason Reason of the interrupted execution.
   * @return Reason of the resumed execution.
   */
  static String resumedReason(String reason) {
    return reason.startsWith(RESUMED_REASON_PREFIX) ? reason : RESUMED_REASON_PREFIX + reason;
  }
}
