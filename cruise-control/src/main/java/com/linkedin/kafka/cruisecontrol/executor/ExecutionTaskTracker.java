/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.utils.Time;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.EXECUTOR_SENSOR;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.BYTES_IN_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType;

/**
 * A class for tracking the (1) dead tasks, (2) aborting/aborted tasks, (3) in progress tasks, and (4) pending tasks.
 *
 * This class is not thread-safe.
 */
public class ExecutionTaskTracker {
  private final Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> _tasksByType;
  // The data movement and execution start time are volatile, because gauge sensors read them from metric reporter threads.
  private volatile long _totalInterBrokerDataToMoveInMB;
  private volatile long _totalIntraBrokerDataToMoveInMB;
  private volatile long _remainingInterBrokerDataToMoveInMB;
  private volatile long _remainingIntraBrokerDataToMoveInMB;
  private volatile long _inExecutionInterBrokerDataMovementInMB;
  private volatile long _inExecutionIntraBrokerDataMovementInMB;
  private volatile long _finishedInterBrokerDataMovementInMB;
  private volatile long _finishedIntraBrokerDataMovementInMB;
  private volatile long _completedInterBrokerDataMovementInMB;
  private volatile long _completedIntraBrokerDataMovementInMB;
  private volatile long _executionStartMs;
  private boolean _isKafkaAssignerMode;
  private final Time _time;
  private volatile boolean _stopRequested;
  // Incremented whenever a task is added or changes its state -- used for detecting changes to persist.
  private long _taskStateVersion;
  private final Meter _interBrokerPartitionMovementRateMeter;
  private final Meter _intraBrokerPartitionMovementRateMeter;
  private final Meter _leadershipMovementRateMeter;
  private final Meter _partitionDataMovementRateMeter;
  // Bytes of the in-progress replica movement tasks that have been accounted for in _partitionDataMovementRateMeter.
  private final Map<ExecutionTask, Long> _meteredDataMovementInBytesByTask;
  // Bytes that have been moved but not yet marked in _partitionDataMovementRateMeter, which is marked in MBs.
  private long _unmeteredDataMovementInBytes;

  public static final String INTER_BROKER_REPLICA_ACTION = "replica-action";
  public static final String INTRA_BROKER_REPLICA_ACTION = "intra-broker-replica-action";
  public static final String LEADERSHIP_ACTION = "leadership-action";
  public static final String IN_PROGRESS = "in-progress";
  public static final String PENDING = "pending";
  public static final String ABORTING = "aborting";
  public static final String ABORTED = "aborted";
  public static final String DEAD = "dead";
  public static final String COMPLETED = "completed";
  public static final String GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE = "ongoing-execution-kafka_assigner";
  public static final String GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE = "ongoing-execution-non_kafka_assigner";
  public static final String METER_INTER_BROKER_PARTITION_MOVEMENT_RATE = "inter-broker-partition-movement-rate";
  public static final String METER_INTRA_BROKER_PARTITION_MOVEMENT_RATE = "intra-broker-partition-movement-rate";
  public static final String METER_LEADERSHIP_MOVEMENT_RATE = "leadership-movement-rate";
  public static final String METER_PARTITION_DATA_MOVEMENT_RATE = "partition-data-movement-rate-MB";
  public static final String GAUGE_ONGOING_EXECUTION_DURATION_MS = "ongoing-execution-duration-ms";
  public static final String GAUGE_INTER_BROKER_DATA_TOTAL_MB = "ongoing-execution-inter-broker-data-movement-total-MB";
  public static final String GAUGE_INTER_BROKER_DATA_FINISHED_MB = "ongoing-execution-inter-broker-data-movement-finished-MB";
  public static final String GAUGE_INTER_BROKER_DATA_COMPLETED_MB = "ongoing-execution-inter-broker-data-movement-completed-MB";
  public static final String GAUGE_INTER_BROKER_DATA_IN_EXECUTION_MB = "ongoing-execution-inter-broker-data-movement-in-execution-MB";
  public static final String GAUGE_INTER_BROKER_DATA_REMAINING_MB = "ongoing-execution-inter-broker-data-movement-remaining-MB";
  public static final String GAUGE_INTRA_BROKER_DATA_TOTAL_MB = "ongoing-execution-intra-broker-data-movement-total-MB";
  public static final String GAUGE_INTRA_BROKER_DATA_FINISHED_MB = "ongoing-execution-intra-broker-data-movement-finished-MB";
  public static final String GAUGE_INTRA_BROKER_DATA_COMPLETED_MB = "ongoing-execution-intra-broker-data-movement-completed-MB";
  public static final String GAUGE_INTRA_BROKER_DATA_IN_EXECUTION_MB = "ongoing-execution-intra-broker-data-movement-in-execution-MB";
  public static final String GAUGE_INTRA_BROKER_DATA_REMAINING_MB = "ongoing-execution-intra-broker-data-movement-remaining-MB";
  private static final long NO_ONGOING_EXECUTION = -1L;

  ExecutionTaskTracker(MetricRegistry dropwizardMetricRegistry, Time time) {
    List<ExecutionTaskState> states = ExecutionTaskState.cachedValues();
    List<TaskType> taskTypes = TaskType.cachedValues();
    _tasksByType = new HashMap<>();
    for (TaskType type : taskTypes) {
      Map<ExecutionTaskState, Set<ExecutionTask>> taskMap = new HashMap<>();
      for (ExecutionTaskState state : states) {
        taskMap.put(state, new HashSet<>());
      }
      _tasksByType.put(type, taskMap);
    }
    _totalInterBrokerDataToMoveInMB = 0L;
    _totalIntraBrokerDataToMoveInMB = 0L;
    _remainingInterBrokerDataToMoveInMB = 0L;
    _remainingIntraBrokerDataToMoveInMB = 0L;
    _inExecutionInterBrokerDataMovementInMB = 0L;
    _inExecutionIntraBrokerDataMovementInMB = 0L;
    _finishedInterBrokerDataMovementInMB = 0L;
    _finishedIntraBrokerDataMovementInMB = 0L;
    _completedInterBrokerDataMovementInMB = 0L;
    _completedIntraBrokerDataMovementInMB = 0L;
    _executionStartMs = NO_ONGOING_EXECUTION;
    _isKafkaAssignerMode = false;
    _time = time;
    _stopRequested = false;
    _taskStateVersion = 0L;
    _interBrokerPartitionMovementRateMeter = new Meter();
    _intraBrokerPartitionMovementRateMeter = new Meter();
    _leadershipMovementRateMeter = new Meter();
    _partitionDataMovementRateMeter = new Meter();
    _meteredDataMovementInBytesByTask = new HashMap<>();
    _unmeteredDataMovementInBytes = 0L;

    // Register gauge sensors.
    registerGaugeSensors(dropwizardMetricRegistry);

    // Register meter sensors.
    registerMeterSensors(dropwizardMetricRegistry);
  }

  private void registerGaugeSensors(MetricRegistry dropwizardMetricRegistry) {
    for (TaskType type : TaskType.cachedValues()) {
      for (ExecutionTaskState state : ExecutionTaskState.cachedValues()) {
        String typeString = type == TaskType.INTER_BROKER_REPLICA_ACTION
                            ? INTER_BROKER_REPLICA_ACTION : type == TaskType.INTRA_BROKER_REPLICA_ACTION
                                                            ? INTRA_BROKER_REPLICA_ACTION : LEADERSHIP_ACTION;
        String stateString = state == ExecutionTaskState.PENDING
                             ? PENDING : state == ExecutionTaskState.IN_PROGRESS
                                         ? IN_PROGRESS : state == ExecutionTaskState.ABORTING
                                                         ? ABORTING : state == ExecutionTaskState.ABORTED
                                                                      ? ABORTED : state == ExecutionTaskState.COMPLETED
                                                                                  ? COMPLETED : DEAD;
        dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, typeString + "-" + stateString),
                                          (Gauge<Integer>) () -> (state == ExecutionTaskState.PENDING && _stopRequested)
                                                                 ? 0 : _tasksByType.get(type).get(state).size());
      }
    }
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_IN_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) () -> _isKafkaAssignerMode
                                                             && !inExecutionTasks(TaskType.cachedValues()).isEmpty() ? 1 : 0);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_IN_NON_KAFKA_ASSIGNER_MODE),
                                      (Gauge<Integer>) () -> !_isKafkaAssignerMode
                                                             && !inExecutionTasks(TaskType.cachedValues()).isEmpty() ? 1 : 0);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_ONGOING_EXECUTION_DURATION_MS),
                                      (Gauge<Long>) this::ongoingExecutionDurationMs);
    // Data to move in the ongoing execution. Similar to the number of pending tasks, the remaining data to move is reported
    // as 0 once a stop is requested, because pending tasks will not be executed. The finished data movement includes aborted
    // and dead tasks, whereas the completed data movement includes only the completed tasks.
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTER_BROKER_DATA_TOTAL_MB),
                                      (Gauge<Long>) () -> _totalInterBrokerDataToMoveInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTER_BROKER_DATA_FINISHED_MB),
                                      (Gauge<Long>) () -> _finishedInterBrokerDataMovementInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTER_BROKER_DATA_COMPLETED_MB),
                                      (Gauge<Long>) () -> _completedInterBrokerDataMovementInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTER_BROKER_DATA_IN_EXECUTION_MB),
                                      (Gauge<Long>) () -> _inExecutionInterBrokerDataMovementInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTER_BROKER_DATA_REMAINING_MB),
                                      (Gauge<Long>) () -> _stopRequested ? 0L : _remainingInterBrokerDataToMoveInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTRA_BROKER_DATA_TOTAL_MB),
                                      (Gauge<Long>) () -> _totalIntraBrokerDataToMoveInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTRA_BROKER_DATA_FINISHED_MB),
                                      (Gauge<Long>) () -> _finishedIntraBrokerDataMovementInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTRA_BROKER_DATA_COMPLETED_MB),
                                      (Gauge<Long>) () -> _completedIntraBrokerDataMovementInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTRA_BROKER_DATA_IN_EXECUTION_MB),
                                      (Gauge<Long>) () -> _inExecutionIntraBrokerDataMovementInMB);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, GAUGE_INTRA_BROKER_DATA_REMAINING_MB),
                                      (Gauge<Long>) () -> _stopRequested ? 0L : _remainingIntraBrokerDataToMoveInMB);
  }

  /**
   * @return Duration of the ongoing execution in ms if there is one, 0 otherwise. The duration is measured from the time
   * the tasks of the execution were added to the tracker.
   */
  private long ongoingExecutionDurationMs() {
    long executionStartMs = _executionStartMs;
    return executionStartMs == NO_ONGOING_EXECUTION ? 0L : _time.milliseconds() - executionStartMs;
  }

  private void registerMeterSensors(MetricRegistry dropwizardMetricRegistry) {
    dropwizardMetricRegistry.register(
        MetricRegistry.name(EXECUTOR_SENSOR, METER_INTER_BROKER_PARTITION_MOVEMENT_RATE), _interBrokerPartitionMovementRateMeter);
    dropwizardMetricRegistry.register(
        MetricRegistry.name(EXECUTOR_SENSOR, METER_INTRA_BROKER_PARTITION_MOVEMENT_RATE), _intraBrokerPartitionMovementRateMeter);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_LEADERSHIP_MOVEMENT_RATE), _leadershipMovementRateMeter);
    dropwizardMetricRegistry.register(MetricRegistry.name(EXECUTOR_SENSOR, METER_PARTITION_DATA_MOVEMENT_RATE), _partitionDataMovementRateMeter);
  }
  /**
   * Update the execution state of the task.
   *
   * @param task The task to update.
   * @param newState New execution state of the task.
   */
  public void markTaskState(ExecutionTask task, ExecutionTaskState newState) {
    _taskStateVersion++;
    _tasksByType.get(task.type()).get(task.state()).remove(task);
    switch (newState) {
      case PENDING:
        // Let it go.
        break;
      case IN_PROGRESS:
        task.inProgress(_time.milliseconds());
        updateDataMovement(task);
        break;
      case ABORTING:
        task.abort();
        break;
      case ABORTED:
        task.aborted(_time.milliseconds());
        updateDataMovement(task);
        break;
      case COMPLETED:
        markTaskExecutionRateMeter(task);
        task.completed(_time.milliseconds());
        updateDataMovement(task);
        break;
      case DEAD:
        task.kill(_time.milliseconds());
        updateDataMovement(task);
        break;
      default:
        break;
    }
    _tasksByType.get(task.type()).get(newState).add(task);
  }

  private void markTaskExecutionRateMeter(ExecutionTask task) {
    switch (task.type()) {
      case INTER_BROKER_REPLICA_ACTION:
        _interBrokerPartitionMovementRateMeter.mark();
        break;
      case INTRA_BROKER_REPLICA_ACTION:
        _intraBrokerPartitionMovementRateMeter.mark();
        break;
      case LEADER_ACTION:
        _leadershipMovementRateMeter.mark();
        break;
      default:
        break;
    }
  }

  private void updateDataMovement(ExecutionTask task) {
    long dataToMove = task.type() == TaskType.INTRA_BROKER_REPLICA_ACTION
                      ? task.proposal().intraBrokerDataToMoveInMB() : task.type() == TaskType.INTER_BROKER_REPLICA_ACTION
                                                                      ? task.proposal().interBrokerDataToMoveInMB() : 0;
    if (task.state() == ExecutionTaskState.IN_PROGRESS) {
      if (task.type() == TaskType.INTRA_BROKER_REPLICA_ACTION) {
        _remainingIntraBrokerDataToMoveInMB -= dataToMove;
        _inExecutionIntraBrokerDataMovementInMB += dataToMove;
      } else if (task.type() == TaskType.INTER_BROKER_REPLICA_ACTION) {
        _remainingInterBrokerDataToMoveInMB -= dataToMove;
        _inExecutionInterBrokerDataMovementInMB += dataToMove;
      }
    } else if (task.state() == ExecutionTaskState.ABORTED
               || task.state() == ExecutionTaskState.DEAD
               || task.state() == ExecutionTaskState.COMPLETED) {
      boolean isCompleted = task.state() == ExecutionTaskState.COMPLETED;
      Long meteredDataMovementInBytes = _meteredDataMovementInBytesByTask.remove(task);
      if (isCompleted) {
        // Mark the data movement of the task that has not been marked while it was in progress. A dead or aborted task (e.g. an
        // in-progress movement rolled back when the execution is stopped) has not moved the rest of its data.
        markDataMovement(dataToMove * BYTES_IN_MB - (meteredDataMovementInBytes == null ? 0L : meteredDataMovementInBytes));
      }
      if (task.type() == TaskType.INTRA_BROKER_REPLICA_ACTION) {
        _inExecutionIntraBrokerDataMovementInMB -= dataToMove;
        _finishedIntraBrokerDataMovementInMB += dataToMove;
        if (isCompleted) {
          _completedIntraBrokerDataMovementInMB += dataToMove;
        }
      } else if (task.type() == TaskType.INTER_BROKER_REPLICA_ACTION) {
        _inExecutionInterBrokerDataMovementInMB -= dataToMove;
        _finishedInterBrokerDataMovementInMB += dataToMove;
        if (isCompleted) {
          _completedInterBrokerDataMovementInMB += dataToMove;
        }
      }
    }
  }

  /**
   * Mark the data movement observed since the last call for the in-progress replica movement tasks in the partition data
   * movement rate meter. This lets the meter report the data movement rate while partition movements are in progress,
   * rather than only when they finish.
   */
  public void markInProgressDataMovement() {
    for (TaskType type : List.of(TaskType.INTER_BROKER_REPLICA_ACTION, TaskType.INTRA_BROKER_REPLICA_ACTION)) {
      for (ExecutionTask task : _tasksByType.get(type).get(ExecutionTaskState.IN_PROGRESS)) {
        if (task.hasDataMovementProgress()) {
          long meteredDataMovementInBytes = _meteredDataMovementInBytesByTask.getOrDefault(task, 0L);
          // The moved data may shrink, e.g. due to retention on the destination replica -- this is not counted as progress.
          if (task.movedDataInBytes() > meteredDataMovementInBytes) {
            markDataMovement(task.movedDataInBytes() - meteredDataMovementInBytes);
            _meteredDataMovementInBytesByTask.put(task, task.movedDataInBytes());
          }
        }
      }
    }
  }

  private void markDataMovement(long dataMovementInBytes) {
    if (dataMovementInBytes > 0L) {
      _unmeteredDataMovementInBytes += dataMovementInBytes;
      long dataMovementInMB = _unmeteredDataMovementInBytes / BYTES_IN_MB;
      if (dataMovementInMB > 0L) {
        _partitionDataMovementRateMeter.mark(dataMovementInMB);
        _unmeteredDataMovementInBytes -= dataMovementInMB * BYTES_IN_MB;
      }
    }
  }

  /**
   * Add new tasks to ExecutionTaskTracker to trace their execution.
   * Tasks are added homogeneously -- all tasks have the same task type.
   * The first tasks added after {@link #clear()} mark the start of the ongoing execution, and clear a stop that was requested
   * while there was no ongoing execution.
   *
   * @param tasks    New tasks to add.
   * @param taskType Task type of new tasks.
   */
  public void addTasksToTrace(Collection<ExecutionTask> tasks, TaskType taskType) {
    if (_executionStartMs == NO_ONGOING_EXECUTION) {
      _executionStartMs = _time.milliseconds();
      // The executor resets its stop signal when an execution starts, so a stop requested while there was no ongoing
      // execution does not apply to this one. Otherwise, the pending tasks and the remaining data to move of this execution
      // would be reported as 0.
      _stopRequested = false;
    }
    _taskStateVersion++;
    _tasksByType.get(taskType).get(ExecutionTaskState.PENDING).addAll(tasks);
    if (taskType == TaskType.INTER_BROKER_REPLICA_ACTION) {
      long dataToMoveInMB = tasks.stream().mapToLong(t -> t.proposal().interBrokerDataToMoveInMB()).sum();
      _remainingInterBrokerDataToMoveInMB += dataToMoveInMB;
      _totalInterBrokerDataToMoveInMB += dataToMoveInMB;
    } else if (taskType == TaskType.INTRA_BROKER_REPLICA_ACTION) {
      long dataToMoveInMB = tasks.stream().mapToLong(t -> t.proposal().intraBrokerDataToMoveInMB()).sum();
      _remainingIntraBrokerDataToMoveInMB += dataToMoveInMB;
      _totalIntraBrokerDataToMoveInMB += dataToMoveInMB;
    }
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode {@code true} if kafka assigner mode, {@code false} otherwise.
   */
  public void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Get the statistic of task execution state.
   *
   * @return The statistic of task execution state.
   */
  private Map<TaskType, Map<ExecutionTaskState, Integer>> taskStat() {
    Map<TaskType, Map<ExecutionTaskState, Integer>> taskStatMap = new HashMap<>();
    for (TaskType type : TaskType.cachedValues()) {
      taskStatMap.put(type, new HashMap<>());
      _tasksByType.get(type).forEach((k, v) -> taskStatMap.get(type).put(k, v.size()));
    }
    return taskStatMap;
  }

  /**
   * Get a filtered list of tasks of different {@link TaskType} and in different {@link ExecutionTaskState}.
   *
   * @param taskTypesToGetFullList  Task types to return complete list of tasks.
   * @return                        A filtered list of tasks.
   */
  private Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> filteredTasksByState(Set<TaskType> taskTypesToGetFullList) {
    Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> tasksByState = new HashMap<>();
    for (TaskType type : taskTypesToGetFullList) {
      tasksByState.put(type, new HashMap<>());
      _tasksByType.get(type).forEach((k, v) -> tasksByState.get(type).put(k, new HashSet<>(v)));
    }
    return tasksByState;
  }

  /**
   * Clear the replica action and leader action tasks.
   */
  public void clear() {
    _taskStateVersion++;
    _tasksByType.values().forEach(m -> m.values().forEach(Set::clear));
    _totalInterBrokerDataToMoveInMB = 0L;
    _totalIntraBrokerDataToMoveInMB = 0L;
    _remainingInterBrokerDataToMoveInMB = 0L;
    _remainingIntraBrokerDataToMoveInMB = 0L;
    _inExecutionInterBrokerDataMovementInMB = 0L;
    _inExecutionIntraBrokerDataMovementInMB = 0L;
    _finishedInterBrokerDataMovementInMB = 0L;
    _finishedIntraBrokerDataMovementInMB = 0L;
    _completedInterBrokerDataMovementInMB = 0L;
    _completedIntraBrokerDataMovementInMB = 0L;
    _executionStartMs = NO_ONGOING_EXECUTION;
    _meteredDataMovementInBytesByTask.clear();
    _unmeteredDataMovementInBytes = 0L;
    _stopRequested = false;
  }

  public void setStopRequested() {
    _stopRequested = true;
  }

  /**
   * @return A number that changes whenever a task is added or changes its state.
   */
  public long taskStateVersion() {
    return _taskStateVersion;
  }

  /**
   * @return All tracked tasks regardless of their type and state.
   */
  public List<ExecutionTask> allTasks() {
    List<ExecutionTask> allTasks = new ArrayList<>();
    _tasksByType.values().forEach(tasksByState -> tasksByState.values().forEach(allTasks::addAll));
    return allTasks;
  }

  // Internal query APIs.
  public int numRemainingInterBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.PENDING).size();
  }

  public long remainingInterBrokerDataToMoveInMB() {
    return _remainingInterBrokerDataToMoveInMB;
  }

  /**
   * @return Number of finished inter broker partition movements, which is the sum of completed, dead, and aborted tasks.
   */
  public int numFinishedInterBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.COMPLETED).size()
           + _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.DEAD).size()
           + _tasksByType.get(TaskType.INTER_BROKER_REPLICA_ACTION).get(ExecutionTaskState.ABORTED).size();
  }

  public long finishedInterBrokerDataMovementInMB() {
    return _finishedInterBrokerDataMovementInMB;
  }

  /**
   * @return Completed inter broker data movement in MB, which excludes the data of dead and aborted tasks.
   */
  public long completedInterBrokerDataMovementInMB() {
    return _completedInterBrokerDataMovementInMB;
  }

  /**
   * @return Completed intra broker data movement in MB, which excludes the data of dead and aborted tasks.
   */
  public long completedIntraBrokerDataMovementInMB() {
    return _completedIntraBrokerDataMovementInMB;
  }

  /**
   * Get tasks in execution with the given task type.
   *
   * @param types Task type.
   * @return Tasks that are in progress or aborting with the given task type.
   */
  public Set<ExecutionTask> inExecutionTasks(Collection<TaskType> types) {
    Set<ExecutionTask> inExecutionTasks = new HashSet<>();
    for (TaskType type : types) {
      inExecutionTasks.addAll(_tasksByType.get(type).get(ExecutionTaskState.IN_PROGRESS));
      inExecutionTasks.addAll(_tasksByType.get(type).get(ExecutionTaskState.ABORTING));
    }
    return inExecutionTasks;
  }

  public long inExecutionInterBrokerDataMovementInMB() {
    return _inExecutionInterBrokerDataMovementInMB;
  }

  public int numRemainingLeadershipMovements() {
    return _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.PENDING).size();
  }

  /**
   * @return Number of finished leadership movements, which is the sum of completed, dead, and aborted tasks.
   */
  public int numFinishedLeadershipMovements() {
    return _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.COMPLETED).size()
           + _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.DEAD).size()
           + _tasksByType.get(TaskType.LEADER_ACTION).get(ExecutionTaskState.ABORTED).size();
  }

  public int numRemainingIntraBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.PENDING).size();
  }

  public long remainingIntraBrokerDataToMoveInMB() {
    return _remainingIntraBrokerDataToMoveInMB;
  }

  /**
   * @return Number of finished intra broker partition movements, which is the sum of completed, dead, and aborted tasks.
   */
  public int numFinishedIntraBrokerPartitionMovements() {
    return _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.COMPLETED).size()
           + _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.DEAD).size()
           + _tasksByType.get(TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTaskState.ABORTED).size();
  }

  public long finishedIntraBrokerDataToMoveInMB() {
    return _finishedIntraBrokerDataMovementInMB;
  }

  public long inExecutionIntraBrokerDataMovementInMB() {
    return _inExecutionIntraBrokerDataMovementInMB;
  }

  /**
   * @param type Task type.
   * @return Number of MBs that have already been moved by the in-progress tasks with the given task type, as observed
   * from their last known data movement progress.
   */
  public long inProgressMovedDataInMB(TaskType type) {
    return _tasksByType.get(type).get(ExecutionTaskState.IN_PROGRESS).stream()
                       .filter(ExecutionTask::hasDataMovementProgress)
                       .mapToLong(ExecutionTask::movedDataInBytes)
                       .sum() / BYTES_IN_MB;
  }

  /**
   * @param type Task type.
   * @return The sum of the last observed data movement rates (in MB/s) of the in-progress tasks with the given task type,
   * or {@code -1} if the data movement rate of none of these tasks is known.
   */
  private double inProgressDataMovementRateInMBPerSec(TaskType type) {
    return _tasksByType.get(type).get(ExecutionTaskState.IN_PROGRESS).stream()
                       .filter(ExecutionTask::hasDataMovementRate)
                       .mapToDouble(ExecutionTask::dataMovementRateInMBPerSec)
                       .reduce(Double::sum)
                       .orElse(-1.0);
  }

  /**
   * Get execution tasks summary.
   *
   * @param taskTypesToGetFullList Task types to return complete list of tasks.
   * @return Execution tasks summary.
   */
  public ExecutionTasksSummary getExecutionTasksSummary(Set<TaskType> taskTypesToGetFullList) {
    return new ExecutionTasksSummary(_finishedInterBrokerDataMovementInMB,
                                     _finishedIntraBrokerDataMovementInMB,
                                     _inExecutionInterBrokerDataMovementInMB,
                                     _inExecutionIntraBrokerDataMovementInMB,
                                     _remainingInterBrokerDataToMoveInMB,
                                     _remainingIntraBrokerDataToMoveInMB,
                                     inProgressMovedDataInMB(TaskType.INTER_BROKER_REPLICA_ACTION),
                                     inProgressMovedDataInMB(TaskType.INTRA_BROKER_REPLICA_ACTION),
                                     inProgressDataMovementRateInMBPerSec(TaskType.INTER_BROKER_REPLICA_ACTION),
                                     inProgressDataMovementRateInMBPerSec(TaskType.INTRA_BROKER_REPLICA_ACTION),
                                     taskStat(),
                                     filteredTasksByState(taskTypesToGetFullList)
    );
  }

  public static class ExecutionTasksSummary {
    private final long _finishedInterBrokerDataMovementInMB;
    private final long _finishedIntraBrokerDataMovementInMB;
    private final long _inExecutionInterBrokerDataMovementInMB;
    private final long _inExecutionIntraBrokerDataMovementInMB;
    private final long _remainingInterBrokerDataToMoveInMB;
    private final long _remainingIntraBrokerDataToMoveInMB;
    private final long _inProgressInterBrokerMovedDataInMB;
    private final long _inProgressIntraBrokerMovedDataInMB;
    private final double _inProgressInterBrokerDataMovementRateInMBPerSec;
    private final double _inProgressIntraBrokerDataMovementRateInMBPerSec;
    private final Map<TaskType, Map<ExecutionTaskState, Integer>> _taskStat;
    private final Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> _filteredTasksByState;

    ExecutionTasksSummary(long finishedInterBrokerDataMovementInMB,
                          long finishedIntraBrokerDataMovementInMB,
                          long inExecutionInterBrokerDataMovementInMB,
                          long inExecutionIntraBrokerDataMovementInMB,
                          long remainingInterBrokerDataToMoveInMB,
                          long remainingIntraBrokerDataToMoveInMB,
                          long inProgressInterBrokerMovedDataInMB,
                          long inProgressIntraBrokerMovedDataInMB,
                          double inProgressInterBrokerDataMovementRateInMBPerSec,
                          double inProgressIntraBrokerDataMovementRateInMBPerSec,
                          Map<TaskType, Map<ExecutionTaskState, Integer>> taskStat,
                          Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> filteredTasksByState) {
      _finishedInterBrokerDataMovementInMB = finishedInterBrokerDataMovementInMB;
      _finishedIntraBrokerDataMovementInMB = finishedIntraBrokerDataMovementInMB;
      _inExecutionInterBrokerDataMovementInMB = inExecutionInterBrokerDataMovementInMB;
      _inExecutionIntraBrokerDataMovementInMB = inExecutionIntraBrokerDataMovementInMB;
      _remainingInterBrokerDataToMoveInMB = remainingInterBrokerDataToMoveInMB;
      _remainingIntraBrokerDataToMoveInMB = remainingIntraBrokerDataToMoveInMB;
      _inProgressInterBrokerMovedDataInMB = inProgressInterBrokerMovedDataInMB;
      _inProgressIntraBrokerMovedDataInMB = inProgressIntraBrokerMovedDataInMB;
      _inProgressInterBrokerDataMovementRateInMBPerSec = inProgressInterBrokerDataMovementRateInMBPerSec;
      _inProgressIntraBrokerDataMovementRateInMBPerSec = inProgressIntraBrokerDataMovementRateInMBPerSec;
      _taskStat = taskStat;
      _filteredTasksByState = filteredTasksByState;
    }

    public long finishedInterBrokerDataMovementInMB() {
      return _finishedInterBrokerDataMovementInMB;
    }

    public long finishedIntraBrokerDataMovementInMB() {
      return _finishedIntraBrokerDataMovementInMB;
    }

    public long inExecutionInterBrokerDataMovementInMB() {
      return _inExecutionInterBrokerDataMovementInMB;
    }

    public long inExecutionIntraBrokerDataMovementInMB() {
      return _inExecutionIntraBrokerDataMovementInMB;
    }

    public long remainingInterBrokerDataToMoveInMB() {
      return _remainingInterBrokerDataToMoveInMB;
    }

    public long remainingIntraBrokerDataToMoveInMB() {
      return _remainingIntraBrokerDataToMoveInMB;
    }

    /**
     * @return Number of MBs that have already been moved by the in-progress inter-broker replica movement tasks.
     */
    public long inProgressInterBrokerMovedDataInMB() {
      return _inProgressInterBrokerMovedDataInMB;
    }

    /**
     * @return Number of MBs that have already been moved by the in-progress intra-broker replica movement tasks.
     */
    public long inProgressIntraBrokerMovedDataInMB() {
      return _inProgressIntraBrokerMovedDataInMB;
    }

    /**
     * @return The sum of the last observed data movement rates (in MB/s) of the in-progress inter-broker replica movement
     * tasks, or {@code -1} if unknown.
     */
    public double inProgressInterBrokerDataMovementRateInMBPerSec() {
      return _inProgressInterBrokerDataMovementRateInMBPerSec;
    }

    /**
     * @return The sum of the last observed data movement rates (in MB/s) of the in-progress intra-broker replica movement
     * tasks, or {@code -1} if unknown.
     */
    public double inProgressIntraBrokerDataMovementRateInMBPerSec() {
      return _inProgressIntraBrokerDataMovementRateInMBPerSec;
    }

    public Map<TaskType, Map<ExecutionTaskState, Integer>> taskStat() {
      return Collections.unmodifiableMap(_taskStat);
    }

    public Map<TaskType, Map<ExecutionTaskState, Set<ExecutionTask>>> filteredTasksByState() {
      return Collections.unmodifiableMap(_filteredTasksByState);
    }
  }
}
