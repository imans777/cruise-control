/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;

/**
 * The replication throttle budget of brokers for inter-broker replica movements.
 * <p>
 * The replication throttle of a broker is shared by all throttled replica movements in progress on that broker: the leader
 * throttle by the movements whose leader (i.e. source) is the broker, the follower throttle by the movements that add a replica
 * to the broker. A movement does not finish if the throttle cannot carry the write rates of the partitions moving through
 * the broker. This class tracks the write rate (in bytes per second) of the movements on each side of each broker:
 * <ul>
 *   <li>Leader side of the old leader: the write rate of the partition times the number of replicas to add.</li>
 *   <li>Follower side of each broker to add a replica to: the write rate of the partition.</li>
 * </ul>
 * A partition without a known write rate is considered to have a write rate of 0.
 * <p>
 * This class is not thread safe.
 */
class ReplicationThrottleBudget {
  private final Map<TopicPartition, Double> _writeRateByPartition;
  private final Function<Integer, Long> _leaderThrottleByBrokerId;
  private final Function<Integer, Long> _followerThrottleByBrokerId;
  private final double _margin;
  private final Map<Integer, Double> _leaderUsageByBrokerId;
  private final Map<Integer, Double> _followerUsageByBrokerId;

  /**
   * @param inProgressTasks The inter-broker replica movement tasks in progress. Tasks of other types or states are ignored.
   * @param writeRateByPartition The write rate of partitions in bytes per second.
   * @param leaderThrottleByBrokerId The leader replication throttle of a broker in bytes per second, {@code null} for no throttle.
   * @param followerThrottleByBrokerId The follower replication throttle of a broker in bytes per second, {@code null} for no throttle.
   * @param margin The fraction by which the write rate of a movement to start is increased to leave a margin to the throttle.
   */
  ReplicationThrottleBudget(Collection<ExecutionTask> inProgressTasks,
                            Map<TopicPartition, Double> writeRateByPartition,
                            Function<Integer, Long> leaderThrottleByBrokerId,
                            Function<Integer, Long> followerThrottleByBrokerId,
                            double margin) {
    _writeRateByPartition = writeRateByPartition;
    _leaderThrottleByBrokerId = leaderThrottleByBrokerId;
    _followerThrottleByBrokerId = followerThrottleByBrokerId;
    _margin = margin;
    _leaderUsageByBrokerId = new HashMap<>();
    _followerUsageByBrokerId = new HashMap<>();
    for (ExecutionTask task : inProgressTasks) {
      if (task.type() == INTER_BROKER_REPLICA_ACTION && task.state() == ExecutionTaskState.IN_PROGRESS) {
        reserve(task);
      }
    }
  }

  /**
   * A task fits if, on each side of each broker it involves, either the throttle can carry the write rate in use plus the write
   * rate of the task increased by the margin, or no write rate is in use. The latter lets a partition with a write rate higher
   * than the throttle start alone rather than wait forever.
   *
   * @param task Inter-broker replica movement task.
   * @return {@code true} if the given task fits in the replication throttle budget, {@code false} otherwise.
   */
  boolean fits(ExecutionTask task) {
    double writeRate = writeRate(task);
    double need = writeRate * (1 + _margin);
    if (!fitsOnBrokerSide(_leaderUsageByBrokerId, _leaderThrottleByBrokerId, task.proposal().oldLeader().brokerId(),
              need * task.proposal().replicasToAdd().size())) {
      return false;
    }
    for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
      if (!fitsOnBrokerSide(_followerUsageByBrokerId, _followerThrottleByBrokerId, replicaToAdd.brokerId(), need)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Reserve the write rate of the given task, so that it is considered to be in use by {@link #fits(ExecutionTask)}.
   *
   * @param task Inter-broker replica movement task.
   */
  void reserve(ExecutionTask task) {
    double writeRate = writeRate(task);
    _leaderUsageByBrokerId.merge(task.proposal().oldLeader().brokerId(), writeRate * task.proposal().replicasToAdd().size(), Double::sum);
    for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
      _followerUsageByBrokerId.merge(replicaToAdd.brokerId(), writeRate, Double::sum);
    }
  }

  private double writeRate(ExecutionTask task) {
    return _writeRateByPartition.getOrDefault(task.proposal().topicPartition(), 0.0);
  }

  private static boolean fitsOnBrokerSide(Map<Integer, Double> usageByBrokerId,
                                          Function<Integer, Long> throttleByBrokerId,
                                          int brokerId,
                                          double need) {
    double usage = usageByBrokerId.getOrDefault(brokerId, 0.0);
    if (usage <= 0.0) {
      return true;
    }
    Long throttle = throttleByBrokerId.apply(brokerId);
    return throttle == null || usage + need <= throttle;
  }
}
