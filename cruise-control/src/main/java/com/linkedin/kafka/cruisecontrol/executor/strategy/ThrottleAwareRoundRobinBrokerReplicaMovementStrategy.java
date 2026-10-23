/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.kafka.common.Cluster;

/**
 * The strategy, which spreads inter-broker replica movements across brokers in a round-robin manner, within the replication
 * throttle of each broker, to avoid a single broker becoming the bottleneck of an execution.
 * <p>
 * When this strategy is part of the (possibly chained) replica movement strategy of an execution, the executor:
 * <ul>
 *   <li>Picks brokers in round-robin order, i.e. the broker with the fewest replica movements scheduled so far in the
 *   execution is picked first, so that a broker gets another movement only after the other brokers had their turn.</li>
 *   <li>Treats every broker hosting a replica of the partition, before or after the movement, as involved in the movement
 *   when choosing movements to start together. Hence, a broker that is not the leader or a destination of a movement
 *   (e.g. a follower whose replica is being removed) is not picked for another movement in the same round.</li>
 *   <li>If the execution has a replication throttle, starts a movement only if the replication throttle of its source (i.e. the
 *   current leader of the partition) and destination brokers can carry the write rates of the movements in progress on them
 *   plus the write rate of the partition, all increased by {@code replica.movement.throttle.write.rate.margin}, unless no
 *   movement is in progress on them. Hence, cooler partitions fill the replication throttle of a broker first, leaving a share
 *   of it to catch up, and a partition with a write rate higher than the throttle starts alone rather than wait forever.
 *   Otherwise, movements whose combined write rate exceeds the throttle never catch up. The write rate of a partition is its
 *   leader bytes in rate in the current metrics window, or its last known rate if the window has no sample yet.</li>
 * </ul>
 * This strategy does not change the order of tasks within a broker, which is determined by the other chained strategies.
 */
public class ThrottleAwareRoundRobinBrokerReplicaMovementStrategy extends AbstractReplicaMovementStrategy {

  @Override
  public Comparator<ExecutionTask> taskComparator(StrategyOptions strategyOptions) {
    return (task1, task2) -> PRIORITIZE_NONE;
  }

  @Override
  public Comparator<ExecutionTask> taskComparator(Cluster cluster) {
    return taskComparator(new StrategyOptions.Builder(cluster).build());
  }

  /**
   * Get the name of this strategy. Name of a strategy provides an identification for the strategy in human readable format.
   */
  @Override
  public String name() {
    return ThrottleAwareRoundRobinBrokerReplicaMovementStrategy.class.getSimpleName();
  }

  /**
   * @param strategy The (possibly chained) replica movement strategy to check.
   * @return {@code true} if the given strategy is or contains {@link ThrottleAwareRoundRobinBrokerReplicaMovementStrategy}, {@code false} otherwise.
   */
  public static boolean isEnabledIn(ReplicaMovementStrategy strategy) {
    return strategy != null && Arrays.asList(strategy.name().split(","))
                                     .contains(ThrottleAwareRoundRobinBrokerReplicaMovementStrategy.class.getSimpleName());
  }
}
