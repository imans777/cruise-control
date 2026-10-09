/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.kafka.common.Cluster;

/**
 * The strategy, which spreads inter-broker replica movements across brokers in a round-robin manner to avoid a single
 * broker becoming the bottleneck of an execution.
 * <p>
 * When this strategy is part of the (possibly chained) replica movement strategy of an execution, the executor:
 * <ul>
 *   <li>Picks brokers in round-robin order, i.e. the broker with the fewest replica movements scheduled so far in the
 *   execution is picked first, so that a broker gets another movement only after the other brokers had their turn.</li>
 *   <li>Treats every broker hosting a replica of the partition, before or after the movement, as involved in the movement
 *   when choosing movements to start together. Hence, a broker that is not the leader or a destination of a movement
 *   (e.g. a follower whose replica is being removed) is not picked for another movement in the same round.</li>
 * </ul>
 * This strategy does not change the order of tasks within a broker, which is determined by the other chained strategies.
 */
public class RoundRobinBrokerReplicaMovementStrategy extends AbstractReplicaMovementStrategy {

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
    return RoundRobinBrokerReplicaMovementStrategy.class.getSimpleName();
  }

  /**
   * @param strategy The (possibly chained) replica movement strategy to check.
   * @return {@code true} if the given strategy is or contains {@link RoundRobinBrokerReplicaMovementStrategy}, {@code false} otherwise.
   */
  public static boolean isEnabledIn(ReplicaMovementStrategy strategy) {
    return strategy != null && Arrays.asList(strategy.name().split(","))
                                     .contains(RoundRobinBrokerReplicaMovementStrategy.class.getSimpleName());
  }
}
