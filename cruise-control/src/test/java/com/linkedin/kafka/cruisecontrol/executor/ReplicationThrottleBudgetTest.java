/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.LEADER_ACTION;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicationThrottleBudgetTest {
  private static final long THROTTLE = 100L;
  private static final long EXECUTION_ALERTING_THRESHOLD_MS = 100L;
  private final ReplicaPlacementInfo _r0 = new ReplicaPlacementInfo(0);
  private final ReplicaPlacementInfo _r1 = new ReplicaPlacementInfo(1);
  private final ReplicaPlacementInfo _r2 = new ReplicaPlacementInfo(2);
  private final ReplicaPlacementInfo _r3 = new ReplicaPlacementInfo(3);
  private final ReplicaPlacementInfo _r4 = new ReplicaPlacementInfo(4);
  private final Map<TopicPartition, Double> _writeRateByPartition = new HashMap<>();
  private int _nextPartition = 0;

  @Test
  public void testLeaderSideMultipliedByNumReplicasToAdd() {
    // In progress: 30 on the leader side of broker 0 (one replica to add, to broker 1).
    ExecutionTask inProgress = inProgress(task(30.0, _r0, List.of(_r0), List.of(_r0, _r1)));
    ReplicationThrottleBudget budget = budget(List.of(inProgress), 0.0);
    // 30 + 36 * 2 > 100 on the leader side of broker 0.
    assertFalse(budget.fits(task(36.0, _r0, List.of(_r0), List.of(_r0, _r2, _r3))));
    // 30 + 36 * 1 <= 100 on the leader side of broker 0.
    assertTrue(budget.fits(task(36.0, _r0, List.of(_r0), List.of(_r0, _r2))));
  }

  @Test
  public void testFollowerSide() {
    // In progress: 60 on the follower side of broker 2.
    ExecutionTask inProgress = inProgress(task(60.0, _r0, List.of(_r0), List.of(_r0, _r2)));
    ReplicationThrottleBudget budget = budget(List.of(inProgress), 0.0);
    // 60 + 50 > 100 on the follower side of broker 2, while the leader side of broker 1 has nothing in use.
    assertFalse(budget.fits(task(50.0, _r1, List.of(_r1), List.of(_r1, _r2))));
    // 60 + 40 <= 100 on the follower side of broker 2.
    assertTrue(budget.fits(task(40.0, _r1, List.of(_r1), List.of(_r1, _r2))));
    // Broker 2 as a leader does not share the follower side usage.
    assertTrue(budget.fits(task(90.0, _r2, List.of(_r2), List.of(_r2, _r3))));
  }

  @Test
  public void testMargin() {
    ExecutionTask inProgress = inProgress(task(50.0, _r0, List.of(_r0), List.of(_r0, _r1)));
    ExecutionTask task = task(40.0, _r2, List.of(_r2), List.of(_r2, _r1));
    // 50 + 40 <= 100 without margin.
    assertTrue(budget(List.of(inProgress), 0.0).fits(task));
    // 50 + 40 * 1.25 = 100 <= 100.
    assertTrue(budget(List.of(inProgress), 0.25).fits(task));
    // 50 + 40 * 1.5 > 100.
    assertFalse(budget(List.of(inProgress), 0.5).fits(task));
  }

  @Test
  public void testFitsIfNothingInUse() {
    // A partition with a write rate higher than the throttle fits if nothing is in use on its brokers.
    ExecutionTask hotTask = task(150.0, _r0, List.of(_r0), List.of(_r0, _r1));
    assertTrue(budget(Collections.emptyList(), 0.2).fits(hotTask));
    // But not if something is in use on any of its brokers.
    ExecutionTask inProgress = inProgress(task(1.0, _r2, List.of(_r2), List.of(_r2, _r1)));
    assertFalse(budget(List.of(inProgress), 0.2).fits(hotTask));
    // Tasks that are not in progress or not inter-broker replica movements are not in use.
    ExecutionTask pending = task(1.0, _r2, List.of(_r2), List.of(_r2, _r1));
    ExecutionTask leaderAction = new ExecutionTask(0L, inProgress.proposal(), LEADER_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    leaderAction.inProgress(0L);
    assertTrue(budget(List.of(pending, leaderAction), 0.2).fits(hotTask));
  }

  @Test
  public void testReserveAccumulates() {
    ReplicationThrottleBudget budget = budget(Collections.emptyList(), 0.0);
    ExecutionTask task0 = task(40.0, _r0, List.of(_r0), List.of(_r0, _r2));
    ExecutionTask task1 = task(40.0, _r1, List.of(_r1), List.of(_r1, _r2));
    ExecutionTask task2 = task(40.0, _r3, List.of(_r3), List.of(_r3, _r2));
    assertTrue(budget.fits(task0));
    budget.reserve(task0);
    // 40 + 40 <= 100 on the follower side of broker 2.
    assertTrue(budget.fits(task1));
    budget.reserve(task1);
    // 80 + 40 > 100 on the follower side of broker 2.
    assertFalse(budget.fits(task2));
    // Reserving on the leader side of broker 4 makes it in use.
    ExecutionTask hotTask = task(150.0, _r4, List.of(_r4), List.of(_r4, _r0));
    assertTrue(budget.fits(hotTask));
    budget.reserve(task(10.0, _r4, List.of(_r4), List.of(_r4, _r3)));
    assertFalse(budget.fits(hotTask));
  }

  @Test
  public void testPartitionWithoutWriteRate() {
    ExecutionTask inProgress = inProgress(task(90.0, _r0, List.of(_r0), List.of(_r0, _r1)));
    ReplicationThrottleBudget budget = budget(List.of(inProgress), 0.2);
    // A partition without a known write rate needs nothing.
    ExecutionTask unknownTask = task(null, _r0, List.of(_r0), List.of(_r0, _r1));
    assertTrue(budget.fits(unknownTask));
    budget.reserve(unknownTask);
    // 90 + 8 * 1.2 <= 100 on both sides.
    assertTrue(budget.fits(task(8.0, _r0, List.of(_r0), List.of(_r0, _r1))));
    // An in-progress partition without a known write rate does not make its brokers in use.
    ExecutionTask unknownInProgress = inProgress(task(null, _r2, List.of(_r2), List.of(_r2, _r3)));
    assertTrue(budget(List.of(unknownInProgress), 0.2).fits(task(150.0, _r2, List.of(_r2), List.of(_r2, _r3))));
  }

  private ReplicationThrottleBudget budget(Collection<ExecutionTask> inProgressTasks, double margin) {
    return new ReplicationThrottleBudget(inProgressTasks, _writeRateByPartition, brokerId -> THROTTLE, brokerId -> THROTTLE, margin);
  }

  /**
   * Create an inter-broker replica movement task of a new partition with the given write rate.
   *
   * @param writeRate The write rate of the partition, or {@code null} if unknown.
   * @param oldLeader The old leader.
   * @param oldReplicas The old replicas.
   * @param newReplicas The new replicas.
   * @return The inter-broker replica movement task.
   */
  private ExecutionTask task(Double writeRate,
                             ReplicaPlacementInfo oldLeader,
                             List<ReplicaPlacementInfo> oldReplicas,
                             List<ReplicaPlacementInfo> newReplicas) {
    TopicPartition tp = new TopicPartition(TOPIC0, _nextPartition++);
    if (writeRate != null) {
      _writeRateByPartition.put(tp, writeRate);
    }
    ExecutionProposal proposal = new ExecutionProposal(tp, 10, oldLeader, oldReplicas, newReplicas);
    return new ExecutionTask(0L, proposal, INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private static ExecutionTask inProgress(ExecutionTask task) {
    task.inProgress(0L);
    return task;
  }
}
