/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.DiskCapacityTracker.BlockedAction;
import static com.linkedin.kafka.cruisecontrol.executor.DiskCapacityTracker.DEFAULT_LOG_SEGMENT_DELETE_DELAY_MS;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link DiskCapacityTracker}.
 */
public class DiskCapacityTrackerTest {
  private static final long GB = 1024L * 1024L * 1024L;
  private static final long DISK = 1000 * GB;
  private static final long ALERTING_THRESHOLD_MS = 1000L;
  private static final String TOPIC = "topic";
  private static final String D1 = "/d1";
  private static final String D2 = "/d2";
  private static final String D3 = "/d3";
  private final MockTime _time = new MockTime(0L, 0L, 0L);
  private long _executionId = 0L;

  /**
   * One broker with three 1000 GB log dirs, d2 starting at 650 GB. Partitions a and b move to d2 (190 GB each), and partitions c
   * (100 GB) and e (150 GB) move off d2, in this order, one at a time. d2 ends at 780 GB, but would reach 1030 GB if b started
   * right after a. b has to wait until c has completed and its replica on d2 is deleted.
   */
  @Test
  public void testIntraBrokerMovementWaitsUntilReplacedReplicaIsDeleted() {
    TopicPartition a = new TopicPartition(TOPIC, 0);
    TopicPartition b = new TopicPartition(TOPIC, 1);
    TopicPartition c = new TopicPartition(TOPIC, 2);
    TopicPartition e = new TopicPartition(TOPIC, 3);
    Map<String, LogDirDescription> logDirs = new HashMap<>();
    logDirs.put(D1, logDir(DISK, Map.of(a, replica(190 * GB), new TopicPartition(TOPIC, 4), replica(300 * GB))));
    logDirs.put(D2, logDir(DISK, Map.of(c, replica(100 * GB), e, replica(150 * GB), new TopicPartition(TOPIC, 5), replica(400 * GB))));
    logDirs.put(D3, logDir(DISK, Map.of(b, replica(190 * GB))));
    DiskCapacityTracker tracker = tracker(Map.of(0, logDirs), Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap());

    ExecutionTask moveA = intraBrokerMovement(a, 0, D1, D2);
    ExecutionTask moveB = intraBrokerMovement(b, 0, D3, D2);
    ExecutionTask moveC = intraBrokerMovement(c, 0, D2, D1);
    ExecutionTask moveE = intraBrokerMovement(e, 0, D2, D3);

    tracker.beginPlanning();
    assertTrue(tracker.fits(moveA));
    tracker.reserve(moveA);
    // d2 is at 650 + 190 = 840 GB, b would take it to 1030 GB.
    assertFalse(tracker.fits(moveB));
    assertTrue(tracker.refusals().get(moveB).contains("log dir /d2 of broker 0"));
    assertTrue(tracker.fits(moveC));
    tracker.reserve(moveC);
    assertTrue(tracker.fits(moveE));
    tracker.reserve(moveE);

    // c completes. Its replica on d2 is deleted log.segment.delete.delay.ms later.
    start(moveA, moveC, moveE);
    moveC.completed(_time.milliseconds());
    _time.sleep(DEFAULT_LOG_SEGMENT_DELETE_DELAY_MS - 1);
    tracker.beginPlanning();
    assertFalse(tracker.fits(moveB));
    assertTrue(tracker.hasPendingDeletions());

    _time.sleep(1);
    tracker.beginPlanning();
    // d2 is at 840 - 100 = 740 GB, b takes it to 930 GB.
    assertFalse(tracker.hasPendingDeletions());
    assertTrue(tracker.fits(moveB));
    assertTrue(tracker.refusals().isEmpty());
  }

  /**
   * Broker 1 has one 1000 GB log dir at 700 GB. Two 190 GB partitions move to it, while two 150 GB partitions move off it. Both
   * incoming movements can start together only if nothing else limits them, which would take broker 1 to 1080 GB.
   */
  @Test
  public void testInterBrokerMovementsToTheSameBroker() {
    TopicPartition x = new TopicPartition(TOPIC, 0);
    TopicPartition y = new TopicPartition(TOPIC, 1);
    TopicPartition z = new TopicPartition(TOPIC, 2);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = new HashMap<>();
    logDirsByBroker.put(0, Map.of(D1, logDir(DISK, Map.of(x, replica(190 * GB), y, replica(190 * GB)))));
    logDirsByBroker.put(1, Map.of(D1, logDir(DISK, Map.of(z, replica(150 * GB), new TopicPartition(TOPIC, 3), replica(550 * GB)))));
    logDirsByBroker.put(2, Map.of(D1, logDir(DISK, Collections.emptyMap())));
    Map<Integer, Long> deleteDelayMsByBroker = Map.of(1, 10_000L);
    DiskCapacityTracker tracker = tracker(logDirsByBroker, Collections.emptySet(), Collections.emptyMap(), deleteDelayMsByBroker);

    ExecutionTask moveX = interBrokerMovement(x, 190, 0, 1);
    ExecutionTask moveY = interBrokerMovement(y, 190, 0, 1);
    ExecutionTask moveZ = interBrokerMovement(z, 150, 1, 2);

    tracker.beginPlanning();
    assertTrue(tracker.fits(moveX));
    tracker.reserve(moveX);
    assertFalse(tracker.fits(moveY));
    assertTrue(tracker.fits(moveZ));
    tracker.reserve(moveZ);
    assertEquals(2, tracker.numStartedMovements());

    // Broker 1 uses its own log.segment.delete.delay.ms.
    start(moveX, moveZ);
    moveZ.completed(_time.milliseconds());
    _time.sleep(10_000L);
    tracker.beginPlanning();
    // Broker 1 is at 700 + 190 - 150 = 740 GB, y takes it to 930 GB.
    assertTrue(tracker.fits(moveY));
  }

  @Test
  public void testCapacityAndUsageOfLogDirs() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartition small = new TopicPartition(TOPIC, 1);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = new HashMap<>();
    logDirsByBroker.put(0, Map.of(D1, logDir(DISK, Map.of(tp, replica(100 * GB), small, replica(GB)))));
    // The capacity config (800 GB) is lower than the size of the disk.
    logDirsByBroker.put(1, Map.of(D1, logDir(DISK, Collections.emptyMap())));
    // Kafka does not report the size of the disk: the capacity config (300 GB per log dir) is used.
    logDirsByBroker.put(2, Map.of(D1, new LogDirDescription(null, Collections.emptyMap()),
                                  D2, new LogDirDescription(null, Collections.emptyMap())));
    // Files outside of the replicas take 950 GB of the disk: the usage is the size of the disk minus its usable bytes.
    logDirsByBroker.put(3, Map.of(D1, new LogDirDescription(null, Collections.emptyMap(), DISK, 50 * GB)));
    // Neither Kafka nor the capacity config tell the capacity of the log dirs: they are not checked.
    logDirsByBroker.put(4, Map.of(D1, new LogDirDescription(null, Collections.emptyMap()),
                                  D2, new LogDirDescription(null, Collections.emptyMap())));
    Map<Integer, BrokerCapacityInfo> capacityByBroker = new HashMap<>();
    capacityByBroker.put(1, capacity(800 * 1024, null));
    capacityByBroker.put(2, capacity(600 * 1024, Map.of(D1, 300.0 * 1024, D2, 300.0 * 1024)));
    capacityByBroker.put(4, capacity(600 * 1024, null));
    DiskCapacityTracker tracker = tracker(logDirsByBroker, Collections.emptySet(), capacityByBroker, Collections.emptyMap());
    tracker.beginPlanning();

    // Broker 1: 800 GB of capacity, although the disk has 1000 GB.
    assertTrue(tracker.fits(interBrokerMovement(tp, 100, 0, 1)));
    assertFalse(tracker.fits(interBrokerMovement(new TopicPartition(TOPIC, 7), 900, 0, 1)));

    // 1 GB still fits on broker 3, 100 GB does not.
    assertTrue(tracker.fits(interBrokerMovement(small, 1, 0, 3)));
    assertFalse(tracker.fits(interBrokerMovement(tp, 100, 0, 3)));

    // Broker 2: the new replica may land on either log dir, and each one has 300 GB.
    ExecutionTask moveTo2 = interBrokerMovement(tp, 100, 0, 2);
    assertTrue(tracker.fits(moveTo2));
    tracker.reserve(moveTo2);
    tracker.reserve(interBrokerMovement(small, 1, 0, 2));
    // Both log dirs are charged 101 GB until the new replicas are located.
    assertFalse(tracker.fits(interBrokerMovement(new TopicPartition(TOPIC, 9), 200, 0, 2)));

    assertTrue(tracker.fits(interBrokerMovement(new TopicPartition(TOPIC, 9), 10_000, 0, 4)));
  }

  @Test
  public void testNewReplicaLogDirPredictionAndLocation() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = new HashMap<>();
    logDirsByBroker.put(0, Map.of(D1, logDir(DISK, Map.of(tp, replica(300 * GB)))));
    // d1 has the fewest partitions, but is cordoned. d2 and d3 are tied for the fewest partitions among the others. d2 is nearly full.
    logDirsByBroker.put(1, Map.of(D1, new LogDirDescription(null, Collections.emptyMap(), DISK, DISK, true),
                                  D2, logDir(DISK, Map.of(new TopicPartition(TOPIC, 1), replica(800 * GB))),
                                  D3, logDir(DISK, Map.of(new TopicPartition(TOPIC, 2), replica(10 * GB)))));
    DiskCapacityTracker tracker = tracker(logDirsByBroker, Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap());
    tracker.beginPlanning();

    // Kafka may pick d2, which cannot hold 300 GB.
    ExecutionTask unpinned = interBrokerMovement(tp, 300, 0, 1);
    assertFalse(tracker.fits(unpinned));
    assertTrue(tracker.refusals().get(unpinned).contains("log dir /d2 of broker 1"));

    // A movement that requests d3 is only checked against d3.
    ExecutionTask pinned = pinnedInterBrokerMovement(tp, 300, 0, 1, D3);
    assertTrue(tracker.fits(pinned));
    tracker.reserve(pinned);
    // Pinning is best effort, so the new replica is located once its movement has started.
    assertTrue(tracker.newReplicasToLocate().isEmpty());
    start(pinned);
    TopicPartitionReplica newReplica = new TopicPartitionReplica(TOPIC, 0, 1);
    assertEquals(Set.of(newReplica), tracker.newReplicasToLocate());

    // The replica was created in d2 after all: d3 is freed, d2 is charged.
    tracker.locateNewReplicas(Map.of(newReplica, D2));
    assertTrue(tracker.newReplicasToLocate().isEmpty());
    // d3 (10 GB) now has fewer partitions than d2 (1100 GB): Kafka picks d3.
    assertTrue(tracker.fits(interBrokerMovement(new TopicPartition(TOPIC, 3), 900, 0, 1)));
    assertFalse(tracker.fits(pinnedInterBrokerMovement(new TopicPartition(TOPIC, 4), 1, 0, 1, D2)));
  }

  @Test
  public void testWhenNothingCanStart() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = new HashMap<>();
    logDirsByBroker.put(0, Map.of(D1, logDir(DISK, Map.of(tp, replica(300 * GB)))));
    logDirsByBroker.put(1, Map.of(D1, logDir(DISK, Map.of(new TopicPartition(TOPIC, 1), replica(800 * GB)))));
    // The log dirs of broker 5 could not be described.
    DiskCapacityTracker tracker = tracker(logDirsByBroker, Set.of(5), Collections.emptyMap(), Collections.emptyMap());

    tracker.beginPlanning();
    assertEquals(BlockedAction.NOT_BLOCKED_BY_DISK_CAPACITY, tracker.whenNothingCanStart());

    // Nothing was started since the log dirs were described: the movement does not fit the actual disk usage.
    ExecutionTask tooLarge = interBrokerMovement(tp, 300, 0, 1);
    assertFalse(tracker.fits(tooLarge));
    assertEquals(BlockedAction.FAIL, tracker.whenNothingCanStart());
    assertTrue(tracker.describeRefusals().contains("partition topic-0: it needs 307200 MB on log dir /d1 of broker 1"));

    // A movement to a broker whose log dirs could not be described does not fit.
    ExecutionTask toUnreachableBroker = interBrokerMovement(new TopicPartition(TOPIC, 2), 1, 0, 5);
    assertFalse(tracker.fits(toUnreachableBroker));
    assertTrue(tracker.refusals().get(toUnreachableBroker).contains("broker 5 could not be described"));
    // Movements to other brokers missing from the snapshot (e.g. dead brokers) are not checked.
    assertTrue(tracker.fits(interBrokerMovement(new TopicPartition(TOPIC, 2), 10_000, 0, 6)));

    // A movement was started since: describe the log dirs again before failing.
    ExecutionTask started = interBrokerMovement(new TopicPartition(TOPIC, 4), 1, 0, 1);
    tracker.reserve(started);
    assertEquals(BlockedAction.RETAKE_SNAPSHOT, tracker.whenNothingCanStart());

    // The replica replaced by a completed movement is not deleted yet.
    ExecutionTask completed = interBrokerMovement(new TopicPartition(TOPIC, 1), 800, 1, 0);
    tracker.reserve(completed);
    // A movement that does not complete does not free disk space.
    ExecutionTask dead = interBrokerMovement(tp, 300, 0, 1);
    tracker.reserve(dead);
    start(started, completed, dead);
    completed.completed(_time.milliseconds());
    dead.kill(_time.milliseconds());
    assertEquals(BlockedAction.WAIT_FOR_DELETIONS, tracker.whenNothingCanStart());

    _time.sleep(DEFAULT_LOG_SEGMENT_DELETE_DELAY_MS);
    tracker.beginPlanning();
    assertFalse(tracker.hasPendingDeletions());
    // Broker 1 is at 800 + 1 + 300 - 800 = 301 GB.
    assertTrue(tracker.fits(tooLarge));
    // Broker 0 is at 300 + 800 = 1100 GB: the replica the dead movement would have replaced is still there.
    assertFalse(tracker.fits(interBrokerMovement(new TopicPartition(TOPIC, 5), 1, 1, 0)));
  }

  private DiskCapacityTracker tracker(Map<Integer, Map<String, LogDirDescription>> logDirsByBroker,
                                      Set<Integer> unreachableBrokers,
                                      Map<Integer, BrokerCapacityInfo> capacityByBroker,
                                      Map<Integer, Long> deleteDelayMsByBroker) {
    return new DiskCapacityTracker(logDirsByBroker, unreachableBrokers, capacityByBroker, deleteDelayMsByBroker, _time);
  }

  private void start(ExecutionTask... tasks) {
    for (ExecutionTask task : tasks) {
      task.inProgress(_time.milliseconds());
    }
  }

  private ExecutionTask interBrokerMovement(TopicPartition tp, long sizeInGb, int sourceBroker, int destinationBroker) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(sourceBroker);
    return new ExecutionTask(_executionId++, proposal(tp, sizeInGb * 1024, source, List.of(source),
                                                      List.of(new ReplicaPlacementInfo(destinationBroker))),
                             INTER_BROKER_REPLICA_ACTION, ALERTING_THRESHOLD_MS);
  }

  private ExecutionTask pinnedInterBrokerMovement(TopicPartition tp, long sizeInGb, int sourceBroker, int destinationBroker,
                                                  String destinationLogDir) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(sourceBroker);
    return new ExecutionTask(_executionId++, proposal(tp, sizeInGb * 1024, source, List.of(source),
                                                      List.of(new ReplicaPlacementInfo(destinationBroker, destinationLogDir))),
                             INTER_BROKER_REPLICA_ACTION, ALERTING_THRESHOLD_MS);
  }

  private ExecutionTask intraBrokerMovement(TopicPartition tp, int broker, String sourceLogDir, String destinationLogDir) {
    ReplicaPlacementInfo source = new ReplicaPlacementInfo(broker, sourceLogDir);
    return new ExecutionTask(_executionId++, proposal(tp, 0L, source, List.of(source),
                                                      List.of(new ReplicaPlacementInfo(broker, destinationLogDir))),
                             broker, INTRA_BROKER_REPLICA_ACTION, ALERTING_THRESHOLD_MS);
  }

  private static ExecutionProposal proposal(TopicPartition tp, long sizeInMb, ReplicaPlacementInfo oldLeader,
                                            List<ReplicaPlacementInfo> oldReplicas, List<ReplicaPlacementInfo> newReplicas) {
    return new ExecutionProposal(tp, sizeInMb, oldLeader, oldReplicas, newReplicas);
  }

  private static LogDirDescription logDir(long totalBytes, Map<TopicPartition, ReplicaInfo> replicas) {
    long used = replicas.values().stream().mapToLong(ReplicaInfo::size).sum();
    return new LogDirDescription(null, replicas, totalBytes, totalBytes - used);
  }

  private static ReplicaInfo replica(long size) {
    return new ReplicaInfo(size, 0L, false);
  }

  private static BrokerCapacityInfo capacity(double diskCapacityInMb, Map<String, Double> diskCapacityByLogDirInMb) {
    Map<Resource, Double> capacity = new HashMap<>();
    capacity.put(Resource.DISK, diskCapacityInMb);
    capacity.put(Resource.CPU, 100.0);
    capacity.put(Resource.NW_IN, 10_000.0);
    capacity.put(Resource.NW_OUT, 10_000.0);
    return new BrokerCapacityInfo(capacity, diskCapacityByLogDirInMb);
  }
}
