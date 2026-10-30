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

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link LogdirSpaceCheck}.
 */
public class LogdirSpaceCheckTest {
  private static final String TOPIC = "topic";
  private static final String D1 = "/d1";
  private static final String D2 = "/d2";
  private static final int BROKER_ID_0 = 0;
  private static final int BROKER_ID_1 = 1;
  private static final long MB = 1024L * 1024L;
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  private static TopicPartition tp(int partition) {
    return new TopicPartition(TOPIC, partition);
  }

  /**
   * @param executionId Execution id of the task.
   * @param partition Partition of {@link #TOPIC}.
   * @param sizeInMb Partition size in MB.
   * @param destinationLogdir Logdir of broker 1 to move the replica to.
   * @return A task that moves the replica of the given partition on broker 0 to the given logdir of broker 1.
   */
  private static ExecutionTask interBrokerTask(long executionId, int partition, long sizeInMb, String destinationLogdir) {
    ExecutionProposal proposal = new ExecutionProposal(tp(partition), sizeInMb, new ReplicaPlacementInfo(BROKER_ID_0, D1),
                                                       List.of(new ReplicaPlacementInfo(BROKER_ID_0, D1)),
                                                       List.of(new ReplicaPlacementInfo(BROKER_ID_1, destinationLogdir)));
    return new ExecutionTask(executionId, proposal, INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  /**
   * @param executionId Execution id of the task.
   * @param partition Partition of {@link #TOPIC}.
   * @param sizeInMb Partition size in MB.
   * @param sourceLogdir Logdir of broker 1 to move the replica from.
   * @param destinationLogdir Logdir of broker 1 to move the replica to.
   * @return A task that moves the replica of the given partition on broker 1 between the given logdirs.
   */
  private static ExecutionTask intraBrokerTask(long executionId, int partition, long sizeInMb, String sourceLogdir,
                                               String destinationLogdir) {
    ExecutionProposal proposal = new ExecutionProposal(tp(partition), sizeInMb, new ReplicaPlacementInfo(BROKER_ID_1, sourceLogdir),
                                                       List.of(new ReplicaPlacementInfo(BROKER_ID_1, sourceLogdir)),
                                                       List.of(new ReplicaPlacementInfo(BROKER_ID_1, destinationLogdir)));
    return new ExecutionTask(executionId, proposal, BROKER_ID_1, INTRA_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private static ReplicaInfo replica(long sizeInMb, boolean isFuture) {
    return new ReplicaInfo(sizeInMb * MB, 0L, isFuture);
  }

  private static DescribeLogDirsResult describeLogDirsResult(Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions)
      throws ReflectiveOperationException {
    // The constructor is package private.
    Constructor<DescribeLogDirsResult> constructor = DescribeLogDirsResult.class.getDeclaredConstructor(Map.class);
    constructor.setAccessible(true);
    return constructor.newInstance(descriptions);
  }

  /**
   * @param descriptionByLogdirOfBroker1 Description of each logdir of broker 1.
   * @return An admin client that describes the logdirs of broker 1 as given, and fails to describe the logdirs of broker 0.
   */
  private static Admin adminClient(Map<String, LogDirDescription> descriptionByLogdirOfBroker1) throws ReflectiveOperationException {
    KafkaFutureImpl<Map<String, LogDirDescription>> failedFuture = new KafkaFutureImpl<>();
    failedFuture.completeExceptionally(new TimeoutException(""));
    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = new HashMap<>();
    descriptions.put(BROKER_ID_0, failedFuture);
    descriptions.put(BROKER_ID_1, KafkaFuture.completedFuture(descriptionByLogdirOfBroker1));
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(adminClient.describeLogDirs(Set.of(BROKER_ID_0, BROKER_ID_1))).andReturn(describeLogDirsResult(descriptions)).anyTimes();
    EasyMock.replay(adminClient);
    return adminClient;
  }

  private static LogdirSpaceCheck check(Admin adminClient, Map<String, Double> capacityInMbByLogdirOfBroker1) {
    return new LogdirSpaceCheck(adminClient, CONFIG, Set.of(BROKER_ID_0, BROKER_ID_1), Map.of(BROKER_ID_1, capacityInMbByLogdirOfBroker1));
  }

  @Test
  public void testHasPlannedLogdirs() {
    ExecutionProposal withoutLogdir = new ExecutionProposal(tp(0), 10, new ReplicaPlacementInfo(BROKER_ID_0),
                                                            List.of(new ReplicaPlacementInfo(BROKER_ID_0)),
                                                            List.of(new ReplicaPlacementInfo(BROKER_ID_1)));
    ExecutionTask interBrokerTaskWithoutLogdir = new ExecutionTask(0L, withoutLogdir, INTER_BROKER_REPLICA_ACTION,
                                                                   EXECUTION_ALERTING_THRESHOLD_MS);
    // Intra-broker replica movements alone (e.g. rebalance_disk) have no planned logdir for a new replica.
    assertFalse(LogdirSpaceCheck.hasPlannedLogdirs(List.of(interBrokerTaskWithoutLogdir, intraBrokerTask(1L, 1, 10, D1, D2))));
    assertTrue(LogdirSpaceCheck.hasPlannedLogdirs(List.of(interBrokerTaskWithoutLogdir, interBrokerTask(2L, 2, 10, D1))));
  }

  @Test
  public void testSourceLogdir() {
    assertEquals(D1, LogdirSpaceCheck.sourceLogdir(intraBrokerTask(0L, 0, 10, D1, D2)));
    assertNull(LogdirSpaceCheck.sourceLogdir(interBrokerTask(1L, 1, 10, D1)));
  }

  /**
   * Logdir D1 of broker 1 has 1000 MB capacity and hosts 780 MB, a native replica of 190 MB among them. Two replicas of 190 MB
   * each move to D1 (inter-broker), and the native replica moves from D1 to D2 (intra-broker): the second new replica fits
   * only once the native replica has left.
   */
  @Test
  public void testDefersMovementUntilDestinationLogdirCanHoldReplica() throws ReflectiveOperationException {
    Map<String, LogDirDescription> descriptionByLogdir = new HashMap<>();
    descriptionByLogdir.put(D1, new LogDirDescription(null, Map.of(tp(10), replica(590, false), tp(11), replica(190, false))));
    descriptionByLogdir.put(D2, new LogDirDescription(null, Map.of()));
    LogdirSpaceCheck check = check(adminClient(descriptionByLogdir), Map.of(D1, 1000.0, D2, 1000.0));
    ExecutionTask firstNewReplica = interBrokerTask(0L, 0, 190, D1);
    ExecutionTask secondNewReplica = interBrokerTask(1L, 1, 190, D1);
    ExecutionTask nativeReplicaOffD1 = intraBrokerTask(2L, 11, 190, D1, D2);

    check.refresh(List.of(), Set.of());
    assertTrue(check.tryReserve(firstNewReplica));
    assertFalse(check.hasBlockedLogdirs());
    // 30 MB are left on D1 after reserving the first new replica.
    assertFalse(check.tryReserve(secondNewReplica));
    assertTrue(check.hasBlockedLogdirs());
    assertEquals(Map.of(BROKER_ID_1, Set.of(D1)), check.blockedLogdirs());
    assertEquals("logdir /d1 of broker 1 (30 MB free, a replica needs 190 MB)", check.describeBlockedLogdirs());
    // The native replica can move to D2.
    assertTrue(check.tryReserve(nativeReplicaOffD1));

    // The first new replica is being created (50 MB so far, in its planned logdir), and the native replica is being moved (its
    // future replica in D2 has 100 MB so far). The rest of them are reserved upon refresh, which clears the blocked logdirs.
    descriptionByLogdir.put(D1, new LogDirDescription(null, Map.of(tp(10), replica(590, false), tp(11), replica(190, false),
                                                                   tp(0), replica(50, false))));
    descriptionByLogdir.put(D2, new LogDirDescription(null, Map.of(tp(11), replica(100, true))));
    check.refresh(List.of(firstNewReplica, nativeReplicaOffD1), Set.of(new TopicPartitionReplica(TOPIC, 0, BROKER_ID_1)));
    assertFalse(check.hasBlockedLogdirs());
    assertFalse(check.tryReserve(secondNewReplica));
    // D2 has 1000 - 100 - 90 MB free.
    assertEquals("logdir /d1 of broker 1 (30 MB free, a replica needs 190 MB)", check.describeBlockedLogdirs());

    // Once the native replica has left D1, the second new replica fits.
    descriptionByLogdir.put(D1, new LogDirDescription(null, Map.of(tp(10), replica(590, false), tp(0), replica(190, false))));
    descriptionByLogdir.put(D2, new LogDirDescription(null, Map.of(tp(11), replica(190, false))));
    check.refresh(List.of(), Set.of());
    assertTrue(check.tryReserve(secondNewReplica));
  }

  @Test
  public void testReservesNewReplicaInRecordedLogdirUntilCreated() throws ReflectiveOperationException {
    LogdirSpaceCheck check = check(adminClient(Map.of(D1, new LogDirDescription(null, Map.of()))), Map.of(D1, 300.0));
    ExecutionTask inProgress = interBrokerTask(0L, 0, 200, D1);
    ExecutionTask pending = interBrokerTask(1L, 1, 200, D1);
    // The replica in progress is not created yet: its full size is reserved in its recorded logdir.
    check.refresh(List.of(inProgress), Set.of(new TopicPartitionReplica(TOPIC, 0, BROKER_ID_1)));
    assertFalse(check.tryReserve(pending));
    // If its logdir was not recorded, the broker picks the logdir to create it in: nothing is reserved until it is created.
    check.refresh(List.of(inProgress), Set.of());
    assertTrue(check.tryReserve(pending));
  }

  @Test
  public void testUsesSmallerOfConfiguredCapacityAndUsableBytes() throws ReflectiveOperationException {
    ExecutionTask task = interBrokerTask(0L, 0, 200, D1);
    // Configured capacity leaves 1000 MB, but the broker reports 100 MB usable.
    Admin adminClient = adminClient(Map.of(D1, new LogDirDescription(null, Map.of(), 2000 * MB, 100 * MB)));
    LogdirSpaceCheck check = check(adminClient, Map.of(D1, 1000.0));
    check.refresh(List.of(), Set.of());
    assertFalse(check.tryReserve(task));
    // Without a configured capacity, the usable bytes apply alone.
    check = check(adminClient, Map.of());
    check.refresh(List.of(), Set.of());
    assertFalse(check.tryReserve(task));
    // Without either, the logdir is not checked.
    check = check(adminClient(Map.of(D1, new LogDirDescription(null, Map.of()))), Map.of());
    check.refresh(List.of(), Set.of());
    assertTrue(check.tryReserve(task));
  }

  @Test
  public void testUsesLargestReplicaOfPartitionAsReplicaSize() throws ReflectiveOperationException {
    // The replica of partition 0 on broker 1 (e.g. the leader) has grown to 400 MB since the proposal (100 MB) was generated.
    Map<String, LogDirDescription> descriptionByLogdir = Map.of(D1, new LogDirDescription(null, Map.of(tp(0), replica(400, false))),
                                                                D2, new LogDirDescription(null, Map.of()));
    LogdirSpaceCheck check = check(adminClient(descriptionByLogdir), Map.of(D1, 1000.0, D2, 300.0));
    check.refresh(List.of(), Set.of());
    assertFalse(check.tryReserve(intraBrokerTask(0L, 0, 100, D1, D2)));
  }

  @Test
  public void testDoesNotCheckBrokersWithUnknownLogdirs() throws ReflectiveOperationException {
    // Broker 0 fails to describe its logdirs: a movement to it starts without a check.
    LogdirSpaceCheck check = check(adminClient(Map.of(D1, new LogDirDescription(null, Map.of()))), Map.of(D1, 1.0));
    ExecutionProposal proposal = new ExecutionProposal(tp(0), 200, new ReplicaPlacementInfo(BROKER_ID_1, D1),
                                                       List.of(new ReplicaPlacementInfo(BROKER_ID_1, D1)),
                                                       List.of(new ReplicaPlacementInfo(BROKER_ID_0, D1)));
    check.refresh(List.of(), Set.of());
    assertTrue(check.tryReserve(new ExecutionTask(0L, proposal, INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS)));
  }
}
