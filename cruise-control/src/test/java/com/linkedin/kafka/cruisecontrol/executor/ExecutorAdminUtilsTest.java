/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.BYTES_IN_MB;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test class for {@link ExecutorAdminUtils}.
 */
public class ExecutorAdminUtilsTest {
  private static final TopicPartition TP = new TopicPartition(TOPIC0, 0);
  private static final String LOGDIR_0 = "d0";
  private static final String LOGDIR_1 = "d1";
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  private static final long PARTITION_SIZE_IN_MB = 10L;
  private static final List<Node> NODES = Arrays.asList(new Node(0, "host0", 100),
                                                        new Node(1, "host1", 100),
                                                        new Node(2, "host2", 100),
                                                        new Node(3, "host3", 100));
  private static final ReplicaPlacementInfo R0 = new ReplicaPlacementInfo(0);
  private static final ReplicaPlacementInfo R1 = new ReplicaPlacementInfo(1);
  private static final ReplicaPlacementInfo R2 = new ReplicaPlacementInfo(2);
  private static final ReplicaPlacementInfo R3 = new ReplicaPlacementInfo(3);

  @Test
  public void testCancelIntraBrokerReplicaMovements() throws Exception {
    // Move replicas on brokers 0, 1, and 2 from d0 to d1.
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(2, LOGDIR_0)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(2, LOGDIR_1)));
    ExecutionTask task0 = intraBrokerTask(0, proposal, 0);
    ExecutionTask task1 = intraBrokerTask(1, proposal, 1);
    ExecutionTask task2 = intraBrokerTask(2, proposal, 2);
    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TP.topic(), TP.partition(), 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TP.topic(), TP.partition(), 1);

    // Movements of replicas on brokers 0 and 1 are in progress. Logdir information of the replica on broker 2 is unknown.
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask = new HashMap<>();
    logdirInfoByTask.put(task0, replicaLogDirInfo(LOGDIR_0, LOGDIR_1));
    logdirInfoByTask.put(task1, replicaLogDirInfo(LOGDIR_0, LOGDIR_1));

    // Cancellation of the movement on broker 0 succeeds, whereas the one on broker 1 fails.
    KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
    failedFuture.completeExceptionally(new KafkaStorageException("Disk failure"));
    Map<TopicPartitionReplica, KafkaFuture<Void>> futureByReplica = new HashMap<>();
    futureByReplica.put(tpr0, KafkaFuture.completedFuture(null));
    futureByReplica.put(tpr1, failedFuture);
    AlterReplicaLogDirsResult mockResult = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    Capture<Map<TopicPartitionReplica, String>> capturedAssignment = Capture.newInstance();
    EasyMock.expect(mockAdminClient.alterReplicaLogDirs(EasyMock.capture(capturedAssignment))).andReturn(mockResult);
    EasyMock.replay(mockAdminClient, mockResult);

    Set<ExecutionTask> cancelledTasks =
        ExecutorAdminUtils.cancelIntraBrokerReplicaMovements(Arrays.asList(task0, task1, task2), logdirInfoByTask, mockAdminClient, CONFIG);

    // Replicas with known logdir are requested to move back to their current logdir to rollback the ongoing movement.
    Map<TopicPartitionReplica, String> expectedAssignment = new HashMap<>();
    expectedAssignment.put(tpr0, LOGDIR_0);
    expectedAssignment.put(tpr1, LOGDIR_0);
    assertEquals(expectedAssignment, capturedAssignment.getValue());
    // Only the task whose cancellation has been accepted is reported as cancelled.
    assertEquals(Collections.singleton(task0), cancelledTasks);
    EasyMock.verify(mockAdminClient, mockResult);
  }

  @Test
  public void testCancelIntraBrokerReplicaMovementsWithUnknownLogdirs() {
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR_0)),
                                                       List.of(new ReplicaPlacementInfo(0, LOGDIR_1)));
    ExecutionTask task = intraBrokerTask(0, proposal, 0);

    // No request is sent to the cluster if there is no replica with known logdir.
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.replay(mockAdminClient);

    Set<ExecutionTask> cancelledTasks =
        ExecutorAdminUtils.cancelIntraBrokerReplicaMovements(Collections.singletonList(task), Collections.emptyMap(), mockAdminClient, CONFIG);

    assertTrue(cancelledTasks.isEmpty());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testGetLogdirInfoForExecutionTaskSkipsReplicaWithOfflineLogdir() throws Exception {
    // Move the replicas on brokers 0 and 1 from d0 to d1. Broker 1's logdir is offline.
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_0)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_1)));
    ExecutionTask taskWithOnlineLogdir = intraBrokerTask(0, proposal, 0);
    ExecutionTask taskWithOfflineLogdir = intraBrokerTask(1, proposal, 1);
    TopicPartitionReplica tpr0 = new TopicPartitionReplica(TP.topic(), TP.partition(), 0);
    TopicPartitionReplica tpr1 = new TopicPartitionReplica(TP.topic(), TP.partition(), 1);

    // A replica on an offline logdir is reported with a SUCCESSFUL future that carries no logdir at all -- not with a
    // failed future -- so the response has to be filtered on its content rather than on the future completing.
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
    futureByReplica.put(tpr0, KafkaFuture.completedFuture(replicaLogDirInfo(LOGDIR_0, LOGDIR_1)));
    futureByReplica.put(tpr1, KafkaFuture.completedFuture(offlineReplicaLogDirInfo()));
    DescribeReplicaLogDirsResult mockResult = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeReplicaLogDirs(EasyMock.anyObject())).andReturn(mockResult);
    EasyMock.replay(mockAdminClient, mockResult);

    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask =
        ExecutorAdminUtils.getLogdirInfoForExecutionTask(Arrays.asList(taskWithOnlineLogdir, taskWithOfflineLogdir),
                                                         mockAdminClient, CONFIG);

    // Only the replica with a known current logdir is reported. Reporting the other one would make
    // ExecutionUtils#isIntraBrokerReplicaActionDone dereference a null logdir, and would hide the task from
    // Executor#maybeMarkTaskAsDead, whose only condition for an intra-broker task is its absence from this map.
    assertEquals(Collections.singleton(taskWithOnlineLogdir), logdirInfoByTask.keySet());
    assertEquals(LOGDIR_0, logdirInfoByTask.get(taskWithOnlineLogdir).getCurrentReplicaLogDir());
    EasyMock.verify(mockAdminClient, mockResult);
  }

  @Test
  public void testGetLogdirInfoForExecutionTaskTellsApartTasksWithoutResponse() throws Exception {
    // Move the replicas on brokers 0-3 from d0 to d1.
    List<ReplicaPlacementInfo> oldReplicas = new ArrayList<>();
    List<ReplicaPlacementInfo> newReplicas = new ArrayList<>();
    for (int brokerId = 0; brokerId < 4; brokerId++) {
      oldReplicas.add(new ReplicaPlacementInfo(brokerId, LOGDIR_0));
      newReplicas.add(new ReplicaPlacementInfo(brokerId, LOGDIR_1));
    }
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0), oldReplicas, newReplicas);
    List<ExecutionTask> tasks = new ArrayList<>();
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
    for (int brokerId = 0; brokerId < 4; brokerId++) {
      tasks.add(intraBrokerTask(brokerId, proposal, brokerId));
    }
    // Broker 0 responds with the logdirs of its replica, broker 1 responds that the logdir of its replica is offline, no
    // response arrives in time from broker 2, and broker 3 responds with an error.
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 0), KafkaFuture.completedFuture(replicaLogDirInfo(LOGDIR_0, LOGDIR_1)));
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 1), KafkaFuture.completedFuture(offlineReplicaLogDirInfo()));
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 2),
                        failedFuture(new TimeoutException("Timed out waiting for a node assignment. Call: describeReplicaLogDirs")));
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 3), failedFuture(new UnknownServerException("Unknown")));
    DescribeReplicaLogDirsResult mockResult = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeReplicaLogDirs(EasyMock.anyObject())).andReturn(mockResult);
    EasyMock.replay(mockAdminClient, mockResult);

    Set<ExecutionTask> tasksWithoutResponse = new HashSet<>();
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask =
        ExecutorAdminUtils.getLogdirInfoForExecutionTask(tasks, mockAdminClient, CONFIG, tasksWithoutResponse);

    // Only the task of broker 2 has an unknown state. The tasks of brokers 1 and 3 got a response without a current logdir.
    assertEquals(Collections.singleton(tasks.get(0)), logdirInfoByTask.keySet());
    assertEquals(Collections.singleton(tasks.get(2)), tasksWithoutResponse);
    EasyMock.verify(mockAdminClient, mockResult);
  }

  @Test
  public void testGetLogdirInfoForExecutionTaskWaitsForAllBrokersAtOnce() {
    // Move the replicas on brokers 0-2 from d0 to d1. No broker responds.
    List<ReplicaPlacementInfo> oldReplicas = new ArrayList<>();
    List<ReplicaPlacementInfo> newReplicas = new ArrayList<>();
    for (int brokerId = 0; brokerId < 3; brokerId++) {
      oldReplicas.add(new ReplicaPlacementInfo(brokerId, LOGDIR_0));
      newReplicas.add(new ReplicaPlacementInfo(brokerId, LOGDIR_1));
    }
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0), oldReplicas, newReplicas);
    List<ExecutionTask> tasks = new ArrayList<>();
    Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futureByReplica = new HashMap<>();
    for (int brokerId = 0; brokerId < 3; brokerId++) {
      tasks.add(intraBrokerTask(brokerId, proposal, brokerId));
      futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), brokerId), new KafkaFutureImpl<>());
    }
    DescribeReplicaLogDirsResult mockResult = EasyMock.mock(DescribeReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeReplicaLogDirs(EasyMock.anyObject())).andReturn(mockResult);
    EasyMock.replay(mockAdminClient, mockResult);

    long timeoutMs = 300L;
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG, Long.toString(timeoutMs));
    Set<ExecutionTask> tasksWithoutResponse = new HashSet<>();
    long startMs = System.currentTimeMillis();
    Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask =
        ExecutorAdminUtils.getLogdirInfoForExecutionTask(tasks, mockAdminClient, new KafkaCruiseControlConfig(props), tasksWithoutResponse);
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertTrue(logdirInfoByTask.isEmpty());
    assertEquals(new HashSet<>(tasks), tasksWithoutResponse);
    // The requests to the brokers are in flight at the same time: the timeout is not waited for each broker in turn.
    assertTrue("Waited " + elapsedMs + " ms", elapsedMs < tasks.size() * timeoutMs);
    EasyMock.verify(mockAdminClient, mockResult);
  }

  @Test
  public void testExecuteIntraBrokerReplicaMovementsKeepsTasksWithoutResponseInProgress() {
    // Move the replicas on brokers 0-2 from d0 to d1.
    ExecutionProposal proposal = new ExecutionProposal(TP, 10, new ReplicaPlacementInfo(0, LOGDIR_0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_0),
                                                                     new ReplicaPlacementInfo(2, LOGDIR_0)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(1, LOGDIR_1),
                                                                     new ReplicaPlacementInfo(2, LOGDIR_1)));
    ExecutionTask acceptedTask = intraBrokerTask(0, proposal, 0);
    ExecutionTask taskWithoutResponse = intraBrokerTask(1, proposal, 1);
    ExecutionTask refusedTask = intraBrokerTask(2, proposal, 2);

    // Broker 0 accepts the movement, no response arrives in time from broker 1, and broker 2 refuses the movement.
    Map<TopicPartitionReplica, KafkaFuture<Void>> futureByReplica = new HashMap<>();
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 0), KafkaFuture.completedFuture(null));
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 1),
                        failedFuture(new TimeoutException("Timed out waiting for a node assignment. Call: alterReplicaLogDirs")));
    futureByReplica.put(new TopicPartitionReplica(TP.topic(), TP.partition(), 2), failedFuture(new KafkaStorageException("Disk failure")));
    AlterReplicaLogDirsResult mockResult = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(mockResult.values()).andReturn(futureByReplica);
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.alterReplicaLogDirs(EasyMock.anyObject())).andReturn(mockResult);
    // Only the refused task is marked as dead: the movement of the other one may have started.
    ExecutionTaskManager mockExecutionTaskManager = EasyMock.mock(ExecutionTaskManager.class);
    mockExecutionTaskManager.markTaskAborting(refusedTask);
    mockExecutionTaskManager.markTaskDead(refusedTask);
    EasyMock.replay(mockAdminClient, mockResult, mockExecutionTaskManager);

    ExecutorAdminUtils.executeIntraBrokerReplicaMovements(Arrays.asList(acceptedTask, taskWithoutResponse, refusedTask),
                                                          mockAdminClient, mockExecutionTaskManager, CONFIG);

    EasyMock.verify(mockAdminClient, mockResult, mockExecutionTaskManager);
  }

  private static <T> KafkaFuture<T> failedFuture(Throwable failure) {
    KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
    future.completeExceptionally(failure);
    return future;
  }

  private static ExecutionTask intraBrokerTask(long executionId, ExecutionProposal proposal, int brokerId) {
    ExecutionTask task = new ExecutionTask(executionId, proposal, brokerId, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION,
                                           EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(0L);
    return task;
  }

  private static ReplicaLogDirInfo offlineReplicaLogDirInfo() throws Exception {
    // What the admin client hands back for a replica whose logdir is offline: every field unset.
    Constructor<ReplicaLogDirInfo> constructor = ReplicaLogDirInfo.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static ReplicaLogDirInfo replicaLogDirInfo(String currentLogdir, String futureLogdir) throws Exception {
    // Reflectively set constructor from package private to public.
    Constructor<ReplicaLogDirInfo> constructor =
        ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class, long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(currentLogdir, 0L, futureLogdir, 0L);
  }

  /**
   * @return A task to move the partition from brokers [0, 1] (leader: 0) to brokers [2, 3].
   */
  private static ExecutionTask interBrokerReplicaTask() {
    ExecutionProposal proposal = new ExecutionProposal(TP, PARTITION_SIZE_IN_MB, R0, Arrays.asList(R0, R1), Arrays.asList(R2, R3));
    return new ExecutionTask(0L, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  /**
   * @return A task to move the replica of the partition on broker 0 from logdir /d1 to logdir /d2.
   */
  private static ExecutionTask intraBrokerReplicaTask() {
    ExecutionProposal proposal = new ExecutionProposal(TP, PARTITION_SIZE_IN_MB, new ReplicaPlacementInfo(0, "/d1"),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, "/d1"), new ReplicaPlacementInfo(1, "/d1")),
                                                       Arrays.asList(new ReplicaPlacementInfo(0, "/d2"), new ReplicaPlacementInfo(1, "/d1")));
    return new ExecutionTask(1L, proposal, 0, ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
  }

  private static Cluster cluster(Node leader) {
    Node[] replicas = {NODES.get(0), NODES.get(1)};
    PartitionInfo partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), leader, replicas, replicas);
    return new Cluster("cluster", NODES, Collections.singleton(partitionInfo), Collections.emptySet(), Collections.emptySet());
  }

  private static Map<String, LogDirDescription> logDir(String path, long sizeInMB, boolean isFuture) {
    return Collections.singletonMap(path, new LogDirDescription(null,
        Collections.singletonMap(TP, new ReplicaInfo(sizeInMB * BYTES_IN_MB, 0L, isFuture))));
  }

  private static Map<String, LogDirDescription> offlineLogDir(String path) {
    return Collections.singletonMap(path, new LogDirDescription(new KafkaStorageException("Offline"), Collections.emptyMap()));
  }

  @Test
  public void testUpdateInterBrokerReplicaMovementProgress() {
    ExecutionTask task = interBrokerReplicaTask();
    task.inProgress(0L);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = new HashMap<>();
    logDirsByBrokerId.put(0, logDir("/d1", 300L, false));
    logDirsByBrokerId.put(2, logDir("/d1", 100L, false));
    // The replica has not been created on broker 3 yet.
    logDirsByBrokerId.put(3, Collections.singletonMap("/d1", new LogDirDescription(null, Collections.emptyMap())));

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 0L);
    assertTrue(task.hasDataMovementProgress());
    assertEquals(100L * BYTES_IN_MB, task.movedDataInBytes());
    // Leader replica size (300 MB) for each of the two replicas being created.
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());
    // The data movement rate is unknown until the progress is observed twice.
    assertFalse(task.hasDataMovementRate());
    Map<String, Object> taskJson = task.getJsonStructure();
    assertEquals(100L, taskJson.get("movedDataInMB"));
    assertEquals(600L, taskJson.get("totalDataToMoveInMB"));
    assertFalse(taskJson.containsKey("dataMovementRateInMBPerSec"));

    // 60 MB are moved in 60 seconds.
    logDirsByBrokerId.put(3, logDir("/d1", 60L, false));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 60_000L);
    assertEquals(60_000L, task.getJsonStructure().get("dataMovementProgressUpdateTimeMs"));
    assertEquals(160L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(1.0, task.dataMovementRateInMBPerSec(), 1e-9);
    assertEquals(1.0, (double) task.getJsonStructure().get("dataMovementRateInMBPerSec"), 1e-9);

    // The partition has no leader (e.g. the leader broker is offline): keep the last known total.
    logDirsByBrokerId.put(3, logDir("/d1", 90L, false));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(null), logDirsByBrokerId, 90_000L);
    assertEquals(190L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());

    // A destination broker fails to respond (e.g. it is offline): keep the last known progress and rate.
    logDirsByBrokerId.remove(3);
    logDirsByBrokerId.put(2, logDir("/d1", 200L, false));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 120_000L);
    assertEquals(190L * BYTES_IN_MB, task.movedDataInBytes());
    // The time of the last update tells that the progress is stale.
    assertEquals(90_000L, task.getJsonStructure().get("dataMovementProgressUpdateTimeMs"));
    assertEquals(1.0, task.dataMovementRateInMBPerSec(), 1e-9);

    // The logdir of a destination broker is offline: the replica may be on it, so no progress is made since the last observation.
    logDirsByBrokerId.put(3, offlineLogDir("/d1"));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 150_000L);
    assertEquals(190L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(0.0, task.dataMovementRateInMBPerSec(), 1e-9);

    // Progress is not reported once the task is no longer in progress.
    task.completed(1L);
    assertFalse(task.getJsonStructure().containsKey("movedDataInMB"));
    assertFalse(task.getJsonStructure().containsKey("dataMovementRateInMBPerSec"));
  }

  @Test
  public void testFallBackToEstimatedPartitionSizeWithoutLeader() {
    ExecutionTask task = interBrokerReplicaTask();
    task.inProgress(0L);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = new HashMap<>();
    logDirsByBrokerId.put(2, logDir("/d1", 1L, false));
    logDirsByBrokerId.put(3, logDir("/d1", 2L, false));

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(null), logDirsByBrokerId, 0L);
    assertEquals(3L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(2 * PARTITION_SIZE_IN_MB * BYTES_IN_MB, task.totalDataToMoveInBytes());
  }

  @Test
  public void testUpdateIntraBrokerReplicaMovementProgress() {
    ExecutionTask task = intraBrokerReplicaTask();
    task.inProgress(0L);
    Map<String, LogDirDescription> logDirs = new HashMap<>(logDir("/d1", 50L, false));
    // The future replica has not been created on the destination disk yet.
    logDirs.put("/d2", new LogDirDescription(null, Collections.emptyMap()));
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = Collections.singletonMap(0, logDirs);

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 0L);
    assertEquals(0L, task.movedDataInBytes());
    assertEquals(50L * BYTES_IN_MB, task.totalDataToMoveInBytes());

    // 20 MB are moved in 10 seconds.
    logDirs.putAll(logDir("/d2", 20L, true));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 10_000L);
    assertEquals(20L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(50L * BYTES_IN_MB, task.totalDataToMoveInBytes());
    assertEquals(2.0, task.dataMovementRateInMBPerSec(), 1e-9);
    Map<String, Object> taskJson = task.getJsonStructure();
    assertEquals(20L, taskJson.get("movedDataInMB"));
    assertEquals(50L, taskJson.get("totalDataToMoveInMB"));

    // The destination disk goes offline: the future replica may be on it, so no progress is made since the last observation.
    logDirs.putAll(offlineLogDir("/d2"));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), logDirsByBrokerId, 20_000L);
    assertEquals(20L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(0.0, task.dataMovementRateInMBPerSec(), 1e-9);
  }

  @Test
  public void testIntraBrokerReplicaMovementCompletedBetweenChecks() {
    ExecutionTask task = intraBrokerReplicaTask();
    task.inProgress(0L);
    Map<String, LogDirDescription> logDirs = new HashMap<>(logDir("/d1", 50L, false));
    logDirs.putAll(logDir("/d2", 40L, true));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), Collections.singletonMap(0, logDirs), 0L);
    assertEquals(40L * BYTES_IN_MB, task.movedDataInBytes());

    // The future replica has replaced the current replica on the destination disk: all data has been moved.
    Map<String, LogDirDescription> logDirsAfterCompletion = new HashMap<>(logDir("/d2", 50L, false));
    logDirsAfterCompletion.put("/d1", new LogDirDescription(null, Collections.emptyMap()));
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)),
                                                  Collections.singletonMap(0, logDirsAfterCompletion), 10_000L);
    assertEquals(50L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(50L * BYTES_IN_MB, task.totalDataToMoveInBytes());
  }

  @Test
  public void testIgnoreTasksNotInProgress() {
    ExecutionTask pendingTask = interBrokerReplicaTask();
    ExecutionProposal leaderProposal = new ExecutionProposal(TP, PARTITION_SIZE_IN_MB, R0, Arrays.asList(R0, R1), Arrays.asList(R1, R0));
    ExecutionTask leaderTask = new ExecutionTask(2L, leaderProposal, ExecutionTask.TaskType.LEADER_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    leaderTask.inProgress(0L);
    Map<Integer, Map<String, LogDirDescription>> logDirsByBrokerId = new HashMap<>();
    for (int brokerId = 0; brokerId < NODES.size(); brokerId++) {
      logDirsByBrokerId.put(brokerId, logDir("/d1", 100L, false));
    }

    ExecutorAdminUtils.updateDataMovementProgress(Arrays.asList(pendingTask, leaderTask), cluster(NODES.get(0)), logDirsByBrokerId, 0L);
    assertFalse(pendingTask.hasDataMovementProgress());
    assertFalse(leaderTask.hasDataMovementProgress());
    assertFalse(pendingTask.getJsonStructure().containsKey("movedDataInMB"));
  }

  @Test
  public void testUpdateDataMovementProgressWithAdminClient() {
    ExecutionTask task = interBrokerReplicaTask();
    task.inProgress(0L);

    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futureByBrokerId = new HashMap<>();
    futureByBrokerId.put(0, KafkaFuture.completedFuture(logDir("/d1", 300L, false)));
    futureByBrokerId.put(2, KafkaFuture.completedFuture(logDir("/d1", 100L, false)));
    futureByBrokerId.put(3, KafkaFuture.completedFuture(logDir("/d1", 40L, false)));
    DescribeLogDirsResult describeLogDirsResult = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(describeLogDirsResult.descriptions()).andReturn(futureByBrokerId);
    Admin adminClient = EasyMock.mock(Admin.class);
    // Only the leader and the destination brokers are described.
    EasyMock.expect(adminClient.describeLogDirs(EasyMock.eq(new HashSet<>(Arrays.asList(0, 2, 3)))))
            .andReturn(describeLogDirsResult);
    EasyMock.replay(describeLogDirsResult, adminClient);

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), adminClient, config, 0L);
    EasyMock.verify(describeLogDirsResult, adminClient);
    assertEquals(140L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());

    // A destination broker fails to respond: keep the last known progress.
    KafkaFutureImpl<Map<String, LogDirDescription>> failedFuture = new KafkaFutureImpl<>();
    failedFuture.completeExceptionally(new TimeoutException("Timed out"));
    futureByBrokerId.put(2, KafkaFuture.completedFuture(logDir("/d1", 200L, false)));
    futureByBrokerId.put(3, failedFuture);
    EasyMock.reset(describeLogDirsResult, adminClient);
    EasyMock.expect(describeLogDirsResult.descriptions()).andReturn(futureByBrokerId);
    EasyMock.expect(adminClient.describeLogDirs(EasyMock.anyObject())).andReturn(describeLogDirsResult);
    EasyMock.replay(describeLogDirsResult, adminClient);

    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), adminClient, config, 1L);
    EasyMock.verify(describeLogDirsResult, adminClient);
    assertEquals(140L * BYTES_IN_MB, task.movedDataInBytes());
    assertEquals(600L * BYTES_IN_MB, task.totalDataToMoveInBytes());

    // The describeLogDirs request fails altogether: keep the last known progress.
    EasyMock.reset(describeLogDirsResult, adminClient);
    EasyMock.expect(adminClient.describeLogDirs(EasyMock.anyObject())).andThrow(new IllegalStateException("Admin client is closed"));
    EasyMock.replay(adminClient);
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(task), cluster(NODES.get(0)), adminClient, config, 2L);
    EasyMock.verify(adminClient);
    assertEquals(140L * BYTES_IN_MB, task.movedDataInBytes());
  }

  @Test
  public void testNoDescribeLogDirsRequestWithoutInProgressReplicaTasks() {
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.replay(adminClient);
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    ExecutorAdminUtils.updateDataMovementProgress(Collections.singleton(interBrokerReplicaTask()), cluster(NODES.get(0)), adminClient, config, 0L);
    // No describeLogDirs request is sent, since the only task is pending.
    EasyMock.verify(adminClient);
  }
}
