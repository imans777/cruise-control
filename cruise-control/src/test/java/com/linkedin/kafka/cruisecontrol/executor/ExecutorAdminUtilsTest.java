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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.EXECUTION_ALERTING_THRESHOLD_MS;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ExecutorAdminUtils}.
 */
public class ExecutorAdminUtilsTest {
  private static final String TOPIC = "topic";
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  private static ExecutionTask interBrokerReplicaMovementTask(long executionId, ExecutionProposal proposal) {
    ExecutionTask task = new ExecutionTask(executionId, proposal, INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(0L);
    return task;
  }

  private static KafkaFuture<Void> failedFuture(Throwable exception) {
    KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
    future.completeExceptionally(exception);
    return future;
  }

  private static KafkaFuture<ReplicaLogDirInfo> failedReplicaLogDirInfoFuture(Throwable exception) {
    KafkaFutureImpl<ReplicaLogDirInfo> future = new KafkaFutureImpl<>();
    future.completeExceptionally(exception);
    return future;
  }

  /**
   * @param currentLogdir The current logdir of a replica, {@code null} if the broker does not host the replica.
   * @return Logdir information of a replica with the given current logdir and no future replica.
   */
  static ReplicaLogDirInfo replicaLogDirInfo(String currentLogdir) throws ReflectiveOperationException {
    // The constructor is package private.
    Constructor<ReplicaLogDirInfo> constructor = ReplicaLogDirInfo.class.getDeclaredConstructor(String.class, long.class, String.class,
                                                                                                long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(currentLogdir, currentLogdir == null ? -1L : 0L, null, -1L);
  }

  private static DescribeReplicaLogDirsResult describeReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> values)
      throws ReflectiveOperationException {
    // The constructor is package private.
    Constructor<DescribeReplicaLogDirsResult> constructor = DescribeReplicaLogDirsResult.class.getDeclaredConstructor(Map.class);
    constructor.setAccessible(true);
    return constructor.newInstance(values);
  }

  @Test
  public void testSetLogdirsOfReplicasToAdd() throws ReflectiveOperationException {
    // The replica on broker 1 moves to logdir d1 of broker 2.
    ExecutionProposal proposal0 = new ExecutionProposal(new TopicPartition(TOPIC, 0), 10, new ReplicaPlacementInfo(0, "d0"),
                                                        List.of(new ReplicaPlacementInfo(0, "d0"), new ReplicaPlacementInfo(1, "d0")),
                                                        List.of(new ReplicaPlacementInfo(0, "d0"), new ReplicaPlacementInfo(2, "d1")));
    // The replicas on brokers 0 and 1 move to logdir d0 of broker 3 and logdir d1 of broker 4, respectively.
    ExecutionProposal proposal1 = new ExecutionProposal(new TopicPartition(TOPIC, 1), 10, new ReplicaPlacementInfo(0, "d0"),
                                                        List.of(new ReplicaPlacementInfo(0, "d0"), new ReplicaPlacementInfo(1, "d1")),
                                                        List.of(new ReplicaPlacementInfo(3, "d0"), new ReplicaPlacementInfo(4, "d1")));
    TopicPartitionReplica replica0OnBroker2 = new TopicPartitionReplica(TOPIC, 0, 2);
    TopicPartitionReplica replica1OnBroker3 = new TopicPartitionReplica(TOPIC, 1, 3);
    TopicPartitionReplica replica1OnBroker4 = new TopicPartitionReplica(TOPIC, 1, 4);
    Set<TopicPartitionReplica> replicasToAdd = Set.of(replica0OnBroker2, replica1OnBroker3, replica1OnBroker4);

    // None of the replicas exists yet. Brokers respond with ReplicaNotAvailableException for replicas that do not exist yet.
    // Failures to set a logdir (e.g. due to an offline logdir) are tolerated, since the broker picks the logdir of the replica in
    // that case.
    AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(result.values()).andReturn(Map.of(replica0OnBroker2, failedFuture(new ReplicaNotAvailableException("")),
                                                      replica1OnBroker3, KafkaFuture.completedFuture(null),
                                                      replica1OnBroker4, failedFuture(new KafkaStorageException(""))));
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(adminClient.describeReplicaLogDirs(replicasToAdd))
            .andReturn(describeReplicaLogDirsResult(Map.of(replica0OnBroker2, KafkaFuture.completedFuture(replicaLogDirInfo(null)),
                                                           replica1OnBroker3, KafkaFuture.completedFuture(replicaLogDirInfo(null)),
                                                           replica1OnBroker4, KafkaFuture.completedFuture(replicaLogDirInfo(null)))));
    EasyMock.expect(adminClient.alterReplicaLogDirs(Map.of(replica0OnBroker2, "d1", replica1OnBroker3, "d0", replica1OnBroker4, "d1")))
            .andReturn(result);
    EasyMock.replay(result, adminClient);

    // Only the logdir of the replica that the broker responded with ReplicaNotAvailableException for is recorded.
    assertEquals(Set.of(replica0OnBroker2),
                 ExecutorAdminUtils.setLogdirsOfReplicasToAdd(List.of(interBrokerReplicaMovementTask(0L, proposal0),
                                                                      interBrokerReplicaMovementTask(1L, proposal1)),
                                                              adminClient, CONFIG));
    EasyMock.verify(result, adminClient);
  }

  @Test
  public void testSetLogdirsOfReplicasToAddSkipsExistingReplicas() throws ReflectiveOperationException {
    // The replicas on brokers 0 and 1 move to logdir d0 of broker 3 and logdir d1 of broker 4, respectively.
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC, 1), 10, new ReplicaPlacementInfo(0, "d0"),
                                                       List.of(new ReplicaPlacementInfo(0, "d0"), new ReplicaPlacementInfo(1, "d1")),
                                                       List.of(new ReplicaPlacementInfo(3, "d0"), new ReplicaPlacementInfo(4, "d1")));
    TopicPartitionReplica replicaOnBroker3 = new TopicPartitionReplica(TOPIC, 1, 3);
    TopicPartitionReplica replicaOnBroker4 = new TopicPartitionReplica(TOPIC, 1, 4);

    // Broker 3 already hosts the replica (e.g. the cluster changed after the proposal was generated): requesting a logdir for it
    // would move it to that logdir. Hence, the logdir of the replica on broker 4 is requested only.
    AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(result.values()).andReturn(Map.of(replicaOnBroker4, failedFuture(new ReplicaNotAvailableException(""))));
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(adminClient.describeReplicaLogDirs(Set.of(replicaOnBroker3, replicaOnBroker4)))
            .andReturn(describeReplicaLogDirsResult(Map.of(replicaOnBroker3, KafkaFuture.completedFuture(replicaLogDirInfo("d1")),
                                                           replicaOnBroker4, KafkaFuture.completedFuture(replicaLogDirInfo(null)))));
    EasyMock.expect(adminClient.alterReplicaLogDirs(Map.of(replicaOnBroker4, "d1"))).andReturn(result);
    EasyMock.replay(result, adminClient);

    assertEquals(Set.of(replicaOnBroker4),
                 ExecutorAdminUtils.setLogdirsOfReplicasToAdd(List.of(interBrokerReplicaMovementTask(0L, proposal)), adminClient, CONFIG));
    EasyMock.verify(result, adminClient);

    // If the existence of a replica cannot be checked, its logdir is not requested either. No request is expected if no logdir is
    // left to request.
    EasyMock.reset(adminClient);
    EasyMock.expect(adminClient.describeReplicaLogDirs(Set.of(replicaOnBroker3, replicaOnBroker4)))
            .andReturn(describeReplicaLogDirsResult(Map.of(replicaOnBroker3, KafkaFuture.completedFuture(replicaLogDirInfo("d1")),
                                                           replicaOnBroker4, failedReplicaLogDirInfoFuture(new KafkaStorageException("")))));
    EasyMock.replay(adminClient);

    assertEquals(Set.of(),
                 ExecutorAdminUtils.setLogdirsOfReplicasToAdd(List.of(interBrokerReplicaMovementTask(0L, proposal)), adminClient, CONFIG));
    EasyMock.verify(adminClient);
  }

  @Test
  public void testSetLogdirsOfReplicasToAddWithoutLogdirs() {
    // The replica placement over disks is not populated (e.g. in proposals generated by inter-broker goals only).
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC, 0), 10, new ReplicaPlacementInfo(0),
                                                       List.of(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       List.of(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
    // No request is expected to be sent to brokers.
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.replay(adminClient);

    assertEquals(Set.of(), ExecutorAdminUtils.setLogdirsOfReplicasToAdd(List.of(interBrokerReplicaMovementTask(0L, proposal)),
                                                                        adminClient, CONFIG));
    EasyMock.verify(adminClient);
  }
}
