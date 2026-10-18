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
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
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

  @Test
  public void testSetLogdirsOfReplicasToAdd() {
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

    // Brokers respond with ReplicaNotAvailableException for replicas that do not exist yet. Failures to set a logdir (e.g. due
    // to an offline logdir) are tolerated, since the broker picks the logdir of the replica in that case.
    AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(result.values()).andReturn(Map.of(replica0OnBroker2, failedFuture(new ReplicaNotAvailableException("")),
                                                      replica1OnBroker3, KafkaFuture.completedFuture(null),
                                                      replica1OnBroker4, failedFuture(new KafkaStorageException(""))));
    AdminClient adminClient = EasyMock.mock(AdminClient.class);
    EasyMock.expect(adminClient.alterReplicaLogDirs(Map.of(replica0OnBroker2, "d1", replica1OnBroker3, "d0", replica1OnBroker4, "d1")))
            .andReturn(result);
    EasyMock.replay(result, adminClient);

    ExecutorAdminUtils.setLogdirsOfReplicasToAdd(List.of(interBrokerReplicaMovementTask(0L, proposal0),
                                                         interBrokerReplicaMovementTask(1L, proposal1)),
                                                 adminClient, CONFIG);
    EasyMock.verify(result, adminClient);
  }

  @Test
  public void testSetLogdirsOfReplicasToAddWithoutLogdirs() {
    // The replica placement over disks is not populated (e.g. in proposals generated by inter-broker goals only).
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC, 0), 10, new ReplicaPlacementInfo(0),
                                                       List.of(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       List.of(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
    // No request is expected to be sent to brokers.
    AdminClient adminClient = EasyMock.mock(AdminClient.class);
    EasyMock.replay(adminClient);

    ExecutorAdminUtils.setLogdirsOfReplicasToAdd(List.of(interBrokerReplicaMovementTask(0L, proposal)), adminClient, CONFIG);
    EasyMock.verify(adminClient);
  }
}
