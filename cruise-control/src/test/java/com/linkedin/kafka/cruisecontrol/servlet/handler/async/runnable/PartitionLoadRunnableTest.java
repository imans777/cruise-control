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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PartitionLoadRunnableTest {
  // T1-0 [0, 2], T1-1 [1, 0], T2-0 [1, 2], T2-1 [0, 2], T2-2 [0, 1] -- the leader is first.
  private static final ClusterModel CLUSTER_MODEL = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
  private static final TopicPartition T1P0 = new TopicPartition(T1, 0);
  private static final TopicPartition T1P1 = new TopicPartition(T1, 1);
  private static final TopicPartition T2P0 = new TopicPartition(T2, 0);
  private static final TopicPartition T2P1 = new TopicPartition(T2, 1);

  private static List<TopicPartition> topicPartitions(List<Partition> partitions) {
    return partitions.stream().map(Partition::topicPartition).collect(Collectors.toList());
  }

  @Test
  public void testIsInScope() {
    assertTrue(PartitionLoadRunnable.isInScope(CLUSTER_MODEL.partition(T1P0), null, Integer.MIN_VALUE, Integer.MAX_VALUE));
    assertTrue(PartitionLoadRunnable.isInScope(CLUSTER_MODEL.partition(T1P0), Pattern.compile("T1"), 0, 0));
    assertFalse(PartitionLoadRunnable.isInScope(CLUSTER_MODEL.partition(T1P1), Pattern.compile("T1"), 0, 0));
    assertFalse(PartitionLoadRunnable.isInScope(CLUSTER_MODEL.partition(T2P0), Pattern.compile("T1"), 0, 0));
    assertTrue(PartitionLoadRunnable.isInScope(CLUSTER_MODEL.partition(T2P1), Pattern.compile("T.*"), 1, 2));
  }

  @Test
  public void testPartitionsOnLogDirs() {
    List<Partition> partitions = List.of(CLUSTER_MODEL.partition(T1P0), CLUSTER_MODEL.partition(T1P1), CLUSTER_MODEL.partition(T2P0),
                                         CLUSTER_MODEL.partition(T2P1));
    Map<TopicPartition, Map<Integer, String>> logDirByReplica = Map.of(T1P0, Map.of(0, "/a", 2, "/b"),
                                                                       T1P1, Map.of(1, "/b", 0, "/b/"),
                                                                       T2P0, Map.of(1, "/a", 2, "/a"));
    // A requested log directory matches regardless of a trailing separator, and the order of the partitions is kept.
    assertEquals(List.of(T1P0, T1P1), topicPartitions(PartitionLoadRunnable.partitionsOnLogDirs(partitions, logDirByReplica,
                                                                                                Map.of(0, Set.of("/a", "/b")))));
    assertEquals(List.of(T1P0, T2P0), topicPartitions(PartitionLoadRunnable.partitionsOnLogDirs(partitions, logDirByReplica,
                                                                                                Map.of(2, Set.of("/a/", "/b")))));
    // The log directories of the replicas of T2-1 are unknown.
    assertEquals(List.of(), topicPartitions(PartitionLoadRunnable.partitionsOnLogDirs(partitions, logDirByReplica,
                                                                                      Map.of(1, Set.of("/c")))));
  }

  @Test
  public void testCurrentLogDirByReplica() {
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = Map.of(
        0, Map.of("/a", new LogDirDescription(null, Map.of(T1P0, new ReplicaInfo(1L, 0L, false), T2P1, new ReplicaInfo(1L, 0L, false))),
                  // A future replica -- i.e. one that is being moved to this log directory -- is not the current replica.
                  "/b", new LogDirDescription(null, Map.of(T1P0, new ReplicaInfo(1L, 0L, true)))),
        2, Map.of("/a", new LogDirDescription(null, Map.of(T1P0, new ReplicaInfo(1L, 0L, false)))));
    assertEquals(Map.of(T1P0, Map.of(0, "/a", 2, "/a")), RunnableUtils.currentLogDirByReplica(logDirsByBroker, Set.of(T1P0, T1P1)));
  }

  @Test
  public void testWithoutTrailingSeparator() {
    assertEquals("/a", RunnableUtils.withoutTrailingSeparator("/a//"));
    assertEquals("/a", RunnableUtils.withoutTrailingSeparator("/a"));
    assertEquals("/", RunnableUtils.withoutTrailingSeparator("/"));
  }
}
