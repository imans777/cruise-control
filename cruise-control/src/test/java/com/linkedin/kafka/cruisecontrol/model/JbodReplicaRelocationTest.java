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

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for relocating replicas across brokers in a cluster model with the replica placement over disks (i.e. JBOD).
 */
public class JbodReplicaRelocationTest {
  private static final double DELTA = 1E-6;
  private static final double RELATIVE_DELTA = 1E-5;
  private static final Map<Integer, Integer> RACK_BY_BROKER = Map.of(0, 0, 1, 1, 2, 2);
  private static final TopicPartition T0P0 = new TopicPartition("T0", 0);
  private static final TopicPartition T0P1 = new TopicPartition("T0", 1);
  private static final TopicPartition T0P2 = new TopicPartition("T0", 2);
  private static final TopicPartition T0P3 = new TopicPartition("T0", 3);

  /**
   * Three brokers with two disks each (see {@link TestConstants#DISK_CAPACITY}), and four single-replica partitions:
   * <ul>
   *   <li>Broker 0: {@link #T0P0} (30000) on {@link TestConstants#LOGDIR0}, {@link #T0P1} (10000) on {@link TestConstants#LOGDIR1}.</li>
   *   <li>Broker 1: {@link #T0P2} (50000) on {@link TestConstants#LOGDIR0}, {@link #T0P3} (5000) on {@link TestConstants#LOGDIR1}.</li>
   *   <li>Broker 2: No replicas.</li>
   * </ul>
   *
   * @return Cluster model for the tests.
   */
  private static ClusterModel jbodCluster() {
    ClusterModel cluster = DeterministicCluster.getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY,
                                                                      TestConstants.DISK_CAPACITY);
    createReplica(cluster, 0, T0P0, LOGDIR0, 30000.0);
    createReplica(cluster, 0, T0P1, LOGDIR1, 10000.0);
    createReplica(cluster, 1, T0P2, LOGDIR0, 50000.0);
    createReplica(cluster, 1, T0P3, LOGDIR1, 5000.0);
    return cluster;
  }

  private static void createReplica(ClusterModel cluster, int brokerId, TopicPartition tp, String logdir, double diskUsage) {
    String rackId = RACK_BY_BROKER.get(brokerId).toString();
    cluster.createReplica(rackId, brokerId, tp, 0, true, false, logdir, false);
    cluster.setReplicaLoad(rackId, brokerId, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, diskUsage),
                           Collections.singletonList(1L));
  }

  /**
   * Verify that each replica of each broker using JBOD resides in exactly one disk of the broker, and the disk utilization
   * of each such broker is the total utilization of its disks.
   *
   * @param cluster The cluster model to verify.
   */
  public static void verifyDiskConsistency(ClusterModel cluster) {
    for (Broker broker : cluster.brokers()) {
      if (!broker.isUsingJBOD()) {
        continue;
      }
      double totalDiskUtilization = 0.0;
      Set<Replica> replicasOnDisks = new HashSet<>();
      for (Disk disk : broker.disks()) {
        totalDiskUtilization += disk.utilization();
        for (Replica replica : disk.replicas()) {
          assertSame(String.format("Replica %s on disk %s of broker %d is on broker %d.", replica.topicPartition(), disk.logDir(),
                                   broker.id(), replica.broker().id()), broker, replica.broker());
          assertSame(String.format("Replica %s on disk %s of broker %d is on disk %s.", replica.topicPartition(), disk.logDir(),
                                   broker.id(), replica.disk()), disk, replica.disk());
          assertTrue(String.format("Replica %s is on more than one disk of broker %d.", replica.topicPartition(), broker.id()),
                     replicasOnDisks.add(replica));
        }
      }
      assertEquals(String.format("Replicas of broker %d do not match the replicas on its disks.", broker.id()),
                   broker.replicas(), replicasOnDisks);
      // The broker load and the disk utilization are aggregated with different precisions, hence use a relative tolerance.
      double brokerDiskUtilization = broker.load().expectedUtilizationFor(Resource.DISK);
      assertEquals(String.format("Disk utilization of broker %d does not match the utilization of its disks.", broker.id()),
                   brokerDiskUtilization, totalDiskUtilization, Math.max(DELTA, brokerDiskUtilization * RELATIVE_DELTA));
    }
  }

  @Test
  public void testRelocateReplicaAcrossBrokersPlacesReplicaOnLeastUtilizedDisk() {
    ClusterModel cluster = jbodCluster();
    // After hosting T0P0, the utilization of LOGDIR0 and LOGDIR1 of broker 1 would be 80000 and 35000, respectively.
    cluster.relocateReplica(T0P0, 0, 1);

    Replica replica = cluster.broker(1).replica(T0P0);
    assertSame(cluster.broker(1).disk(LOGDIR1), replica.disk());
    assertTrue(cluster.broker(1).disk(LOGDIR1).replicas().contains(replica));
    assertEquals(35000.0, cluster.broker(1).disk(LOGDIR1).utilization(), DELTA);
    // The replica is removed from its source disk.
    assertTrue(cluster.broker(0).disk(LOGDIR0).replicas().isEmpty());
    assertEquals(0.0, cluster.broker(0).disk(LOGDIR0).utilization(), DELTA);
    assertEquals(List.of(new ReplicaPlacementInfo(1, LOGDIR1)), cluster.getReplicaDistribution().get(T0P0));
    verifyDiskConsistency(cluster);
  }

  @Test
  public void testRelocateReplicaAcrossBrokersBreaksTiesByLogdir() {
    ClusterModel cluster = jbodCluster();
    // Both disks of broker 2 are empty.
    cluster.relocateReplica(T0P1, 0, 2);

    assertSame(cluster.broker(2).disk(LOGDIR0), cluster.broker(2).replica(T0P1).disk());
    verifyDiskConsistency(cluster);
  }

  @Test
  public void testRelocateReplicaBackToOriginalBrokerRestoresOriginalDisk() {
    ClusterModel cluster = jbodCluster();
    Map<TopicPartition, List<ReplicaPlacementInfo>> initialReplicaDistribution = cluster.getReplicaDistribution();
    // Empty LOGDIR0 of broker 0, so that it becomes the least utilized disk of broker 0.
    cluster.relocateReplica(T0P0, 0, 2);
    // Relocate T0P1, which originally resides in LOGDIR1 of broker 0, to broker 2 and then back to broker 0.
    cluster.relocateReplica(T0P1, 0, 2);
    cluster.relocateReplica(T0P1, 2, 0);

    assertSame(cluster.broker(0).disk(LOGDIR1), cluster.broker(0).replica(T0P1).disk());
    assertEquals(initialReplicaDistribution.get(T0P1), cluster.getReplicaDistribution().get(T0P1));
    verifyDiskConsistency(cluster);
  }

  @Test
  public void testRelocateReplicaAcrossBrokersSkipsDeadDisks() {
    ClusterModel cluster = jbodCluster();
    // LOGDIR1 would be the least utilized disk of broker 1 after hosting T0P0, but it is dead.
    cluster.markDiskDead(1, LOGDIR1);
    cluster.relocateReplica(T0P0, 0, 1);

    assertSame(cluster.broker(1).disk(LOGDIR0), cluster.broker(1).replica(T0P0).disk());
    assertEquals(80000.0, cluster.broker(1).disk(LOGDIR0).utilization(), DELTA);
    verifyDiskConsistency(cluster);
  }

  @Test
  public void testDeleteReplicaRemovesReplicaFromDisk() {
    ClusterModel cluster = jbodCluster();
    // Add a follower of T0P2 to LOGDIR1 of broker 2, then delete it.
    String rackId = RACK_BY_BROKER.get(2).toString();
    cluster.createReplica(rackId, 2, T0P2, 1, false, false, LOGDIR1, false);
    cluster.setReplicaLoad(rackId, 2, T0P2, getAggregatedMetricValues(1.0, 10.0, 0.0, 50000.0), Collections.singletonList(1L));
    assertEquals(50000.0, cluster.broker(2).disk(LOGDIR1).utilization(), DELTA);
    cluster.deleteReplica(T0P2, 2);

    assertTrue(cluster.broker(2).disk(LOGDIR1).replicas().isEmpty());
    assertEquals(0.0, cluster.broker(2).disk(LOGDIR1).utilization(), DELTA);
    verifyDiskConsistency(cluster);
  }

  @Test
  public void testRelocateReplicaAcrossBrokersWithoutReplicaPlacementInfo() {
    ClusterModel cluster = DeterministicCluster.getHomogeneousCluster(RACK_BY_BROKER, TestConstants.BROKER_CAPACITY, null);
    createReplica(cluster, 0, T0P0, null, 30000.0);
    cluster.relocateReplica(T0P0, 0, 1);

    assertNull(cluster.broker(1).replica(T0P0).disk());
    assertFalse(cluster.broker(1).isUsingJBOD());
    assertEquals(List.of(new ReplicaPlacementInfo(1)), cluster.getReplicaDistribution().get(T0P0));
  }
}
