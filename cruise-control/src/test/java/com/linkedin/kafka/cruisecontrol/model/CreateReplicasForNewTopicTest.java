/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CreateReplicasForNewTopicTest {
  private static final String NEW_TOPIC = "new_topic";
  private static final int PARTITION_COUNT = 4;
  private static final short REPLICATION_FACTOR = 2;
  private static final double PARTITION_DISK_MB = 10.0;
  private static final double PARTITION_NW_IN_KBPS = 20.0;
  private static final double PARTITION_NW_OUT_KBPS = 30.0;
  private static final double EPSILON = 1E-6;

  private static Map<String, List<Integer>> brokersByRack(ClusterModel clusterModel) {
    Map<String, List<Integer>> brokersByRack = new TreeMap<>();
    clusterModel.brokers().forEach(b -> brokersByRack.computeIfAbsent(b.rack().id(), r -> new ArrayList<>()).add(b.id()));
    return brokersByRack;
  }

  @Test
  public void testCreateReplicasForNewTopic() {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    double initialDiskUtilization = clusterModel.load().expectedUtilizationFor(Resource.DISK);
    double initialNwInUtilization = clusterModel.load().expectedUtilizationFor(Resource.NW_IN);

    clusterModel.createReplicasForNewTopic(NEW_TOPIC, PARTITION_COUNT, REPLICATION_FACTOR, PARTITION_DISK_MB, PARTITION_NW_IN_KBPS,
                                           PARTITION_NW_OUT_KBPS, brokersByRack(clusterModel));
    // Load of the new replicas must be consistent across the cluster hierarchy.
    clusterModel.sanityCheck();

    List<Partition> partitions = clusterModel.getPartitionsByTopic().get(NEW_TOPIC);
    assertEquals(PARTITION_COUNT, partitions.size());
    assertEquals(REPLICATION_FACTOR, clusterModel.replicationFactorByTopic().get(NEW_TOPIC).intValue());
    for (Partition partition : partitions) {
      assertEquals(REPLICATION_FACTOR, partition.replicas().size());
      // Replicas of the same partition must be distinguishable, although none of them has an existing original broker.
      assertEquals(REPLICATION_FACTOR, new HashSet<>(partition.replicas()).size());
      // Two racks in the cluster -- replicas are placed in a rack-aware way.
      assertEquals(REPLICATION_FACTOR, partition.partitionRacks().size());

      Replica leader = partition.leader();
      assertTrue(leader.isImmigrant());
      assertEquals(PARTITION_DISK_MB, leader.load().expectedUtilizationFor(Resource.DISK), EPSILON);
      assertEquals(PARTITION_NW_IN_KBPS, leader.load().expectedUtilizationFor(Resource.NW_IN), EPSILON);
      // The leader also serves replication bytes out to its follower.
      assertEquals(PARTITION_NW_OUT_KBPS + PARTITION_NW_IN_KBPS * (REPLICATION_FACTOR - 1),
                   leader.load().expectedUtilizationFor(Resource.NW_OUT), EPSILON);
      assertEquals(0.0, leader.load().expectedUtilizationFor(Resource.CPU), EPSILON);
      for (Replica follower : partition.followers()) {
        assertTrue(follower.isImmigrant());
        assertEquals(PARTITION_DISK_MB, follower.load().expectedUtilizationFor(Resource.DISK), EPSILON);
        assertEquals(PARTITION_NW_IN_KBPS, follower.load().expectedUtilizationFor(Resource.NW_IN), EPSILON);
        assertEquals(0.0, follower.load().expectedUtilizationFor(Resource.NW_OUT), EPSILON);
      }
    }
    int numNewReplicas = PARTITION_COUNT * REPLICATION_FACTOR;
    assertEquals(initialDiskUtilization + numNewReplicas * PARTITION_DISK_MB,
                 clusterModel.load().expectedUtilizationFor(Resource.DISK), EPSILON);
    assertEquals(initialNwInUtilization + numNewReplicas * PARTITION_NW_IN_KBPS,
                 clusterModel.load().expectedUtilizationFor(Resource.NW_IN), EPSILON);
  }

  @Test
  public void testCreateReplicasForNewTopicSanityChecks() {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    Map<String, List<Integer>> brokersByRack = brokersByRack(clusterModel);
    // Expected: IllegalArgumentException if the topic already exists in the cluster model.
    assertThrows(IllegalArgumentException.class, () -> clusterModel.createReplicasForNewTopic(DeterministicCluster.T1, PARTITION_COUNT,
                                                                                              REPLICATION_FACTOR, PARTITION_DISK_MB,
                                                                                              PARTITION_NW_IN_KBPS, PARTITION_NW_OUT_KBPS,
                                                                                              brokersByRack));
    // Expected: IllegalArgumentException if the replication factor is more than the number of brokers.
    assertThrows(IllegalArgumentException.class, () -> clusterModel.createReplicasForNewTopic(NEW_TOPIC, PARTITION_COUNT,
                                                                                              (short) (clusterModel.brokers().size() + 1),
                                                                                              PARTITION_DISK_MB, PARTITION_NW_IN_KBPS,
                                                                                              PARTITION_NW_OUT_KBPS, brokersByRack));
  }
}
