/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for execution proposals.
 */
public class ExecutionProposalTest {
  public static final TopicPartition TP = new TopicPartition("topic", 0);
  private final ReplicaPlacementInfo _r0 = new ReplicaPlacementInfo(0);
  private final ReplicaPlacementInfo _r1 = new ReplicaPlacementInfo(1);
  private final ReplicaPlacementInfo _r2 = new ReplicaPlacementInfo(2);

  private final ReplicaPlacementInfo _r0d0 = new ReplicaPlacementInfo(0, "tmp0");
  private final ReplicaPlacementInfo _r0d1 = new ReplicaPlacementInfo(0, "tmp1");
  private final ReplicaPlacementInfo _r1d1 = new ReplicaPlacementInfo(1, "tmp1");

  private static final Node NODE_0 = new Node(0, "host0", 100);
  private static final Node NODE_1 = new Node(1, "host1", 200);
  private static final Node NODE_2 = new Node(2, "host2", 300);

  @Test (expected = IllegalArgumentException.class)
  public void testNullNewReplicaList() {
    new ExecutionProposal(TP, 0, _r1, Arrays.asList(_r0, _r1), null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testEmptyNewReplicaList() {
    new ExecutionProposal(TP, 0, _r1, Arrays.asList(_r0, _r1), Collections.emptyList());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDuplicateReplicaInNewReplicaList() {
    new ExecutionProposal(TP, 0, _r1, Arrays.asList(_r0, _r1), Arrays.asList(_r2, _r2));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testOldLeaderMissingFromOldReplicas() {
    new ExecutionProposal(TP, 0, _r2, Arrays.asList(_r0, _r1), Arrays.asList(_r1, _r2));
  }

  @Test
  public void testIntraBrokerReplicaMovements() {
    ExecutionProposal p = new ExecutionProposal(TP, 0, _r0d0, Arrays.asList(_r0d0, _r1d1), Arrays.asList(_r0d1, _r1d1));
    Assert.assertEquals(1, p.replicasToMoveBetweenDisksByBroker().size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testMingleReplicaMovements() {
    new ExecutionProposal(TP, 0, _r0d0, Arrays.asList(_r0d0, _r1), Arrays.asList(_r0d1, _r2));
  }

  @Test
  public void testIsReplicaSetPreserved() {
    // Reorder only.
    assertTrue(new ExecutionProposal(TP, 0, _r0, Arrays.asList(_r0, _r1, _r2), Arrays.asList(_r1, _r0, _r2)).isReplicaSetPreserved());
    // Intra-broker replica movement only.
    assertTrue(new ExecutionProposal(TP, 0, _r0d0, Arrays.asList(_r0d0, _r1d1), Arrays.asList(_r0d1, _r1d1)).isReplicaSetPreserved());
    // Replica movement across brokers.
    assertFalse(new ExecutionProposal(TP, 0, _r0, Arrays.asList(_r0, _r1), Arrays.asList(_r0, _r2)).isReplicaSetPreserved());
    // Replication factor increase and decrease.
    assertFalse(new ExecutionProposal(TP, 0, _r0, Arrays.asList(_r0, _r1), Arrays.asList(_r0, _r1, _r2)).isReplicaSetPreserved());
    assertFalse(new ExecutionProposal(TP, 0, _r0, Arrays.asList(_r0, _r1, _r2), Arrays.asList(_r0, _r1)).isReplicaSetPreserved());
  }

  @Test
  public void testReorderOnUnderReplicatedPartitionIsCompletedOnceOrderMatches() {
    // Promote broker 1 as the preferred leader while broker 2 is out of sync.
    ExecutionProposal reorder = new ExecutionProposal(TP, 0, _r0, Arrays.asList(_r0, _r1, _r2), Arrays.asList(_r1, _r0, _r2));
    Node[] inSync = {NODE_0, NODE_1};
    // The new order is not reflected yet.
    PartitionInfo beforeReorder = new PartitionInfo(TP.topic(), TP.partition(), NODE_0, new Node[]{NODE_0, NODE_1, NODE_2}, inSync);
    assertFalse(reorder.isInterBrokerMovementCompleted(beforeReorder));
    // The new order is reflected: completed despite the out-of-sync replica, since a reorder moves no data.
    PartitionInfo afterReorder = new PartitionInfo(TP.topic(), TP.partition(), NODE_0, new Node[]{NODE_1, NODE_0, NODE_2}, inSync);
    assertTrue(reorder.isInterBrokerMovementCompleted(afterReorder));
    assertTrue(reorder.isInterBrokerMovementAborted(afterReorder));
    // A rollback to the original order is considered aborted.
    assertTrue(reorder.isInterBrokerMovementAborted(beforeReorder));
  }

  @Test
  public void testReplicaAdditionIsCompletedOnlyOnceAllReplicasAreInSync() {
    ExecutionProposal move = new ExecutionProposal(TP, 0, _r0, Arrays.asList(_r0, _r1), Arrays.asList(_r0, _r2));
    Node[] newReplicas = {NODE_0, NODE_2};
    // The new replica on broker 2 has not caught up yet.
    PartitionInfo catchingUp = new PartitionInfo(TP.topic(), TP.partition(), NODE_0, newReplicas, new Node[]{NODE_0});
    assertFalse(move.isInterBrokerMovementCompleted(catchingUp));
    PartitionInfo inSync = new PartitionInfo(TP.topic(), TP.partition(), NODE_0, newReplicas, newReplicas);
    assertTrue(move.isInterBrokerMovementCompleted(inSync));
  }

  @Test
  public void testIntraBrokerOnlyMovementOnUnderReplicatedPartitionIsInterBrokerComplete() {
    ExecutionProposal diskMove = new ExecutionProposal(TP, 0, _r0d0, Arrays.asList(_r0d0, _r1d1), Arrays.asList(_r0d1, _r1d1));
    PartitionInfo underReplicated = new PartitionInfo(TP.topic(), TP.partition(), NODE_0, new Node[]{NODE_0, NODE_1}, new Node[]{NODE_0});
    assertTrue(diskMove.isInterBrokerMovementCompleted(underReplicated));
  }

  @Test
  public void testAreAllReplicasInSync() {
    // Verify: If isr is the same as replicas, all replicas are in-sync
    Node[] replicas = new Node[2];
    replicas[0] = NODE_0;
    replicas[1] = NODE_1;

    Node[] isr = new Node[2];
    isr[0] = NODE_0;
    isr[1] = NODE_1;
    PartitionInfo partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), NODE_1, replicas, isr);
    assertTrue(ExecutionProposal.areAllReplicasInSync(partitionInfo));

    // Verify: If isr is smaller than replicas, not all replicas are in-sync
    Node[] smallIsr = new Node[1];
    smallIsr[0] = NODE_0;
    partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), NODE_1, replicas, smallIsr);
    assertFalse(ExecutionProposal.areAllReplicasInSync(partitionInfo));

    // Verify: If isr is greater than replicas, all replicas are in-sync
    Node[] greaterIsr = new Node[3];
    greaterIsr[0] = NODE_0;
    greaterIsr[1] = NODE_1;
    greaterIsr[2] = NODE_2;
    partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), NODE_1, replicas, greaterIsr);
    assertTrue(ExecutionProposal.areAllReplicasInSync(partitionInfo));

    // Verify: If isr has the same size as replicas, but replicas are not same as in-sync replicas, then not all replicas are in-sync.
    Node[] isrWithDifferentBrokerIds = new Node[2];
    isrWithDifferentBrokerIds[0] = NODE_0;
    isrWithDifferentBrokerIds[1] = NODE_2;
    partitionInfo = new PartitionInfo(TP.topic(), TP.partition(), NODE_1, replicas, isrWithDifferentBrokerIds);
    assertFalse(ExecutionProposal.areAllReplicasInSync(partitionInfo));
  }
}
