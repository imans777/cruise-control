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

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.ReassignPartitionsProposalBuilder.ReassignPartitionsPlan;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RequestedPartitionReassignment;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.PartitionReassignmentDetails;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class ReassignPartitionsProposalBuilderTest {
  private static final String TOPIC = "t";
  private static final long MB = 1024L * 1024L;
  private static final long LARGE_DISK = 1024L * 1024L * MB;
  private static final Node N0 = new Node(0, "host0", 9092, "r0");
  private static final Node N1 = new Node(1, "host1", 9092, "r1");
  private static final Node N2 = new Node(2, "host2", 9092, "r2");
  private static final Node N3 = new Node(3, "host3", 9092, "r0");
  // Broker 4 is dead -- i.e. it is not among the nodes of the cluster.
  private static final Node N4 = new Node(4, "", -1);
  // All replicas are in sync.
  private static final TopicPartition T0 = new TopicPartition(TOPIC, 0);
  // The leader is not the preferred leader.
  private static final TopicPartition T1 = new TopicPartition(TOPIC, 1);
  // A replica is on the dead broker.
  private static final TopicPartition T2 = new TopicPartition(TOPIC, 2);
  // Offline.
  private static final TopicPartition T3 = new TopicPartition(TOPIC, 3);
  // The replica on broker 2 is out of sync.
  private static final TopicPartition T4 = new TopicPartition(TOPIC, 4);
  // Being reassigned.
  private static final TopicPartition T5 = new TopicPartition(TOPIC, 5);
  private static final Cluster CLUSTER = new Cluster("cluster", List.of(N0, N1, N2, N3), List.of(
      partition(T0, N0, List.of(N0, N1, N2), List.of(N0, N1, N2)),
      partition(T1, N2, List.of(N1, N2, N0), List.of(N0, N1, N2)),
      partition(T2, N0, List.of(N0, N1, N4), List.of(N0, N1)),
      partition(T3, null, List.of(N0, N1), List.of()),
      partition(T4, N0, List.of(N0, N1, N2), List.of(N0, N1)),
      partition(T5, N0, List.of(N0, N1, N2), List.of(N0, N1, N2))), Collections.emptySet(), Collections.emptySet());

  private static PartitionInfo partition(TopicPartition tp, Node leader, List<Node> replicas, List<Node> inSyncReplicas) {
    Node[] replicaArray = replicas.toArray(new Node[0]);
    return new PartitionInfo(tp.topic(), tp.partition(), leader, replicaArray, inSyncReplicas.toArray(new Node[0]),
                             Arrays.stream(replicaArray).filter(node -> node.id() == N4.id()).toArray(Node[]::new));
  }

  private static ReplicaInfo replica(long sizeInMB) {
    return new ReplicaInfo(sizeInMB * MB, 0L, false);
  }

  private static LogDirDescription logDir(Map<TopicPartition, ReplicaInfo> replicas) {
    return new LogDirDescription(null, replicas, LARGE_DISK, LARGE_DISK);
  }

  /**
   * @return Log directories of the alive brokers, in which each replica of T0 has 10 MB, and each replica of T1 has 5 MB.
   */
  private static Map<Integer, Map<String, LogDirDescription>> logDirsByBroker() {
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = new HashMap<>();
    logDirsByBroker.put(0, Map.of(
        "/d0a", logDir(Map.of(T0, replica(10), T1, replica(5), T2, replica(1), T3, replica(1), T4, replica(1), T5, replica(1))),
        "/d0b", logDir(Map.of())));
    logDirsByBroker.put(1, Map.of(
        "/d1a", logDir(Map.of(T0, replica(10), T1, replica(5), T2, replica(1), T3, replica(1), T4, replica(1), T5, replica(1))),
        "/d1b", logDir(Map.of()),
        "/d1c", new LogDirDescription(null, Map.of(), LARGE_DISK, LARGE_DISK, true),
        "/d1d", new LogDirDescription(new KafkaStorageException("disk failure"), Map.of()),
        "/d1e", new LogDirDescription(null, Map.of(), LARGE_DISK, MB)));
    logDirsByBroker.put(2, Map.of(
        "/d2a", logDir(Map.of(T0, replica(10), T1, replica(5), T4, replica(1), T5, replica(1))),
        "/d2b", logDir(Map.of(T1, new ReplicaInfo(MB, 0L, true)))));
    logDirsByBroker.put(3, Map.of("/d3a", logDir(Map.of()), "/d3b", logDir(Map.of())));
    return logDirsByBroker;
  }

  private static ReassignPartitionsPlan build(RequestedPartitionReassignment... requested) {
    return ReassignPartitionsProposalBuilder.build(List.of(requested), CLUSTER, Set.of(T5), logDirsByBroker(), Collections.emptyMap());
  }

  private static String problemsOf(RequestedPartitionReassignment... requested) {
    return assertThrows(UserRequestException.class, () -> build(requested)).getMessage();
  }

  private static RequestedPartitionReassignment leader(TopicPartition tp, int leader) {
    return RequestedPartitionReassignment.withLeader(tp, leader);
  }

  private static RequestedPartitionReassignment replicas(TopicPartition tp, List<Integer> replicas, String... logDirs) {
    return RequestedPartitionReassignment.withReplicas(tp, replicas, logDirs.length == 0 ? null : List.of(logDirs));
  }

  private static RequestedPartitionReassignment logDirs(TopicPartition tp, Map<Integer, String> logDirByBroker) {
    return RequestedPartitionReassignment.withLogDirs(tp, logDirByBroker);
  }

  private static ReplicaPlacementInfo rpi(int broker, String logDir) {
    return new ReplicaPlacementInfo(broker, logDir);
  }

  private static void assertDetails(PartitionReassignmentDetails details,
                                    List<Integer> newReplicas,
                                    List<String> newLogDirs,
                                    Set<ReassignmentAction> actions,
                                    long dataToMoveInMB) {
    assertEquals(newReplicas, details.newReplicas());
    assertEquals(newReplicas.get(0).intValue(), details.newLeader());
    assertEquals(newLogDirs, details.newLogDirs());
    assertEquals(actions, details.actions());
    assertEquals(dataToMoveInMB, details.interBrokerDataToMoveInMB() + details.intraBrokerDataToMoveInMB());
  }

  @Test
  public void testPromoteReplica() {
    ReassignPartitionsPlan plan = build(leader(T0, 1));

    PartitionReassignmentDetails details = plan.partitions().get(0);
    assertEquals(T0, details.topicPartition());
    assertEquals(List.of(0, 1, 2), details.currentReplicas());
    assertEquals(0, details.currentLeader());
    assertEquals(List.of("/d0a", "/d1a", "/d2a"), details.currentLogDirs());
    assertDetails(details, List.of(1, 0, 2), List.of("/d1a", "/d0a", "/d2a"),
                  EnumSet.of(ReassignmentAction.REPLICA_ORDER_CHANGE, ReassignmentAction.LEADERSHIP_MOVEMENT), 0L);
    assertTrue(details.warnings().isEmpty());

    ExecutionProposal proposal = plan.proposals().get(T0);
    assertEquals(rpi(0, "/d0a"), proposal.oldLeader());
    assertEquals(List.of(rpi(0, "/d0a"), rpi(1, "/d1a"), rpi(2, "/d2a")), proposal.oldReplicas());
    assertEquals(List.of(rpi(1, "/d1a"), rpi(0, "/d0a"), rpi(2, "/d2a")), proposal.newReplicas());
    assertTrue(proposal.isReplicaSetPreserved());
    assertTrue(proposal.replicasToMoveBetweenDisksByBroker().isEmpty());
    assertFalse(proposal.hasReplicaAction());
    assertTrue(proposal.hasLeaderAction());
    assertEquals(0L, proposal.interBrokerDataToMoveInMB());
    assertEquals(Set.of(proposal), plan.proposalsToExecute());
    assertTrue(plan.logDirHints().isEmpty());
    assertFalse(plan.hasIntraBrokerReplicaMovements());
  }

  @Test
  public void testElectPreferredLeader() {
    // The requested leader is already the first replica -- i.e. only a leader election is needed.
    ReassignPartitionsPlan plan = build(leader(T1, 1));

    PartitionReassignmentDetails details = plan.partitions().get(0);
    assertEquals(2, details.currentLeader());
    assertDetails(details, List.of(1, 2, 0), List.of("/d1a", "/d2a", "/d0a"), EnumSet.of(ReassignmentAction.LEADERSHIP_MOVEMENT), 0L);
    ExecutionProposal proposal = plan.proposals().get(T1);
    assertEquals(rpi(2, "/d2a"), proposal.oldLeader());
    assertEquals(proposal.oldReplicas(), proposal.newReplicas());
    assertTrue(proposal.hasLeaderAction());
  }

  @Test
  public void testNoChange() {
    ReassignPartitionsPlan plan = build(leader(T0, 0),
                                        replicas(T1, List.of(1, 2, 0), "any", "/d2a/", "ANY"),
                                        logDirs(T4, Map.of(1, "/d1a")));
    assertEquals(3, plan.partitions().size());
    for (PartitionReassignmentDetails details : plan.partitions()) {
      if (details.topicPartition().equals(T1)) {
        // A replica order identical to the current one still elects the preferred leader.
        assertEquals(EnumSet.of(ReassignmentAction.LEADERSHIP_MOVEMENT), details.actions());
      } else {
        assertEquals(EnumSet.of(ReassignmentAction.NO_CHANGE), details.actions());
        assertFalse(details.hasChange());
        assertEquals(details.currentReplicas(), details.newReplicas());
        assertEquals(details.currentLogDirs(), details.newLogDirs());
      }
    }
    assertEquals(Set.of(T1), plan.proposals().keySet());
  }

  @Test
  public void testMoveReplicasBetweenLogDirs() {
    ReassignPartitionsPlan plan = build(logDirs(T0, Map.of(0, "/d0b", 1, "/d1b/")), logDirs(T1, Map.of(0, "/d0b")));

    PartitionReassignmentDetails t0 = plan.partitions().get(0);
    assertDetails(t0, List.of(0, 1, 2), List.of("/d0b", "/d1b", "/d2a"), EnumSet.of(ReassignmentAction.INTRA_BROKER_REPLICA_MOVEMENT), 20L);
    assertEquals(20L, t0.intraBrokerDataToMoveInMB());
    assertTrue(t0.warnings().isEmpty());
    ExecutionProposal t0Proposal = plan.proposals().get(T0);
    assertEquals(Map.of(0, rpi(0, "/d0b"), 1, rpi(1, "/d1b")), t0Proposal.replicasToMoveBetweenDisksByBroker());
    assertTrue(t0Proposal.replicasToAdd().isEmpty());
    assertTrue(t0Proposal.replicasToRemove().isEmpty());

    // The leader of T1 is not the preferred leader, hence the execution also moves the leadership to the preferred leader.
    PartitionReassignmentDetails t1 = plan.partitions().get(1);
    assertDetails(t1, List.of(1, 2, 0), List.of("/d1a", "/d2a", "/d0b"),
                  EnumSet.of(ReassignmentAction.INTRA_BROKER_REPLICA_MOVEMENT, ReassignmentAction.LEADERSHIP_MOVEMENT), 5L);
    assertEquals(1, t1.warnings().size());
    assertTrue(t1.warnings().get(0), t1.warnings().get(0).startsWith("The current leader 2 is not the preferred leader"));

    assertTrue(plan.hasIntraBrokerReplicaMovements());
    assertTrue(plan.logDirHints().isEmpty());
  }

  @Test
  public void testMoveReplicaToAnotherBroker() {
    ReassignPartitionsPlan plan = build(replicas(T0, List.of(3, 1, 2)));

    PartitionReassignmentDetails details = plan.partitions().get(0);
    assertDetails(details, List.of(3, 1, 2), List.of("any", "/d1a", "/d2a"),
                  EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE, ReassignmentAction.LEADERSHIP_MOVEMENT), 10L);
    assertEquals(10L, details.interBrokerDataToMoveInMB());
    assertTrue(details.warnings().isEmpty());
    ExecutionProposal proposal = plan.proposals().get(T0);
    assertEquals(Set.of(rpi(3, null)), proposal.replicasToAdd());
    assertEquals(Set.of(rpi(0, "/d0a")), proposal.replicasToRemove());
    assertTrue(proposal.replicasToMoveBetweenDisksByBroker().isEmpty());
    assertEquals(10L, proposal.interBrokerDataToMoveInMB());
    assertTrue(plan.logDirHints().isEmpty());
  }

  @Test
  public void testMoveReplicaToLogDirOfAnotherBroker() {
    // The log directory of an added replica is requested from its broker before the execution -- it is not an intra-broker movement.
    ReassignPartitionsPlan plan = build(replicas(T0, List.of(3, 1, 2), "/d3b/", "any", "/d2a"), leader(T1, 1));

    PartitionReassignmentDetails details = plan.partitions().get(0);
    assertDetails(details, List.of(3, 1, 2), List.of("/d3b", "/d1a", "/d2a"),
                  EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE, ReassignmentAction.LEADERSHIP_MOVEMENT), 10L);
    ExecutionProposal proposal = plan.proposals().get(T0);
    assertEquals(Set.of(rpi(3, "/d3b")), proposal.replicasToAdd());
    assertTrue(proposal.replicasToMoveBetweenDisksByBroker().isEmpty());
    assertEquals(Map.of(new TopicPartitionReplica(TOPIC, 0, 3), "/d3b"), plan.logDirHints());
    assertFalse(plan.hasIntraBrokerReplicaMovements());
    assertEquals(2, plan.proposalsToExecute().size());
  }

  @Test
  public void testChangeReplicationFactor() {
    ReassignPartitionsPlan plan = build(replicas(T0, List.of(0, 1, 2, 3)), replicas(T1, List.of(2, 0)));

    assertDetails(plan.partitions().get(0), List.of(0, 1, 2, 3), List.of("/d0a", "/d1a", "/d2a", "any"),
                  EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE, ReassignmentAction.REPLICATION_FACTOR_CHANGE), 10L);
    assertTrue(plan.partitions().get(0).warnings().isEmpty());
    assertEquals(Set.of(rpi(3, null)), plan.proposals().get(T0).replicasToAdd());

    assertDetails(plan.partitions().get(1), List.of(2, 0), List.of("/d2a", "/d0a"),
                  EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE, ReassignmentAction.REPLICATION_FACTOR_CHANGE), 0L);
    assertEquals(Set.of(rpi(1, "/d1a")), plan.proposals().get(T1).replicasToRemove());
  }

  @Test
  public void testWarnings() {
    // Brokers 0 and 3 share rack r0.
    ReassignPartitionsPlan plan = build(replicas(T0, List.of(0, 3, 1)), replicas(T4, List.of(0, 2, 3)));

    List<String> t0Warnings = plan.partitions().get(0).warnings();
    assertEquals(List.of("The new replicas are not rack-aware: they share racks {r0=[0, 3]}."), t0Warnings);
    List<String> t4Warnings = plan.partitions().get(1).warnings();
    assertEquals(2, t4Warnings.size());
    assertTrue(t4Warnings.get(0), t4Warnings.get(0).startsWith("Replicas [2] are out of sync"));
    assertEquals("The new replicas are not rack-aware: they share racks {r0=[0, 3]}.", t4Warnings.get(1));
  }

  @Test
  public void testInvalidPartitions() {
    String message = problemsOf(leader(new TopicPartition("u", 0), 1),
                                leader(new TopicPartition(TOPIC, 9), 1),
                                leader(T3, 1),
                                leader(T5, 1),
                                leader(T0, 3),
                                logDirs(T1, Map.of(3, "/d3a")),
                                replicas(T2, List.of(1, 0, 4)),
                                leader(T4, 2));
    assertTrue(message, message.startsWith("Cannot reassign partitions (8 problems):"));
    assertTrue(message, message.contains("- u-0: topic 'u' does not exist."));
    assertTrue(message, message.contains("- t-9: topic 't' has 6 partitions."));
    assertTrue(message, message.contains("- t-3: the partition has no leader (i.e. it is offline)"));
    assertTrue(message, message.contains("- t-5: the partition is being reassigned"));
    assertTrue(message, message.contains("- t-0: the requested leader 3 is not a replica (current replicas: [0, 1, 2])"));
    assertTrue(message, message.contains("- t-1: 'log_dirs' refers to broker 3, which does not host this partition"));
    assertTrue(message, message.contains("- t-2: brokers [4] in the new replica list are not alive"));
    assertTrue(message, message.contains("- t-4: the new leader 2 is not in the in-sync replicas [0, 1]."));
  }

  @Test
  public void testReplaceReplicaOnDeadBroker() {
    ReassignPartitionsPlan plan = build(replicas(T2, List.of(0, 1, 3)));
    assertDetails(plan.partitions().get(0), List.of(0, 1, 3), List.of("/d0a", "/d1a", "any"),
                  EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE), 1L);
    assertEquals(List.of("unknown"), plan.partitions().get(0).currentLogDirs().subList(2, 3));
  }

  @Test
  public void testInvalidLogDirs() {
    assertProblem(logDirs(T0, Map.of(1, "/d1x")), "t-0: broker 1 has no log directory '/d1x' (log directories: [/d1a, /d1b, /d1c, /d1d, /d1e]).");
    assertProblem(logDirs(T0, Map.of(1, "/d1c")), "t-0: log directory '/d1c' on broker 1 is cordoned.");
    assertProblem(logDirs(T0, Map.of(1, "/d1d")), "t-0: log directory '/d1d' on broker 1 is offline (disk failure).");
    assertProblem(logDirs(T0, Map.of(1, "/d1e")),
                  "t-0: log directory '/d1e' on broker 1 has 1 MB usable space, which does not fit the replica of 10 MB.");
    assertProblem(logDirs(T1, Map.of(2, "/d2b")), "t-1: the replica on broker 2 is already being moved to log directory '/d2b'.");
    assertProblem(replicas(T2, List.of(0, 1, 4), "any", "any", "/d4a"),
                  "t-2: cannot verify log directory '/d4a', because broker 4 is not alive.");
    // The placement of an added replica is validated the same way.
    assertProblem(replicas(T0, List.of(0, 1, 3), "any", "any", "/d3x"),
                  "t-0: broker 3 has no log directory '/d3x' (log directories: [/d3a, /d3b]).");
  }

  @Test
  public void testLogDirsOfBrokersThatFailedToDescribe() {
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker = logDirsByBroker();
    logDirsByBroker.remove(0);
    logDirsByBroker.remove(3);
    Map<Integer, String> errorByBroker = Map.of(0, "timed out", 3, "timed out");

    // The size of the partition falls back to the largest known replica size if the size of the leader replica is unknown.
    ReassignPartitionsPlan plan = ReassignPartitionsProposalBuilder.build(List.of(replicas(T0, List.of(3, 1, 2))), CLUSTER,
                                                                          Collections.emptySet(), logDirsByBroker, errorByBroker);
    PartitionReassignmentDetails details = plan.partitions().get(0);
    assertEquals(List.of("unknown", "/d1a", "/d2a"), details.currentLogDirs());
    assertDetails(details, List.of(3, 1, 2), List.of("any", "/d1a", "/d2a"),
                  EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE, ReassignmentAction.LEADERSHIP_MOVEMENT), 10L);
    assertNull(plan.proposals().get(T0).oldLeader().logdir());

    UserRequestException exception = assertThrows(UserRequestException.class, () -> ReassignPartitionsProposalBuilder.build(
        List.of(replicas(T0, List.of(3, 1, 2), "/d3a", "any", "any")), CLUSTER, Collections.emptySet(), logDirsByBroker, errorByBroker));
    assertTrue(exception.getMessage(), exception.getMessage().contains("t-0: cannot verify log directory '/d3a' on broker 3: timed out"));
  }

  @Test
  public void testDiskMovesCannotBeCombinedWithReplicaChanges() {
    // Within a partition.
    assertProblem(replicas(T0, List.of(1, 0, 2), "any", "/d0b", "any"),
                  "t-0: moving existing replicas between log directories {0=/d0b} cannot be combined with replica set or order changes");
    // Across partitions.
    String message = problemsOf(leader(T0, 1), logDirs(T1, Map.of(1, "/d1b")), replicas(T4, List.of(0, 1, 3)));
    assertTrue(message, message.startsWith("Cannot reassign partitions (1 problem):"));
    assertTrue(message, message.contains("Submit the replica set / order changes first [t-0, t-4], and once that execution finishes, "
                                         + "submit the log directory moves [t-1]."));
    // Leader elections and the placement of added replicas can be combined with either.
    build(leader(T1, 1), logDirs(T0, Map.of(1, "/d1b")));
    build(leader(T1, 1), replicas(T0, List.of(3, 1, 2), "/d3a", "any", "any"));
  }

  @Test
  public void testBrokersToDescribe() {
    List<RequestedPartitionReassignment> requested = List.of(leader(T0, 1), replicas(T2, List.of(3, 1, 4)),
                                                             leader(new TopicPartition("u", 0), 1));
    assertEquals(Set.of(0, 1, 2, 3), ReassignPartitionsProposalBuilder.brokersToDescribe(requested, CLUSTER));
  }

  @Test
  public void testMatchingLogDir() {
    assertEquals("/d", ReassignPartitionsProposalBuilder.matchingLogDir("/d/", Set.of("/d", "/e")));
    assertEquals("/d/", ReassignPartitionsProposalBuilder.matchingLogDir("/d", Set.of("/d/", "/e")));
    assertEquals("/d", ReassignPartitionsProposalBuilder.matchingLogDir("/d", Set.of("/d", "/d/")));
    assertNull(ReassignPartitionsProposalBuilder.matchingLogDir("/f", Set.of("/d", "/e")));
  }

  private static void assertProblem(RequestedPartitionReassignment requested, String expectedProblem) {
    String message = problemsOf(requested);
    assertTrue(message, message.startsWith("Cannot reassign partitions (1 problem):\n- " + expectedProblem));
  }
}
