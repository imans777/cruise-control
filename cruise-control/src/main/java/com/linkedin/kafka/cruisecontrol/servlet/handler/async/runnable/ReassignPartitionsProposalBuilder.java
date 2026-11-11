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
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReassignPartitionsBodyParser;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RequestedPartitionReassignment;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.PartitionReassignmentDetails;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;

import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.withoutTrailingSeparator;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ReassignPartitionsBodyParser.LOG_DIRS;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ReassignPartitionsBodyParser.REPLICAS;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.RequestedPartitionReassignment.ANY_LOG_DIR;


/**
 * Builds the execution proposals of a manual partition reassignment from the requested reassignments and a fresh view of the
 * cluster -- i.e. the partition metadata of the requested topics, the alive brokers, the ongoing partition reassignments and the
 * log directories of the involved brokers. It validates the request against the cluster, and reports all the problems together.
 *
 * The first replica of the new replica list of a partition becomes its leader once the proposal is executed. A request may
 * either change replica sets / orders (and leaders), or move replicas between the log directories of their brokers (and change
 * leaders), because the executor runs these in separate phases.
 */
final class ReassignPartitionsProposalBuilder {
  static final String UNKNOWN_LOG_DIR = "unknown";
  private static final double BYTES_IN_MB = 1024.0 * 1024.0;
  private final Cluster _cluster;
  private final Set<Integer> _aliveBrokers;
  private final Set<TopicPartition> _partitionsBeingReassigned;
  private final Map<Integer, Map<String, LogDirDescription>> _logDirsByBroker;
  private final Map<Integer, String> _logDirErrorByBroker;
  private final Map<TopicPartition, Map<Integer, String>> _currentLogDirByReplica;
  private final Map<TopicPartition, Map<Integer, String>> _futureLogDirByReplica;
  private final Map<TopicPartition, Map<Integer, Long>> _sizeInBytesByReplica;
  private final List<String> _problems;
  private final List<PartitionReassignmentDetails> _partitions;
  private final Map<TopicPartition, ExecutionProposal> _proposals;
  private final Map<TopicPartitionReplica, String> _logDirHints;
  private final List<TopicPartition> _withIntraBrokerReplicaMovements;
  private final List<TopicPartition> _withInterBrokerReplicaChanges;

  private ReassignPartitionsProposalBuilder(Collection<TopicPartition> requestedPartitions,
                                            Cluster cluster,
                                            Set<TopicPartition> partitionsBeingReassigned,
                                            Map<Integer, Map<String, LogDirDescription>> logDirsByBroker,
                                            Map<Integer, String> logDirErrorByBroker) {
    _cluster = cluster;
    _aliveBrokers = cluster.nodes().stream().map(Node::id).collect(Collectors.toSet());
    _partitionsBeingReassigned = partitionsBeingReassigned;
    _logDirsByBroker = logDirsByBroker;
    _logDirErrorByBroker = logDirErrorByBroker;
    _currentLogDirByReplica = new HashMap<>();
    _futureLogDirByReplica = new HashMap<>();
    _sizeInBytesByReplica = new HashMap<>();
    Set<TopicPartition> partitionsOfInterest = new HashSet<>(requestedPartitions);
    logDirsByBroker.forEach((broker, logDirs) -> logDirs.forEach((logDir, description) -> {
      for (Map.Entry<TopicPartition, ReplicaInfo> entry : description.replicaInfos().entrySet()) {
        TopicPartition tp = entry.getKey();
        if (!partitionsOfInterest.contains(tp)) {
          continue;
        }
        if (entry.getValue().isFuture()) {
          _futureLogDirByReplica.computeIfAbsent(tp, k -> new HashMap<>()).put(broker, logDir);
        } else {
          _currentLogDirByReplica.computeIfAbsent(tp, k -> new HashMap<>()).put(broker, logDir);
          _sizeInBytesByReplica.computeIfAbsent(tp, k -> new HashMap<>()).put(broker, entry.getValue().size());
        }
      }
    }));
    _problems = new ArrayList<>();
    _partitions = new ArrayList<>();
    _proposals = new LinkedHashMap<>();
    _logDirHints = new LinkedHashMap<>();
    _withIntraBrokerReplicaMovements = new ArrayList<>();
    _withInterBrokerReplicaChanges = new ArrayList<>();
  }

  /**
   * @param requested Requested partition reassignments.
   * @param cluster Fresh metadata of the requested topics and the alive brokers.
   * @return Alive brokers which currently host, or are requested to host, a replica of the requested partitions -- i.e. the
   * brokers whose log directories are relevant to the request.
   */
  static Set<Integer> brokersToDescribe(List<RequestedPartitionReassignment> requested, Cluster cluster) {
    Set<Integer> brokers = new TreeSet<>();
    for (RequestedPartitionReassignment reassignment : requested) {
      PartitionInfo partitionInfo = cluster.partition(reassignment.topicPartition());
      if (partitionInfo != null) {
        Arrays.stream(partitionInfo.replicas()).forEach(node -> brokers.add(node.id()));
      }
      if (reassignment.form() == RequestedPartitionReassignment.Form.REPLICAS) {
        brokers.addAll(reassignment.replicas());
      }
    }
    brokers.retainAll(cluster.nodes().stream().map(Node::id).collect(Collectors.toSet()));
    return brokers;
  }

  /**
   * Build the plan of the given partition reassignment.
   *
   * @param requested Requested partition reassignments.
   * @param cluster Fresh metadata of the requested topics and the alive brokers.
   * @param partitionsBeingReassigned Partitions with an ongoing reassignment.
   * @param logDirsByBroker Log directory descriptions by log directory by broker, for (at least) the brokers to describe.
   * @param logDirErrorByBroker The error message by broker whose log directories could not be described.
   * @return The plan of the given partition reassignment.
   */
  static ReassignPartitionsPlan build(List<RequestedPartitionReassignment> requested,
                                      Cluster cluster,
                                      Set<TopicPartition> partitionsBeingReassigned,
                                      Map<Integer, Map<String, LogDirDescription>> logDirsByBroker,
                                      Map<Integer, String> logDirErrorByBroker) {
    List<TopicPartition> requestedPartitions = requested.stream().map(RequestedPartitionReassignment::topicPartition)
                                                        .collect(Collectors.toList());
    ReassignPartitionsProposalBuilder builder =
        new ReassignPartitionsProposalBuilder(requestedPartitions, cluster, partitionsBeingReassigned, logDirsByBroker, logDirErrorByBroker);
    requested.forEach(builder::plan);
    return builder.toPlan();
  }

  private ReassignPartitionsPlan toPlan() {
    if (!_withIntraBrokerReplicaMovements.isEmpty() && !_withInterBrokerReplicaChanges.isEmpty()) {
      _problems.add(String.format("Moving existing replicas between log directories cannot be combined with replica set or order changes "
                                  + "in the same request, because they are executed in separate phases. Submit the replica set / order "
                                  + "changes first %s, and once that execution finishes, submit the log directory moves %s.",
                                  _withInterBrokerReplicaChanges, _withIntraBrokerReplicaMovements));
    }
    if (!_problems.isEmpty()) {
      throw ReassignPartitionsBodyParser.problems("Cannot reassign partitions", _problems);
    }
    return new ReassignPartitionsPlan(_partitions, _proposals, _logDirHints, !_withIntraBrokerReplicaMovements.isEmpty());
  }

  private void problem(TopicPartition tp, String format, Object... args) {
    _problems.add(tp + ": " + String.format(format, args));
  }

  private void plan(RequestedPartitionReassignment request) {
    TopicPartition tp = request.topicPartition();
    if (!_cluster.topics().contains(tp.topic())) {
      problem(tp, "topic '%s' does not exist.", tp.topic());
      return;
    }
    PartitionInfo partitionInfo = _cluster.partition(tp);
    if (partitionInfo == null) {
      problem(tp, "topic '%s' has %d partitions.", tp.topic(), _cluster.partitionCountForTopic(tp.topic()));
      return;
    }
    List<Integer> currentReplicas = Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList());
    Node leaderNode = partitionInfo.leader();
    if (leaderNode == null || leaderNode.id() < 0 || !currentReplicas.contains(leaderNode.id())) {
      problem(tp, "the partition has no leader (i.e. it is offline); bring a replica back online or use fix_offline_replicas.");
      return;
    }
    if (_partitionsBeingReassigned.contains(tp)) {
      problem(tp, "the partition is being reassigned; wait for the ongoing reassignment to finish, or stop it with "
                  + "stop_proposal_execution?stop_external_agent=true.");
      return;
    }
    int currentLeader = leaderNode.id();
    Set<Integer> inSyncReplicas = Arrays.stream(partitionInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toCollection(TreeSet::new));

    // Expand the request into the new replica list and the requested log directory by broker.
    int numProblems = _problems.size();
    List<Integer> newReplicas;
    Map<Integer, String> requestedLogDirByBroker = new LinkedHashMap<>();
    switch (request.form()) {
      case LEADER:
        int leader = request.leader();
        if (!currentReplicas.contains(leader)) {
          problem(tp, "the requested leader %d is not a replica (current replicas: %s); use '%s' to move a replica to it.",
                  leader, currentReplicas, REPLICAS);
          return;
        }
        newReplicas = new ArrayList<>(currentReplicas.size());
        newReplicas.add(leader);
        currentReplicas.stream().filter(broker -> broker != leader).forEach(newReplicas::add);
        break;
      case LOG_DIRS:
        for (int broker : request.logDirByBroker().keySet()) {
          if (!currentReplicas.contains(broker)) {
            problem(tp, "'%s' refers to broker %d, which does not host this partition (current replicas: %s).", LOG_DIRS, broker,
                    currentReplicas);
          }
        }
        newReplicas = currentReplicas;
        requestedLogDirByBroker.putAll(request.logDirByBroker());
        break;
      case REPLICAS:
        newReplicas = request.replicas();
        if (request.logDirs() != null) {
          for (int i = 0; i < newReplicas.size(); i++) {
            if (!ANY_LOG_DIR.equalsIgnoreCase(request.logDirs().get(i))) {
              requestedLogDirByBroker.put(newReplicas.get(i), request.logDirs().get(i));
            }
          }
        }
        break;
      default:
        throw new IllegalStateException("Unrecognized form " + request.form());
    }
    if (_problems.size() != numProblems) {
      return;
    }

    Set<Integer> currentReplicaSet = new HashSet<>(currentReplicas);
    Set<Integer> newReplicaSet = new HashSet<>(newReplicas);
    List<Integer> replicasToAdd = newReplicas.stream().filter(broker -> !currentReplicaSet.contains(broker)).collect(Collectors.toList());
    List<Integer> replicasToRemove = currentReplicas.stream().filter(broker -> !newReplicaSet.contains(broker)).collect(Collectors.toList());
    boolean replicaSetChange = !replicasToAdd.isEmpty() || !replicasToRemove.isEmpty();
    boolean replicationFactorChange = newReplicas.size() != currentReplicas.size();
    boolean replicaOrderChange = !replicaSetChange && !newReplicas.equals(currentReplicas);
    int newLeader = newReplicas.get(0);
    boolean leaderChange = newLeader != currentLeader;

    // The executor stops the whole execution if a broker in the new replica list of an inter-broker replica action is not alive.
    if (replicaSetChange || replicaOrderChange) {
      List<Integer> deadBrokers = newReplicas.stream().filter(broker -> !_aliveBrokers.contains(broker)).collect(Collectors.toList());
      if (!deadBrokers.isEmpty()) {
        problem(tp, "brokers %s in the new replica list are not alive; replace the replicas on dead brokers in the same request (using "
                    + "'%s').", deadBrokers, REPLICAS);
      }
    } else if (leaderChange && !_aliveBrokers.contains(newLeader)) {
      problem(tp, "the new leader %d is not alive.", newLeader);
    }
    if (leaderChange && currentReplicaSet.contains(newLeader) && !inSyncReplicas.contains(newLeader)) {
      problem(tp, "the new leader %d is not in the in-sync replicas %s.", newLeader, inSyncReplicas);
    }

    long sizeInBytes = partitionSizeInBytes(tp, currentLeader);
    Map<Integer, String> currentLogDirByBroker = _currentLogDirByReplica.getOrDefault(tp, Collections.emptyMap());
    Map<Integer, String> diskMoveByBroker = new LinkedHashMap<>();
    Map<Integer, String> placementByAddedBroker = new LinkedHashMap<>();
    for (Map.Entry<Integer, String> entry : requestedLogDirByBroker.entrySet()) {
      int broker = entry.getKey();
      String logDir = validatedLogDir(tp, broker, entry.getValue(), currentLogDirByBroker.get(broker), sizeInBytes);
      if (logDir == null) {
        continue;
      }
      if (currentReplicaSet.contains(broker)) {
        if (!logDir.equals(currentLogDirByBroker.get(broker))) {
          diskMoveByBroker.put(broker, logDir);
        }
      } else {
        placementByAddedBroker.put(broker, logDir);
      }
    }
    // The executor drops replica reorders and refuses replica additions in an execution with intra-broker replica movements.
    if (!diskMoveByBroker.isEmpty() && (replicaSetChange || replicaOrderChange)) {
      problem(tp, "moving existing replicas between log directories %s cannot be combined with replica set or order changes; submit the "
                  + "log directory moves in a separate request once the reassignment of the replicas finishes.", diskMoveByBroker);
    }
    if (_problems.size() != numProblems) {
      return;
    }

    List<String> warnings = warnings(request, tp, currentLeader, newReplicas, newLeader, leaderChange, replicaSetChange,
                                     currentReplicaSet, inSyncReplicas);
    Set<ReassignmentAction> actions = EnumSet.noneOf(ReassignmentAction.class);
    if (replicaSetChange) {
      actions.add(ReassignmentAction.REPLICA_SET_CHANGE);
    }
    if (replicationFactorChange) {
      actions.add(ReassignmentAction.REPLICATION_FACTOR_CHANGE);
    }
    if (replicaOrderChange) {
      actions.add(ReassignmentAction.REPLICA_ORDER_CHANGE);
    }
    if (!diskMoveByBroker.isEmpty()) {
      actions.add(ReassignmentAction.INTRA_BROKER_REPLICA_MOVEMENT);
    }
    if (leaderChange) {
      actions.add(ReassignmentAction.LEADERSHIP_MOVEMENT);
    }
    long sizeInMB = (long) Math.ceil(sizeInBytes / BYTES_IN_MB);
    List<String> currentLogDirs = currentReplicas.stream().map(broker -> currentLogDirByBroker.getOrDefault(broker, UNKNOWN_LOG_DIR))
                                                 .collect(Collectors.toList());
    List<String> newLogDirs = new ArrayList<>(newReplicas.size());
    for (int broker : newReplicas) {
      if (diskMoveByBroker.containsKey(broker)) {
        newLogDirs.add(diskMoveByBroker.get(broker));
      } else if (currentReplicaSet.contains(broker)) {
        newLogDirs.add(currentLogDirByBroker.getOrDefault(broker, UNKNOWN_LOG_DIR));
      } else {
        newLogDirs.add(placementByAddedBroker.getOrDefault(broker, ANY_LOG_DIR));
      }
    }
    _partitions.add(new PartitionReassignmentDetails(tp, currentReplicas, currentLeader, currentLogDirs, newReplicas, newLeader, newLogDirs,
                                                     actions, sizeInMB, replicasToAdd.size(), diskMoveByBroker.size(), warnings));
    if (actions.isEmpty()) {
      return;
    }

    // Retained replicas must use the identical placement for the executor to recognize replicas to move between disks.
    Map<Integer, ReplicaPlacementInfo> oldPlacementByBroker = new LinkedHashMap<>();
    currentReplicas.forEach(broker -> oldPlacementByBroker.put(broker, new ReplicaPlacementInfo(broker, currentLogDirByBroker.get(broker))));
    List<ReplicaPlacementInfo> newPlacements = new ArrayList<>(newReplicas.size());
    for (int broker : newReplicas) {
      if (diskMoveByBroker.containsKey(broker)) {
        newPlacements.add(new ReplicaPlacementInfo(broker, diskMoveByBroker.get(broker)));
      } else if (currentReplicaSet.contains(broker)) {
        newPlacements.add(oldPlacementByBroker.get(broker));
      } else {
        newPlacements.add(new ReplicaPlacementInfo(broker, placementByAddedBroker.get(broker)));
      }
    }
    try {
      _proposals.put(tp, new ExecutionProposal(tp, sizeInMB, oldPlacementByBroker.get(currentLeader),
                                               new ArrayList<>(oldPlacementByBroker.values()), newPlacements));
    } catch (IllegalArgumentException iae) {
      problem(tp, "%s", iae.getMessage());
      return;
    }
    placementByAddedBroker.forEach((broker, logDir) -> _logDirHints.put(new TopicPartitionReplica(tp.topic(), tp.partition(), broker),
                                                                         logDir));
    if (!diskMoveByBroker.isEmpty()) {
      _withIntraBrokerReplicaMovements.add(tp);
    }
    if (replicaSetChange || replicaOrderChange) {
      _withInterBrokerReplicaChanges.add(tp);
    }
  }

  private List<String> warnings(RequestedPartitionReassignment request,
                                TopicPartition tp,
                                int currentLeader,
                                List<Integer> newReplicas,
                                int newLeader,
                                boolean leaderChange,
                                boolean replicaSetChange,
                                Set<Integer> currentReplicaSet,
                                Set<Integer> inSyncReplicas) {
    List<String> warnings = new ArrayList<>();
    if (request.form() == RequestedPartitionReassignment.Form.LOG_DIRS && leaderChange) {
      warnings.add(String.format("The current leader %d is not the preferred leader; executing this request also moves the leadership to "
                                 + "the preferred leader %d.", currentLeader, newLeader));
    }
    if (replicaSetChange) {
      List<Integer> outOfSyncRetainedReplicas = newReplicas.stream().filter(b -> currentReplicaSet.contains(b) && !inSyncReplicas.contains(b))
                                                           .collect(Collectors.toList());
      if (!outOfSyncRetainedReplicas.isEmpty()) {
        warnings.add(String.format("Replicas %s are out of sync; the reassignment completes only once they rejoin the in-sync replicas.",
                                   outOfSyncRetainedReplicas));
      }
      Map<String, List<Integer>> brokersByRack = new TreeMap<>();
      for (int broker : newReplicas) {
        Node node = _cluster.nodeById(broker);
        if (node == null || !node.hasRack()) {
          return warnings;
        }
        brokersByRack.computeIfAbsent(node.rack(), r -> new ArrayList<>()).add(broker);
      }
      long numRacks = _cluster.nodes().stream().filter(Node::hasRack).map(Node::rack).distinct().count();
      if (brokersByRack.size() < Math.min(newReplicas.size(), numRacks)) {
        Map<String, List<Integer>> sharedRacks = brokersByRack.entrySet().stream().filter(e -> e.getValue().size() > 1)
                                                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                                                                        (a, b) -> a, TreeMap::new));
        warnings.add(String.format("The new replicas are not rack-aware: they share racks %s.", sharedRacks));
      }
    }
    return warnings;
  }

  /**
   * @param tp Topic partition.
   * @param leader Current leader of the partition.
   * @return The size of the leader replica of the partition, or the size of its largest replica if the size of the leader replica is
   * unknown, or 0 if no replica size is known.
   */
  private long partitionSizeInBytes(TopicPartition tp, int leader) {
    Map<Integer, Long> sizeByBroker = _sizeInBytesByReplica.getOrDefault(tp, Collections.emptyMap());
    Long leaderSize = sizeByBroker.get(leader);
    return leaderSize != null ? leaderSize : sizeByBroker.values().stream().mapToLong(Long::longValue).max().orElse(0L);
  }

  /**
   * Validate the given requested log directory on the given broker.
   *
   * @param tp Topic partition.
   * @param broker Broker of the requested log directory.
   * @param requestedLogDir Requested log directory.
   * @param currentLogDir Current log directory of the replica on the broker, or {@code null} if the broker does not host the replica.
   * @param replicaSizeInBytes Size of the replica.
   * @return The log directory as reported by the broker, or {@code null} if the requested log directory is invalid.
   */
  private String validatedLogDir(TopicPartition tp, int broker, String requestedLogDir, String currentLogDir, long replicaSizeInBytes) {
    if (!_aliveBrokers.contains(broker)) {
      problem(tp, "cannot verify log directory '%s', because broker %d is not alive.", requestedLogDir, broker);
      return null;
    }
    if (_logDirErrorByBroker.containsKey(broker)) {
      problem(tp, "cannot verify log directory '%s' on broker %d: %s", requestedLogDir, broker, _logDirErrorByBroker.get(broker));
      return null;
    }
    Map<String, LogDirDescription> logDirs = _logDirsByBroker.getOrDefault(broker, Collections.emptyMap());
    String logDir = matchingLogDir(requestedLogDir, logDirs.keySet());
    if (logDir == null) {
      problem(tp, "broker %d has no log directory '%s' (log directories: %s).", broker, requestedLogDir, new TreeSet<>(logDirs.keySet()));
      return null;
    }
    LogDirDescription description = logDirs.get(logDir);
    if (description.error() != null) {
      problem(tp, "log directory '%s' on broker %d is offline (%s).", logDir, broker, description.error().getMessage());
      return null;
    }
    if (logDir.equals(currentLogDir)) {
      return logDir;
    }
    if (description.isCordoned()) {
      problem(tp, "log directory '%s' on broker %d is cordoned.", logDir, broker);
      return null;
    }
    String futureLogDir = _futureLogDirByReplica.getOrDefault(tp, Collections.emptyMap()).get(broker);
    if (futureLogDir != null) {
      problem(tp, "the replica on broker %d is already being moved to log directory '%s'.", broker, futureLogDir);
      return null;
    }
    if (description.usableBytes().isPresent() && description.usableBytes().getAsLong() <= replicaSizeInBytes) {
      problem(tp, "log directory '%s' on broker %d has %d MB usable space, which does not fit the replica of %d MB.", logDir, broker,
              (long) (description.usableBytes().getAsLong() / BYTES_IN_MB), (long) Math.ceil(replicaSizeInBytes / BYTES_IN_MB));
      return null;
    }
    return logDir;
  }

  /**
   * @param requestedLogDir Requested log directory.
   * @param logDirs Log directories of a broker.
   * @return The log directory of the broker matching the requested one regardless of a trailing separator, or {@code null} if none.
   */
  static String matchingLogDir(String requestedLogDir, Set<String> logDirs) {
    if (logDirs.contains(requestedLogDir)) {
      return requestedLogDir;
    }
    String normalized = withoutTrailingSeparator(requestedLogDir);
    return logDirs.stream().filter(logDir -> withoutTrailingSeparator(logDir).equals(normalized)).findFirst().orElse(null);
  }

  /**
   * The plan of a manual partition reassignment.
   */
  static final class ReassignPartitionsPlan {
    private final List<PartitionReassignmentDetails> _partitions;
    private final Map<TopicPartition, ExecutionProposal> _proposals;
    private final Map<TopicPartitionReplica, String> _logDirHints;
    private final boolean _hasIntraBrokerReplicaMovements;

    ReassignPartitionsPlan(List<PartitionReassignmentDetails> partitions,
                           Map<TopicPartition, ExecutionProposal> proposals,
                           Map<TopicPartitionReplica, String> logDirHints,
                           boolean hasIntraBrokerReplicaMovements) {
      _partitions = Collections.unmodifiableList(partitions);
      _proposals = Collections.unmodifiableMap(proposals);
      _logDirHints = Collections.unmodifiableMap(logDirHints);
      _hasIntraBrokerReplicaMovements = hasIntraBrokerReplicaMovements;
    }

    /**
     * @return Details of each requested partition reassignment, in the order of the request.
     */
    List<PartitionReassignmentDetails> partitions() {
      return _partitions;
    }

    /**
     * @return The execution proposal of each partition to change, in the order of the request.
     */
    Map<TopicPartition, ExecutionProposal> proposals() {
      return _proposals;
    }

    /**
     * @return The proposals to execute.
     */
    Set<ExecutionProposal> proposalsToExecute() {
      return new LinkedHashSet<>(_proposals.values());
    }

    /**
     * @return The requested log directory of each replica to add to a new broker, if specified.
     */
    Map<TopicPartitionReplica, String> logDirHints() {
      return _logDirHints;
    }

    /**
     * @return {@code true} if the plan moves replicas between log directories of their brokers, {@code false} otherwise.
     */
    boolean hasIntraBrokerReplicaMovements() {
      return _hasIntraBrokerReplicaMovements;
    }
  }
}
