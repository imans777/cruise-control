/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;

/**
 * Tracks the disk usage of the log dirs that the replica movements of an execution write to, so that a replica movement is
 * started only if each log dir it writes to can hold a full copy of the replica on top of what the log dir already holds.
 * <p>
 * Kafka copies a replica in full before it deletes the replica it replaces: the new replica of an inter-broker replica
 * movement on the destination broker, and the future replica of an intra-broker replica movement on the destination log dir.
 * The replaced replica is deleted {@code log.segment.delete.delay.ms} after the movement completes. The usage of a log dir is
 * tracked as:
 * <ul>
 *   <li>its usage in a snapshot of the log dirs (see {@code describeLogDirs}), i.e. the sum of the sizes of its replicas
 *   (including future replicas), or its total bytes minus its usable bytes if that is larger,</li>
 *   <li>plus the full size of each replica that a movement started with this tracker writes to it, however much of it has
 *   been copied,</li>
 *   <li>minus the size of each replica that a completed movement removed from it, once the
 *   {@code log.segment.delete.delay.ms} of its broker has elapsed since the movement completed.</li>
 * </ul>
 * A movement fits if, on each log dir it writes to, the tracked usage plus the size of the replica does not exceed the capacity of
 * the log dir: the smaller of its total bytes and its disk capacity in the broker capacity config. A log dir whose capacity is
 * known from neither is not checked. A movement to a broker whose log dirs could not be described does not fit, while the log
 * dirs of other brokers missing from the snapshot (e.g. dead brokers, to which movements fail anyway) are not checked.
 * <p>
 * Kafka picks the log dir of the new replica of an inter-broker replica movement: the online (and not cordoned) log dir with the
 * fewest partitions.
 * On a broker with several online log dirs, a movement is checked against, and charged to, each log dir that Kafka may pick
 * (i.e. the log dirs tied for the fewest partitions), unless the movement specifies the log dir. Once the movement has started,
 * {@link #locateNewReplicas(Map)} moves the charge to the log dir the replica was actually created in.
 * <p>
 * Movements of other types do not write replicas, hence always fit.
 * <p>
 * This class is not thread safe.
 */
final class DiskCapacityTracker {
  static final long DEFAULT_LOG_SEGMENT_DELETE_DELAY_MS = 60_000L;
  static final long BYTES_IN_MB = 1024L * 1024L;
  private static final Logger LOG = LoggerFactory.getLogger(DiskCapacityTracker.class);
  private static final int MAX_BLOCKED_MOVEMENTS_TO_REPORT = 10;
  // The online log dirs by broker, with their capacity and tracked usage.
  private final Map<Integer, Map<String, LogDir>> _logDirsByBroker;
  private final Set<Integer> _unreachableBrokers;
  // The log dir and size of the (non-future) replicas in the snapshot by partition and broker.
  private final Map<TopicPartition, Map<Integer, ReplicaLocation>> _replicasByPartition;
  private final Map<Integer, Long> _deleteDelayMsByBroker;
  private final Time _time;
  // The disk space charged to each started movement.
  private final Map<ExecutionTask, List<Charge>> _chargesByTask;
  // The disk space that each started movement frees once it has completed and its replaced replicas are deleted.
  private final List<Release> _pendingReleases;
  // The reason why each movement did not fit since the last call to beginPlanning().
  private final Map<ExecutionTask, String> _refusals;
  private final Set<TopicPartition> _partitionsReportedAsDeferred;

  /**
   * @param logDirsByBroker The log dirs of brokers involved in the movements to track, as described by {@code describeLogDirs}.
   *                        Movements to brokers missing from this map are not checked, unless they are unreachable.
   * @param unreachableBrokers The brokers whose log dirs could not be described. Movements to them do not fit.
   * @param capacityByBroker The capacity of brokers in the broker capacity config. Brokers missing from this map have no known
   *                         capacity in the config.
   * @param deleteDelayMsByBroker The {@code log.segment.delete.delay.ms} of brokers. Brokers missing from this map are considered
   *                              to use the default of {@value #DEFAULT_LOG_SEGMENT_DELETE_DELAY_MS} ms.
   * @param time The time object to tell when the replicas replaced by completed movements are deleted.
   */
  DiskCapacityTracker(Map<Integer, Map<String, LogDirDescription>> logDirsByBroker,
                      Set<Integer> unreachableBrokers,
                      Map<Integer, BrokerCapacityInfo> capacityByBroker,
                      Map<Integer, Long> deleteDelayMsByBroker,
                      Time time) {
    _logDirsByBroker = new HashMap<>();
    _unreachableBrokers = new HashSet<>(unreachableBrokers);
    _replicasByPartition = new HashMap<>();
    _deleteDelayMsByBroker = new HashMap<>(deleteDelayMsByBroker);
    _time = time;
    _chargesByTask = new HashMap<>();
    _pendingReleases = new ArrayList<>();
    _refusals = new LinkedHashMap<>();
    _partitionsReportedAsDeferred = new HashSet<>();
    logDirsByBroker.forEach((brokerId, logDirs) -> addBroker(brokerId, logDirs, capacityByBroker.get(brokerId)));
  }

  private void addBroker(int brokerId, Map<String, LogDirDescription> logDirs, BrokerCapacityInfo capacity) {
    Map<String, LogDir> onlineLogDirs = new HashMap<>();
    for (Map.Entry<String, LogDirDescription> entry : logDirs.entrySet()) {
      String logDir = entry.getKey();
      LogDirDescription description = entry.getValue();
      if (description.error() != null) {
        continue;
      }
      long sizeOfReplicas = 0L;
      for (Map.Entry<TopicPartition, ReplicaInfo> replica : description.replicaInfos().entrySet()) {
        sizeOfReplicas += replica.getValue().size();
        if (!replica.getValue().isFuture()) {
          _replicasByPartition.computeIfAbsent(replica.getKey(), tp -> new HashMap<>())
                              .put(brokerId, new ReplicaLocation(logDir, replica.getValue().size()));
        }
      }
      long used = sizeOfReplicas;
      if (description.totalBytes().isPresent() && description.usableBytes().isPresent()) {
        used = Math.max(used, description.totalBytes().getAsLong() - description.usableBytes().getAsLong());
      }
      Long capacityBytes = min(description.totalBytes().isPresent() ? description.totalBytes().getAsLong() : null,
                               configuredCapacityBytes(capacity, logDir, logDirs.size()));
      if (capacityBytes == null) {
        LOG.warn("The capacity of log dir {} of broker {} is unknown: Kafka does not report its total bytes, and the broker capacity "
                 + "config has no disk capacity for it. Replica movements to it are not checked against disk capacity.", logDir, brokerId);
      }
      onlineLogDirs.put(logDir, new LogDir(logDir, capacityBytes, used, description.replicaInfos().size(), description.isCordoned()));
    }
    _logDirsByBroker.put(brokerId, onlineLogDirs);
  }

  /**
   * @param capacity The capacity of the broker in the broker capacity config, or {@code null} if unknown.
   * @param logDir The log dir.
   * @param numLogDirs The number of log dirs of the broker.
   * @return The disk capacity of the given log dir in the broker capacity config in bytes, or {@code null} if unknown.
   */
  private static Long configuredCapacityBytes(BrokerCapacityInfo capacity, String logDir, int numLogDirs) {
    if (capacity == null) {
      return null;
    }
    Map<String, Double> diskCapacityByLogDir = capacity.diskCapacityByLogDir();
    if (diskCapacityByLogDir != null && diskCapacityByLogDir.containsKey(logDir)) {
      return (long) (diskCapacityByLogDir.get(logDir) * BYTES_IN_MB);
    }
    Double diskCapacity = capacity.capacity().get(Resource.DISK);
    // The disk capacity of a broker is the capacity of its only log dir.
    return numLogDirs == 1 && diskCapacity != null ? (long) (diskCapacity * BYTES_IN_MB) : null;
  }

  private static Long min(Long a, Long b) {
    if (a == null) {
      return b;
    }
    return b == null ? a : Math.min(a, b);
  }

  /**
   * Prepare for a planning call: release the disk space of the replicas that completed movements replaced and that are deleted
   * by now, and forget the movements that did not fit in the previous planning call.
   */
  void beginPlanning() {
    long now = _time.milliseconds();
    Iterator<Release> iterator = _pendingReleases.iterator();
    while (iterator.hasNext()) {
      Release release = iterator.next();
      if (release.isDue(now)) {
        LogDir logDir = _logDirsByBroker.get(release._brokerId).get(release._logDir);
        logDir._used -= release._bytes;
        logDir._numReplicas--;
        iterator.remove();
      }
    }
    _refusals.clear();
  }

  /**
   * Check whether the given movement fits the disk capacity of the log dirs it writes to. If it does not, the reason is kept
   * until the next call to {@link #beginPlanning()} (see {@link #refusals()}).
   *
   * @param task The movement to check.
   * @return {@code true} if the given movement fits, {@code false} otherwise.
   */
  boolean fits(ExecutionTask task) {
    List<Charge> charges = new ArrayList<>();
    String refusal = charges(task, charges);
    if (refusal == null) {
      for (Charge charge : charges) {
        for (String logDir : charge._logDirs) {
          LogDir dir = _logDirsByBroker.get(charge._brokerId).get(logDir);
          if (dir._capacity != null && dir._used + charge._bytes > dir._capacity) {
            refusal = String.format("it needs %d MB on log dir %s of broker %d, which has %d MB free of %d MB", toMb(charge._bytes),
                                    logDir, charge._brokerId, toMb(Math.max(0L, dir._capacity - dir._used)), toMb(dir._capacity));
            break;
          }
        }
        if (refusal != null) {
          break;
        }
      }
    }
    if (refusal == null) {
      return true;
    }
    _refusals.put(task, refusal);
    if (_partitionsReportedAsDeferred.add(task.proposal().topicPartition())) {
      LOG.info("Deferring the movement of partition {} ({}): {}. It starts once enough disk space is freed.",
               task.proposal().topicPartition(), task.executionId(), refusal);
    } else {
      LOG.debug("Deferring the movement of partition {} ({}): {}.", task.proposal().topicPartition(), task.executionId(), refusal);
    }
    return false;
  }

  /**
   * Charge the disk space of the given movement to the log dirs it writes to, and schedule the release of the disk space of the
   * replicas it replaces. To be called for each movement started after it has been checked with {@link #fits(ExecutionTask)}.
   *
   * @param task The movement to start.
   */
  void reserve(ExecutionTask task) {
    List<Charge> charges = new ArrayList<>();
    if (charges(task, charges) != null || _chargesByTask.containsKey(task)) {
      return;
    }
    for (Charge charge : charges) {
      for (String logDir : charge._logDirs) {
        LogDir dir = _logDirsByBroker.get(charge._brokerId).get(logDir);
        dir._used += charge._bytes;
        dir._numReplicas++;
      }
    }
    _chargesByTask.put(task, charges);
    scheduleReleases(task);
  }

  /**
   * Compute the disk space that the given movement needs on each log dir it writes to.
   *
   * @param task The movement.
   * @param charges A list to populate with the disk space the given movement needs on each broker.
   * @return {@code null} if the disk space the given movement needs is known, otherwise the reason why it is not.
   */
  private String charges(ExecutionTask task, List<Charge> charges) {
    ExecutionProposal proposal = task.proposal();
    if (task.type() == INTER_BROKER_REPLICA_ACTION) {
      long bytes = partitionSizeBytes(proposal);
      for (ReplicaPlacementInfo replicaToAdd : proposal.replicasToAdd()) {
        if (_unreachableBrokers.contains(replicaToAdd.brokerId())) {
          return String.format("the log dirs of its destination broker %d could not be described", replicaToAdd.brokerId());
        }
        Map<String, LogDir> logDirs = _logDirsByBroker.get(replicaToAdd.brokerId());
        // A movement to a broker without (online) log dirs fails anyway, there is nothing to charge.
        if (logDirs == null || logDirs.isEmpty()) {
          continue;
        }
        Set<String> candidates = candidateLogDirs(logDirs, replicaToAdd.logdir());
        charges.add(new Charge(replicaToAdd.brokerId(), candidates, bytes, candidates.size() == 1 && logDirs.size() == 1));
      }
    } else if (task.type() == INTRA_BROKER_REPLICA_ACTION) {
      int brokerId = task.brokerId();
      if (_unreachableBrokers.contains(brokerId)) {
        return String.format("the log dirs of broker %d could not be described", brokerId);
      }
      Map<String, LogDir> logDirs = _logDirsByBroker.get(brokerId);
      String destination = proposal.replicasToMoveBetweenDisksByBroker().get(brokerId).logdir();
      // A movement to an offline or unknown log dir fails anyway, there is nothing to charge.
      if (logDirs != null && logDirs.containsKey(destination)) {
        ReplicaLocation replica = replica(proposal.topicPartition(), brokerId);
        long bytes = replica != null ? replica._sizeBytes : partitionSizeBytes(proposal);
        charges.add(new Charge(brokerId, Collections.singleton(destination), bytes, true));
      }
    }
    return null;
  }

  /**
   * @param logDirs The online log dirs of a broker.
   * @param requestedLogDir The log dir that the movement requests for the new replica, or {@code null} if none.
   * @return The log dirs that the new replica may be created in: the requested log dir if online, otherwise the log dirs tied for
   * the fewest replicas, which Kafka picks from. Cordoned log dirs, which Kafka does not create new replicas in, are left out
   * unless all log dirs are cordoned.
   */
  private static Set<String> candidateLogDirs(Map<String, LogDir> logDirs, String requestedLogDir) {
    if (requestedLogDir != null && logDirs.containsKey(requestedLogDir)) {
      return Collections.singleton(requestedLogDir);
    }
    List<LogDir> eligible = new ArrayList<>();
    logDirs.values().stream().filter(dir -> !dir._cordoned).forEach(eligible::add);
    if (eligible.isEmpty()) {
      eligible.addAll(logDirs.values());
    }
    int fewestReplicas = eligible.stream().mapToInt(dir -> dir._numReplicas).min().orElse(0);
    Set<String> candidates = new TreeSet<>();
    eligible.stream().filter(dir -> dir._numReplicas == fewestReplicas).forEach(dir -> candidates.add(dir._name));
    return candidates;
  }

  private void scheduleReleases(ExecutionTask task) {
    ExecutionProposal proposal = task.proposal();
    if (task.type() == INTER_BROKER_REPLICA_ACTION) {
      for (ReplicaPlacementInfo replicaToRemove : proposal.replicasToRemove()) {
        maybeScheduleRelease(task, proposal.topicPartition(), replicaToRemove.brokerId());
      }
    } else if (task.type() == INTRA_BROKER_REPLICA_ACTION && !_chargesByTask.get(task).isEmpty()) {
      maybeScheduleRelease(task, proposal.topicPartition(), task.brokerId());
    }
  }

  private void maybeScheduleRelease(ExecutionTask task, TopicPartition tp, int brokerId) {
    ReplicaLocation replica = replica(tp, brokerId);
    Map<String, LogDir> logDirs = _logDirsByBroker.get(brokerId);
    if (replica != null && logDirs != null && logDirs.containsKey(replica._logDir)) {
      _pendingReleases.add(new Release(task, brokerId, replica._logDir, replica._sizeBytes,
                                       _deleteDelayMsByBroker.getOrDefault(brokerId, DEFAULT_LOG_SEGMENT_DELETE_DELAY_MS)));
    }
  }

  private ReplicaLocation replica(TopicPartition tp, int brokerId) {
    Map<Integer, ReplicaLocation> replicas = _replicasByPartition.get(tp);
    return replicas == null ? null : replicas.get(brokerId);
  }

  /**
   * @param proposal The proposal of a movement.
   * @return The size of the partition of the given proposal in bytes: the size of its largest replica in the snapshot, or the
   * size in the proposal if the snapshot has no replica of it.
   */
  private long partitionSizeBytes(ExecutionProposal proposal) {
    Map<Integer, ReplicaLocation> replicas = _replicasByPartition.get(proposal.topicPartition());
    if (replicas == null || replicas.isEmpty()) {
      // The data to move of a single intra-broker replica movement is the size of the partition.
      return proposal.intraBrokerDataToMoveInMB() * BYTES_IN_MB;
    }
    return replicas.values().stream().mapToLong(replica -> replica._sizeBytes).max().getAsLong();
  }

  /**
   * @return The new replicas of in-progress or completed inter-broker replica movements whose log dir is not known yet, i.e. the
   * new replicas on brokers with several online log dirs that have not been located by {@link #locateNewReplicas(Map)}.
   */
  Set<TopicPartitionReplica> newReplicasToLocate() {
    Set<TopicPartitionReplica> replicas = new HashSet<>();
    _chargesByTask.forEach((task, charges) -> {
      if (task.type() == INTER_BROKER_REPLICA_ACTION
          && (task.state() == ExecutionTaskState.IN_PROGRESS || task.state() == ExecutionTaskState.COMPLETED)) {
        charges.stream().filter(charge -> !charge._located)
               .forEach(charge -> replicas.add(new TopicPartitionReplica(task.proposal().topic(), task.proposal().partitionId(),
                                                                         charge._brokerId)));
      }
    });
    return replicas;
  }

  /**
   * Move the disk space charged for the new replicas of started inter-broker replica movements to the log dirs they were created in.
   *
   * @param logDirByReplica The log dir of new replicas, as reported by Kafka.
   */
  void locateNewReplicas(Map<TopicPartitionReplica, String> logDirByReplica) {
    _chargesByTask.forEach((task, charges) -> {
      if (task.type() != INTER_BROKER_REPLICA_ACTION) {
        return;
      }
      for (Charge charge : charges) {
        String logDir = logDirByReplica.get(new TopicPartitionReplica(task.proposal().topic(), task.proposal().partitionId(),
                                                                      charge._brokerId));
        if (charge._located || logDir == null) {
          continue;
        }
        Map<String, LogDir> logDirs = _logDirsByBroker.get(charge._brokerId);
        for (String candidate : charge._logDirs) {
          logDirs.get(candidate)._used -= charge._bytes;
          logDirs.get(candidate)._numReplicas--;
        }
        if (logDirs.containsKey(logDir)) {
          logDirs.get(logDir)._used += charge._bytes;
          logDirs.get(logDir)._numReplicas++;
          charge._logDirs = Collections.singleton(logDir);
        } else {
          charge._logDirs = Collections.emptySet();
        }
        charge._located = true;
      }
    });
  }

  /**
   * @return The movements that did not fit since the last call to {@link #beginPlanning()}, with the reason why.
   */
  Map<ExecutionTask, String> refusals() {
    return Collections.unmodifiableMap(_refusals);
  }

  /**
   * @return The number of movements started with this tracker.
   */
  int numStartedMovements() {
    return _chargesByTask.size();
  }

  /**
   * @return {@code true} if a completed movement replaced a replica that is not deleted yet, i.e. disk space that will be freed
   * without any further movement, {@code false} otherwise.
   */
  boolean hasPendingDeletions() {
    return _pendingReleases.stream().anyMatch(release -> release._task.state() == ExecutionTaskState.COMPLETED);
  }

  /**
   * Decide what to do when no movement is in progress and no remaining movement can start.
   *
   * @return The action to take.
   */
  BlockedAction whenNothingCanStart() {
    if (_refusals.isEmpty()) {
      return BlockedAction.NOT_BLOCKED_BY_DISK_CAPACITY;
    }
    if (hasPendingDeletions()) {
      return BlockedAction.WAIT_FOR_DELETIONS;
    }
    // The usage of a log dir is tracked from the snapshot on. Unless the snapshot was taken after the last movement started, take a
    // new one to catch the actual usage (e.g. partitions that grew) before failing the execution.
    return _chargesByTask.isEmpty() ? BlockedAction.FAIL : BlockedAction.RETAKE_SNAPSHOT;
  }

  /**
   * @return A description of the movements that did not fit since the last call to {@link #beginPlanning()}.
   */
  String describeRefusals() {
    StringJoiner description = new StringJoiner("; ");
    int numReported = 0;
    for (Map.Entry<ExecutionTask, String> refusal : _refusals.entrySet()) {
      if (numReported++ == MAX_BLOCKED_MOVEMENTS_TO_REPORT) {
        description.add(String.format("and %d more", _refusals.size() - MAX_BLOCKED_MOVEMENTS_TO_REPORT));
        break;
      }
      description.add(String.format("partition %s: %s", refusal.getKey().proposal().topicPartition(), refusal.getValue()));
    }
    return description.toString();
  }

  private static long toMb(long bytes) {
    return bytes / BYTES_IN_MB;
  }

  /**
   * The action to take when no movement is in progress and no remaining movement can start.
   */
  enum BlockedAction {
    // No remaining movement was deferred for disk capacity.
    NOT_BLOCKED_BY_DISK_CAPACITY,
    // Replicas replaced by completed movements are not deleted yet. Their disk space is freed without any further movement.
    WAIT_FOR_DELETIONS,
    // Take a new snapshot of the log dirs and check the remaining movements again.
    RETAKE_SNAPSHOT,
    // The remaining movements do not fit the snapshot, taken after the last movement started: fail the execution.
    FAIL
  }

  private static final class LogDir {
    private final String _name;
    private final Long _capacity;
    private final boolean _cordoned;
    private long _used;
    private int _numReplicas;

    LogDir(String name, Long capacity, long used, int numReplicas, boolean cordoned) {
      _name = name;
      _capacity = capacity;
      _used = used;
      _numReplicas = numReplicas;
      _cordoned = cordoned;
    }
  }

  private static final class ReplicaLocation {
    private final String _logDir;
    private final long _sizeBytes;

    ReplicaLocation(String logDir, long sizeBytes) {
      _logDir = logDir;
      _sizeBytes = sizeBytes;
    }
  }

  private static final class Charge {
    private final int _brokerId;
    private final long _bytes;
    private Set<String> _logDirs;
    private boolean _located;

    Charge(int brokerId, Set<String> logDirs, long bytes, boolean located) {
      _brokerId = brokerId;
      _logDirs = logDirs;
      _bytes = bytes;
      _located = located;
    }
  }

  private static final class Release {
    private final ExecutionTask _task;
    private final int _brokerId;
    private final String _logDir;
    private final long _bytes;
    private final long _deleteDelayMs;

    Release(ExecutionTask task, int brokerId, String logDir, long bytes, long deleteDelayMs) {
      _task = task;
      _brokerId = brokerId;
      _logDir = logDir;
      _bytes = bytes;
      _deleteDelayMs = deleteDelayMs;
    }

    boolean isDue(long nowMs) {
      return _task.state() == ExecutionTaskState.COMPLETED && nowMs >= _task.endTimeMs() + _deleteDelayMs;
    }
  }
}
