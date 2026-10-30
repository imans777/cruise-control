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

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;

/**
 * Checks that the destination logdir of a replica movement can hold a full copy of the replica before the movement starts.
 *
 * It is used by executions that create new replicas in the logdirs picked by the optimization (i.e. if intra-broker goals
 * are optimized together with inter-broker goals). Such an execution runs all inter-broker replica movements before all
 * intra-broker replica movements, so a logdir may receive new replicas before the intra-broker replica movements that free
 * it have run.
 *
 * The free space of a logdir is the smaller of (1) its capacity in the broker capacity config minus the bytes on it, and
 * (2) the usable bytes reported by the broker, minus the bytes that the replica movements in progress have yet to write to
 * it. A logdir with neither (1) nor (2), or of a broker whose logdirs could not be described, is not checked.
 */
final class LogdirSpaceCheck {
  private static final Logger LOG = LoggerFactory.getLogger(LogdirSpaceCheck.class);
  private static final long BYTES_IN_MB = 1024L * 1024L;
  private final Admin _adminClient;
  private final long _logdirResponseTimeoutMs;
  private final Set<Integer> _brokers;
  private final Map<Integer, Map<String, Double>> _capacityInMbByLogdirByBroker;
  // Free bytes of each checked logdir, net of the bytes reserved for the replica movements in progress or about to start.
  private final Map<Integer, Map<String, Long>> _freeBytesByLogdirByBroker;
  // Size of the largest replica of each partition on the checked brokers.
  private final Map<TopicPartition, Long> _replicaBytesByPartition;
  // Logdirs that could not hold a replica since the last refresh, with the bytes of the largest such replica.
  private final Map<Integer, Map<String, Long>> _neededBytesByBlockedLogdirByBroker;
  private final Set<Long> _deferredTaskIds;

  /**
   * @param adminClient The adminClient to send describeLogDirs requests.
   * @param config The config object that holds all the Cruise Control related configs.
   * @param brokers Brokers involved in the replica movements to check.
   * @param capacityInMbByLogdirByBroker Disk capacity in MB of each logdir (if available in the broker capacity config), by broker.
   */
  LogdirSpaceCheck(Admin adminClient,
                   KafkaCruiseControlConfig config,
                   Set<Integer> brokers,
                   Map<Integer, Map<String, Double>> capacityInMbByLogdirByBroker) {
    _adminClient = adminClient;
    _logdirResponseTimeoutMs = config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG);
    _brokers = brokers;
    _capacityInMbByLogdirByBroker = capacityInMbByLogdirByBroker;
    _freeBytesByLogdirByBroker = new HashMap<>();
    _replicaBytesByPartition = new HashMap<>();
    _neededBytesByBlockedLogdirByBroker = new HashMap<>();
    _deferredTaskIds = new HashSet<>();
  }

  /**
   * @param tasks Replica movement tasks.
   * @return {@code true} if any of the given tasks creates a replica in a logdir picked by the optimization.
   */
  static boolean hasPlannedLogdirs(Collection<ExecutionTask> tasks) {
    return tasks.stream().anyMatch(task -> task.type() == INTER_BROKER_REPLICA_ACTION
                                           && task.proposal().replicasToAdd().stream().anyMatch(r -> r.logdir() != null));
  }

  /**
   * @param task An intra-broker replica movement task.
   * @return The logdir that the replica of the given task moves from, or {@code null} if unknown.
   */
  static String sourceLogdir(ExecutionTask task) {
    return task.proposal().oldReplicas().stream().filter(r -> r.brokerId() == task.brokerId()).findFirst()
               .map(ReplicaPlacementInfo::logdir).orElse(null);
  }

  /**
   * Describe the logdirs of the checked brokers, and reserve the bytes that the given replica movements in progress have
   * yet to write to their destination logdirs. Clears the logdirs recorded as blocked since the last refresh.
   *
   * @param inExecutionTasks Replica movement tasks in progress.
   * @param replicasWithRecordedLogdirs Replicas to add whose logdirs their destination brokers recorded.
   */
  void refresh(Collection<ExecutionTask> inExecutionTasks, Set<TopicPartitionReplica> replicasWithRecordedLogdirs) {
    _freeBytesByLogdirByBroker.clear();
    _replicaBytesByPartition.clear();
    _neededBytesByBlockedLogdirByBroker.clear();
    // Logdir and size of each replica, and of each future replica, by broker.
    Map<Integer, Map<TopicPartition, String>> logdirByReplicaByBroker = new HashMap<>();
    Map<Integer, Map<TopicPartition, Long>> bytesByReplicaByBroker = new HashMap<>();
    Map<Integer, Map<TopicPartition, String>> logdirByFutureReplicaByBroker = new HashMap<>();
    Map<Integer, Map<TopicPartition, Long>> bytesByFutureReplicaByBroker = new HashMap<>();
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry
        : _adminClient.describeLogDirs(_brokers).descriptions().entrySet()) {
      int brokerId = entry.getKey();
      Map<String, LogDirDescription> descriptionByLogdir;
      try {
        descriptionByLogdir = entry.getValue().get(_logdirResponseTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        LOG.warn("Failed to describe the logdirs of broker {}, hence replica movements to it start without checking the space of "
                 + "their destination logdirs.", brokerId, e);
        continue;
      }
      Map<String, Double> capacityInMbByLogdir = _capacityInMbByLogdirByBroker.getOrDefault(brokerId, Collections.emptyMap());
      Map<String, Long> freeBytesByLogdir = new HashMap<>();
      descriptionByLogdir.forEach((logdir, description) -> {
        if (description.error() != null) {
          return;
        }
        long usedBytes = 0L;
        for (Map.Entry<TopicPartition, ReplicaInfo> replica : description.replicaInfos().entrySet()) {
          TopicPartition tp = replica.getKey();
          long bytes = replica.getValue().size();
          usedBytes += bytes;
          if (replica.getValue().isFuture()) {
            logdirByFutureReplicaByBroker.computeIfAbsent(brokerId, b -> new HashMap<>()).put(tp, logdir);
            bytesByFutureReplicaByBroker.computeIfAbsent(brokerId, b -> new HashMap<>()).put(tp, bytes);
          } else {
            logdirByReplicaByBroker.computeIfAbsent(brokerId, b -> new HashMap<>()).put(tp, logdir);
            bytesByReplicaByBroker.computeIfAbsent(brokerId, b -> new HashMap<>()).put(tp, bytes);
            _replicaBytesByPartition.merge(tp, bytes, Math::max);
          }
        }
        Long freeBytes = null;
        Double capacityInMb = capacityInMbByLogdir.get(logdir);
        if (capacityInMb != null) {
          freeBytes = (long) (capacityInMb * BYTES_IN_MB) - usedBytes;
        }
        if (description.usableBytes().isPresent()) {
          long usableBytes = description.usableBytes().getAsLong();
          freeBytes = freeBytes == null ? usableBytes : Math.min(freeBytes, usableBytes);
        }
        if (freeBytes != null) {
          freeBytesByLogdir.put(logdir, freeBytes);
        }
      });
      _freeBytesByLogdirByBroker.put(brokerId, freeBytesByLogdir);
    }

    for (ExecutionTask task : inExecutionTasks) {
      TopicPartition tp = task.proposal().topicPartition();
      long replicaBytes = replicaBytes(task);
      if (task.type() == INTER_BROKER_REPLICA_ACTION) {
        for (ReplicaPlacementInfo replicaToAdd : task.proposal().replicasToAdd()) {
          int brokerId = replicaToAdd.brokerId();
          String logdir = logdirByReplicaByBroker.getOrDefault(brokerId, Collections.emptyMap()).get(tp);
          if (logdir != null) {
            // The replica is being created: reserve the rest of it in the logdir that it is created in.
            reserve(brokerId, logdir, replicaBytes - bytesByReplicaByBroker.get(brokerId).get(tp));
          } else if (replicaToAdd.logdir() != null
                     && replicasWithRecordedLogdirs.contains(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId))) {
            reserve(brokerId, replicaToAdd.logdir(), replicaBytes);
          }
        }
      } else if (task.type() == INTRA_BROKER_REPLICA_ACTION) {
        int brokerId = task.brokerId();
        String destinationLogdir = task.proposal().replicasToMoveBetweenDisksByBroker().get(brokerId).logdir();
        if (!destinationLogdir.equals(logdirByReplicaByBroker.getOrDefault(brokerId, Collections.emptyMap()).get(tp))) {
          // The replica is being moved: reserve the rest of its future replica in the destination logdir.
          boolean isFutureReplicaCreated =
              destinationLogdir.equals(logdirByFutureReplicaByBroker.getOrDefault(brokerId, Collections.emptyMap()).get(tp));
          reserve(brokerId, destinationLogdir, replicaBytes - (isFutureReplicaCreated ? bytesByFutureReplicaByBroker.get(brokerId).get(tp) : 0L));
        }
      }
    }
  }

  /**
   * Check whether the destination logdirs of the given replica movement task can hold a full copy of its replica. If so,
   * reserve the space for it until the next {@link #refresh(Collection, Set)}. Otherwise, record the logdir as blocked.
   *
   * @param task A replica movement task about to start.
   * @return {@code true} if the task can start, {@code false} if it must wait for space on a destination logdir.
   */
  boolean tryReserve(ExecutionTask task) {
    Map<Integer, String> destinationLogdirByBroker = destinationLogdirs(task);
    long replicaBytes = replicaBytes(task);
    for (Map.Entry<Integer, String> destination : destinationLogdirByBroker.entrySet()) {
      Long freeBytes = _freeBytesByLogdirByBroker.getOrDefault(destination.getKey(), Collections.emptyMap()).get(destination.getValue());
      if (freeBytes != null && freeBytes < replicaBytes) {
        _neededBytesByBlockedLogdirByBroker.computeIfAbsent(destination.getKey(), b -> new HashMap<>())
                                           .merge(destination.getValue(), replicaBytes, Math::max);
        if (_deferredTaskIds.add(task.executionId())) {
          LOG.info("Task {} waits for space: logdir {} of broker {} can hold {} MB more, the replica needs {} MB.", task,
                   destination.getValue(), destination.getKey(), Math.max(0L, freeBytes) / BYTES_IN_MB, replicaBytes / BYTES_IN_MB);
        }
        return false;
      }
    }
    destinationLogdirByBroker.forEach((brokerId, logdir) -> reserve(brokerId, logdir, replicaBytes));
    return true;
  }

  /**
   * @return {@code true} if a replica movement task could not start for want of space since the last refresh.
   */
  boolean hasBlockedLogdirs() {
    return !_neededBytesByBlockedLogdirByBroker.isEmpty();
  }

  /**
   * @return Logdirs that could not hold a replica since the last refresh, by broker.
   */
  Map<Integer, Set<String>> blockedLogdirs() {
    Map<Integer, Set<String>> blockedLogdirs = new HashMap<>();
    _neededBytesByBlockedLogdirByBroker.forEach((brokerId, neededBytesByLogdir) ->
        blockedLogdirs.put(brokerId, new HashSet<>(neededBytesByLogdir.keySet())));
    return blockedLogdirs;
  }

  /**
   * @return A description of the logdirs that could not hold a replica since the last refresh.
   */
  String describeBlockedLogdirs() {
    StringJoiner description = new StringJoiner(", ");
    _neededBytesByBlockedLogdirByBroker.forEach((brokerId, neededBytesByLogdir) -> neededBytesByLogdir.forEach((logdir, neededBytes) -> {
      long freeBytes = Math.max(0L, _freeBytesByLogdirByBroker.get(brokerId).get(logdir));
      description.add(String.format("logdir %s of broker %d (%d MB free, a replica needs %d MB)", logdir, brokerId,
                                    freeBytes / BYTES_IN_MB, neededBytes / BYTES_IN_MB));
    }));
    return description.toString();
  }

  private void reserve(int brokerId, String logdir, long bytes) {
    Map<String, Long> freeBytesByLogdir = _freeBytesByLogdirByBroker.get(brokerId);
    if (freeBytesByLogdir != null && freeBytesByLogdir.containsKey(logdir) && bytes > 0) {
      freeBytesByLogdir.put(logdir, freeBytesByLogdir.get(logdir) - bytes);
    }
  }

  /**
   * @param task A replica movement task.
   * @return The bytes of a full copy of the replica of the given task: the larger of the size in the proposal and the size of
   * the largest replica of the partition on the checked brokers.
   */
  private long replicaBytes(ExecutionTask task) {
    // The intra-broker data to move of a proposal is the size of a replica of its partition.
    return Math.max(task.proposal().intraBrokerDataToMoveInMB() * BYTES_IN_MB,
                    _replicaBytesByPartition.getOrDefault(task.proposal().topicPartition(), 0L));
  }

  /**
   * @param task A replica movement task.
   * @return The logdirs that the given task creates replicas in (if known), by broker.
   */
  private static Map<Integer, String> destinationLogdirs(ExecutionTask task) {
    Map<Integer, String> destinationLogdirByBroker = new HashMap<>();
    if (task.type() == INTER_BROKER_REPLICA_ACTION) {
      task.proposal().replicasToAdd().stream().filter(r -> r.logdir() != null)
          .forEach(r -> destinationLogdirByBroker.put(r.brokerId(), r.logdir()));
    } else if (task.type() == INTRA_BROKER_REPLICA_ACTION) {
      destinationLogdirByBroker.put(task.brokerId(), task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId()).logdir());
    }
    return destinationLogdirByBroker;
  }
}
