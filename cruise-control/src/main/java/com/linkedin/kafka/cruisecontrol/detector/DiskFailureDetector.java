/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

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

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.DISK_FAILURES_CLASS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.FAILED_DISKS_FILE_PATH_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.brokersWithOfflineReplicas;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.brokersWithOfflineReplicasOfLeaderlessPartitions;

/**
 * This class detects disk failures.
 *
 * The detector keeps the time each failed disk was first detected, and reports it as the failure time of the disk. This lets
 * the self-healing of a disk failure wait until a given time since the failure has passed. The failure time of fixable disk
 * failures is persisted in {@link com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig#FAILED_DISKS_FILE_PATH_CONFIG},
 * so that it survives a restart of Cruise Control.
 **/
public class DiskFailureDetector extends AbstractAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureDetector.class);
  public static final String FAILED_DISKS_OBJECT_CONFIG = "failed.disks.object";
  // Config to indicate whether detected disk failures are fixable or not.
  public static final String DISK_FAILURES_FIXABLE_CONFIG = "disk.failures.fixable.object";
  private final Admin _adminClient;
  private ModelGeneration _lastCheckedModelGeneration;
  private final KafkaCruiseControlConfig _config;
  // Failed disks of brokers with offline replicas that can be moved, and the time they were first detected, grouped by broker.
  private final Map<Integer, Map<String, Long>> _failedDisks;
  // Failed disks of brokers with only offline replicas that cannot be moved, and the time they were first detected, grouped by broker.
  private final Map<Integer, Map<String, Long>> _unfixableFailedDisks;
  // Failed disks that are not reported because their broker has no offline replicas, grouped by broker.
  private final Map<Integer, Set<String>> _emptyFailedDisks;
  private final File _failedDisksFile;

  public DiskFailureDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
    super(anomalies, kafkaCruiseControl);
    _adminClient = kafkaCruiseControl.adminClient();
    _lastCheckedModelGeneration = new ModelGeneration(0, -1L);
    _config = _kafkaCruiseControl.config();
    _failedDisks = new HashMap<>();
    _unfixableFailedDisks = new HashMap<>();
    _emptyFailedDisks = new HashMap<>();
    _failedDisksFile = new File(_config.getString(FAILED_DISKS_FILE_PATH_CONFIG));
    // Load the failed disk information.
    parsePersistedFailedDisks(loadPersistedFailedDisks());
  }

  /**
   * Retrieve the {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the disk failure detector
   * is ready to check for an anomaly.
   *
   * Skip disk failure detection if any of the following is satisfied:
   * <ul>
   *   <li>Cluster model generation has not changed since the last disk failure check, unless skipModelGenerationCheck is
   *   {@code true}.</li>
   *   <li>There are dead brokers in the cluster,
   *   {@link KafkaBrokerFailureDetector} should take care of the anomaly.</li>
   *   <li>{@link AnomalyDetectorUtils#getAnomalyDetectionStatus(KafkaCruiseControl, boolean, boolean)} is not {@link AnomalyDetectionStatus#READY}.
   *   <li>See {@link AnomalyDetectionStatus} for details.</li>
   * </ul>
   *
   * @param skipModelGenerationCheck {@code true} to skip checking whether the cluster model generation has changed.
   * @return The {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the anomaly detector is ready.
   */
  private AnomalyDetectionStatus getDiskFailureDetectionStatus(boolean skipModelGenerationCheck) {
    ModelGeneration currentClusterModelGeneration = _kafkaCruiseControl.loadMonitor().clusterModelGeneration();
    if (!skipModelGenerationCheck && currentClusterModelGeneration.equals(_lastCheckedModelGeneration)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping disk failure detection because the model generation hasn't changed. Current model generation {}",
                  _kafkaCruiseControl.loadMonitor().clusterModelGeneration());
      }
      return AnomalyDetectionStatus.SKIP_MODEL_GENERATION_NOT_CHANGED;
    }
    _lastCheckedModelGeneration = currentClusterModelGeneration;

    Set<Integer> deadBrokers = _kafkaCruiseControl.loadMonitor().deadBrokersWithReplicas(MAX_METADATA_WAIT_MS);
    if (!deadBrokers.isEmpty()) {
      LOG.debug("Skipping disk failure detection because there are dead broker in the cluster, dead broker: {}", deadBrokers);
      return AnomalyDetectionStatus.SKIP_HAS_DEAD_BROKERS;
    }

    return getAnomalyDetectionStatus(_kafkaCruiseControl, false, true);
  }

  @Override
  public void run() {
    detectDiskFailures(false);
  }

  /**
   * Detect disk failures.
   *
   * @param skipModelGenerationCheck {@code true} to detect disk failures even if the cluster model generation has not changed
   * since the last detection (e.g. to check pending disk failures once their self-healing threshold has passed),
   * {@code false} otherwise.
   */
  synchronized void detectDiskFailures(boolean skipModelGenerationCheck) {
    try {
      if (getDiskFailureDetectionStatus(skipModelGenerationCheck) != AnomalyDetectionStatus.READY) {
        return;
      }
      Map<Integer, Set<String>> failedLogdirsByBroker = new HashMap<>();
      Set<Integer> brokersWithUnknownLogdirs = new HashSet<>();
      Set<Integer> aliveBrokers = _kafkaCruiseControl.kafkaCluster().nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
      _adminClient.describeLogDirs(aliveBrokers).descriptions().forEach((broker, future) -> {
        try {
          future.get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
            if (info.error() != null) {
              failedLogdirsByBroker.computeIfAbsent(broker, b -> new HashSet<>()).add(logdir);
            }
          });
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
          LOG.warn("Retrieving logdir information for broker {} encountered exception {}.", broker, e);
          brokersWithUnknownLogdirs.add(broker);
        }
      });
      updateAndReportDiskFailures(failedLogdirsByBroker, brokersWithUnknownLogdirs);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      LOG.debug("Disk failure detection finished.");
    }
  }

  /**
   * Update the failed disks with the given failed logdirs, and report them. Kafka metadata tells which brokers have offline
   * replicas, but not on which of their disks. Hence, the failed disks of a broker are:
   * <ul>
   *   <li>Reported as fixable if the broker has offline replicas of partitions with a leader, which can be moved.</li>
   *   <li>Reported as unfixable if the broker only has offline replicas of partitions without a leader, which cannot be moved.</li>
   *   <li>Not reported if the broker has no offline replicas. Such disks are empty (e.g. their replicas have already been moved
   *   off them), but Kafka reports them as failed until they are removed from the log.dirs of the broker. Fixing them would only
   *   fail to find offline replicas to move.</li>
   * </ul>
   * A failed disk keeps the time it was first detected for as long as it is reported in the same way. Hence, the failure time of
   * a disk counts from when it was first detected on a broker with offline replicas, rather than from an earlier time when it was
   * empty.
   *
   * @param failedLogdirsByBroker Failed logdirs, grouped by broker.
   * @param brokersWithUnknownLogdirs Brokers whose logdirs could not be retrieved. Their failed disks are kept, but not reported.
   */
  private void updateAndReportDiskFailures(Map<Integer, Set<String>> failedLogdirsByBroker, Set<Integer> brokersWithUnknownLogdirs) {
    Map<Integer, Set<String>> fixableFailedLogdirs = new HashMap<>();
    Map<Integer, Set<String>> unfixableFailedLogdirs = new HashMap<>();
    Map<Integer, Set<String>> emptyFailedLogdirs = new HashMap<>();
    if (!failedLogdirsByBroker.isEmpty()) {
      Cluster cluster = _kafkaCruiseControl.loadMonitor().refreshClusterAndGeneration().cluster();
      Set<Integer> brokersWithMovableOfflineReplicas = brokersWithOfflineReplicas(cluster);
      Set<Integer> brokersWithOfflineReplicasOfLeaderlessPartitions = brokersWithOfflineReplicasOfLeaderlessPartitions(cluster);
      failedLogdirsByBroker.forEach((broker, logdirs) -> {
        if (brokersWithMovableOfflineReplicas.contains(broker)) {
          fixableFailedLogdirs.put(broker, logdirs);
        } else if (brokersWithOfflineReplicasOfLeaderlessPartitions.contains(broker)) {
          unfixableFailedLogdirs.put(broker, logdirs);
        } else {
          emptyFailedLogdirs.put(broker, logdirs);
        }
      });
    }
    long nowMs = _kafkaCruiseControl.timeMs();
    if (updateFailedDisks(_failedDisks, fixableFailedLogdirs, brokersWithUnknownLogdirs, nowMs)) {
      // Persist the updated failed disks.
      persistFailedDisks();
    }
    updateFailedDisks(_unfixableFailedDisks, unfixableFailedLogdirs, brokersWithUnknownLogdirs, nowMs);
    warnAboutNewEmptyFailedDisks(emptyFailedLogdirs, brokersWithUnknownLogdirs);
    reportDiskFailures(failedDisksOf(_failedDisks, fixableFailedLogdirs.keySet()), true, nowMs);
    reportDiskFailures(failedDisksOf(_unfixableFailedDisks, unfixableFailedLogdirs.keySet()), false, nowMs);
  }

  /**
   * Update the given failed disks with the currently failed logdirs: keep the time that the disks which are still failed were
   * first detected, add the disks that have failed since, and remove the disks that are no longer failed. Disks of brokers whose
   * logdirs could not be retrieved are kept as is.
   *
   * @param failedDisks Failed disks and the time they were first detected, grouped by broker.
   * @param currentFailedLogdirs Currently failed logdirs, grouped by broker.
   * @param brokersWithUnknownLogdirs Brokers whose logdirs could not be retrieved.
   * @param nowMs The current time in milliseconds.
   * @return {@code true} if the given failed disks have been updated, {@code false} otherwise.
   */
  private static boolean updateFailedDisks(Map<Integer, Map<String, Long>> failedDisks,
                                           Map<Integer, Set<String>> currentFailedLogdirs,
                                           Set<Integer> brokersWithUnknownLogdirs,
                                           long nowMs) {
    boolean updated = failedDisks.keySet().removeIf(broker -> !currentFailedLogdirs.containsKey(broker)
                                                              && !brokersWithUnknownLogdirs.contains(broker));
    for (Map.Entry<Integer, Set<String>> entry : currentFailedLogdirs.entrySet()) {
      Map<String, Long> failedDisksOfBroker = failedDisks.computeIfAbsent(entry.getKey(), broker -> new HashMap<>());
      updated |= failedDisksOfBroker.keySet().retainAll(entry.getValue());
      for (String logdir : entry.getValue()) {
        updated |= failedDisksOfBroker.putIfAbsent(logdir, nowMs) == null;
      }
    }
    return updated;
  }

  /**
   * @param failedDisks Failed disks and the time they were first detected, grouped by broker.
   * @param brokers Brokers whose failed disks to retrieve.
   * @return A copy of the failed disks of the given brokers.
   */
  private static Map<Integer, Map<String, Long>> failedDisksOf(Map<Integer, Map<String, Long>> failedDisks, Set<Integer> brokers) {
    Map<Integer, Map<String, Long>> failedDisksOfBrokers = new HashMap<>();
    brokers.forEach(broker -> failedDisksOfBrokers.put(broker, Map.copyOf(failedDisks.get(broker))));
    return failedDisksOfBrokers;
  }

  private void reportDiskFailures(Map<Integer, Map<String, Long>> failedDisksByBroker, boolean fixable, long nowMs) {
    if (failedDisksByBroker.isEmpty()) {
      return;
    }
    Map<String, Object> parameterConfigOverrides = new HashMap<>();
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    parameterConfigOverrides.put(FAILED_DISKS_OBJECT_CONFIG, failedDisksByBroker);
    parameterConfigOverrides.put(DISK_FAILURES_FIXABLE_CONFIG, fixable);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, nowMs);
    DiskFailures diskFailures = _config.getConfiguredInstance(DISK_FAILURES_CLASS_CONFIG,
                                                              DiskFailures.class,
                                                              parameterConfigOverrides);
    _anomalies.add(diskFailures);
  }

  /**
   * Warn once about each failed disk that is not reported because its broker has no offline replicas.
   *
   * @param emptyFailedLogdirs Failed logdirs of brokers without offline replicas, grouped by broker.
   * @param brokersWithUnknownLogdirs Brokers whose logdirs could not be retrieved.
   */
  private void warnAboutNewEmptyFailedDisks(Map<Integer, Set<String>> emptyFailedLogdirs, Set<Integer> brokersWithUnknownLogdirs) {
    Map<Integer, Set<String>> newEmptyFailedLogdirs = new HashMap<>();
    emptyFailedLogdirs.forEach((broker, logdirs) -> {
      Set<String> alreadyWarned = _emptyFailedDisks.getOrDefault(broker, Collections.emptySet());
      Set<String> newLogdirs = logdirs.stream().filter(logdir -> !alreadyWarned.contains(logdir)).collect(Collectors.toSet());
      if (!newLogdirs.isEmpty()) {
        newEmptyFailedLogdirs.put(broker, newLogdirs);
      }
    });
    if (!newEmptyFailedLogdirs.isEmpty()) {
      LOG.warn("Skipping self-healing for failed disks {} (by broker), because Kafka metadata shows no offline replicas on "
               + "their brokers to move. If these disks are empty, remove them from the log.dirs of their brokers.",
               newEmptyFailedLogdirs);
    }
    _emptyFailedDisks.keySet().retainAll(brokersWithUnknownLogdirs);
    emptyFailedLogdirs.forEach((broker, logdirs) -> _emptyFailedDisks.put(broker, new HashSet<>(logdirs)));
  }

  /**
   * Package private for unit test.
   * @return A copy of the fixable failed disks and the time they were first detected, grouped by broker.
   */
  synchronized Map<Integer, Map<String, Long>> failedDisks() {
    return failedDisksOf(_failedDisks, _failedDisks.keySet());
  }

  private void persistFailedDisks() {
    try {
      Files.writeString(_failedDisksFile.toPath(), failedDisksString(), StandardCharsets.UTF_8);

      Set<PosixFilePermission> perms = new HashSet<>();
      perms.add(PosixFilePermission.OWNER_READ);
      perms.add(PosixFilePermission.OWNER_WRITE);
      try {
        Files.setPosixFilePermissions(_failedDisksFile.toPath(), perms);
      } catch (IOException e) {
        LOG.warn("Update file permissions of failed disks file ({}) was unsuccessful:\n{}", _failedDisksFile.toPath(), e.toString());
      }
    } catch (IOException e) {
      // let it go.
      LOG.error("Failed to persist the failed disks.", e);
    }
  }

  /**
   * Package private for unit test.
   * @return Persisted failed disks as a {@code String}, or an empty {@code String} if no failed disks have been persisted in
   * the file, or {@code null} if failed to load the failed disks from the existing {@link #_failedDisksFile}.
   */
  String loadPersistedFailedDisks() {
    String failedDisksString = null;
    try {
      failedDisksString = Files.readString(_failedDisksFile.toPath(), StandardCharsets.UTF_8);
    } catch (FileNotFoundException | NoSuchFileException fnfe) {
      // This means no failed disks have ever been persisted in the file.
      failedDisksString = "";
    } catch (IOException ioe) {
      // let it go.
      LOG.error("Failed to load the failed disks.", ioe);
    }
    return failedDisksString;
  }

  /**
   * @return The fixable failed disks as comma-separated {@code brokerId:logdir=firstDetectionTimeMs} entries.
   */
  private String failedDisksString() {
    StringJoiner failedDisks = new StringJoiner(",");
    new TreeMap<>(_failedDisks).forEach((broker, failedDisksOfBroker) -> new TreeMap<>(failedDisksOfBroker).forEach(
        (logdir, firstDetectionTimeMs) -> failedDisks.add(String.format("%d:%s=%d", broker, logdir, firstDetectionTimeMs))));
    return failedDisks.toString();
  }

  /**
   * Parse the persisted failed disks (see {@link #failedDisksString()}). A logdir cannot contain a comma, because log.dirs is a
   * comma-separated list, but it may contain ':' or '='. Hence, the broker id ends at the first ':', and the time starts after the
   * last '='. If the persisted failed disks cannot be parsed, they are ignored: the self-healing of the disks that are still failed
   * then waits again from the time they are next detected, but never starts earlier than it should.
   *
   * @param failedDisksString Persisted failed disks.
   */
  private void parsePersistedFailedDisks(String failedDisksString) {
    if (failedDisksString == null || failedDisksString.isBlank()) {
      return;
    }
    try {
      for (String entry : failedDisksString.trim().split(",")) {
        int brokerIdEnd = entry.indexOf(':');
        int timeStart = entry.lastIndexOf('=');
        if (brokerIdEnd < 1 || timeStart <= brokerIdEnd + 1) {
          throw new IllegalArgumentException("Invalid failed disk entry: " + entry);
        }
        _failedDisks.computeIfAbsent(Integer.parseInt(entry.substring(0, brokerIdEnd)), broker -> new HashMap<>())
                    .put(entry.substring(brokerIdEnd + 1, timeStart), Long.parseLong(entry.substring(timeStart + 1)));
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Ignoring the persisted failed disks in {}, which cannot be parsed: {}", _failedDisksFile.toPath(), failedDisksString, e);
      _failedDisks.clear();
    }
  }
}
