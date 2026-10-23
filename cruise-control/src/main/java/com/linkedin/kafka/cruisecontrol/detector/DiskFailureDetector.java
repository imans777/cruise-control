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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
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
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getAnomalyDetectionStatus;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.brokersWithOfflineReplicas;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.brokersWithOfflineReplicasOfLeaderlessPartitions;

/**
 * This class detects disk failures.
 **/
public class DiskFailureDetector extends AbstractAnomalyDetector implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureDetector.class);
  public static final String FAILED_DISKS_OBJECT_CONFIG = "failed.disks.object";
  // Config to indicate whether detected disk failures are fixable or not.
  public static final String DISK_FAILURES_FIXABLE_CONFIG = "disk.failures.fixable.object";
  private final Admin _adminClient;
  private ModelGeneration _lastCheckedModelGeneration;
  private final KafkaCruiseControlConfig _config;
  // Failed disks that are not reported because their broker has no offline replicas, grouped by broker.
  private final Map<Integer, Set<String>> _emptyFailedDisks;

  public DiskFailureDetector(Queue<Anomaly> anomalies, KafkaCruiseControl kafkaCruiseControl) {
    super(anomalies, kafkaCruiseControl);
    _adminClient = kafkaCruiseControl.adminClient();
    _lastCheckedModelGeneration = new ModelGeneration(0, -1L);
    _config = _kafkaCruiseControl.config();
    _emptyFailedDisks = new HashMap<>();
  }

  /**
   * Retrieve the {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the disk failure detector
   * is ready to check for an anomaly.
   *
   * Skip disk failure detection if any of the following is satisfied:
   * <ul>
   *   <li>Cluster model generation has not changed since the last disk failure check.</li>
   *   <li>There are dead brokers in the cluster,
   *   {@link KafkaBrokerFailureDetector} should take care of the anomaly.</li>
   *   <li>{@link AnomalyDetectorUtils#getAnomalyDetectionStatus(KafkaCruiseControl, boolean, boolean)} is not {@link AnomalyDetectionStatus#READY}.
   *   <li>See {@link AnomalyDetectionStatus} for details.</li>
   * </ul>
   *
   * @return The {@link AnomalyDetectionStatus anomaly detection status}, indicating whether the anomaly detector is ready.
   */
  private AnomalyDetectionStatus getDiskFailureDetectionStatus() {
    ModelGeneration currentClusterModelGeneration = _kafkaCruiseControl.loadMonitor().clusterModelGeneration();
    if (currentClusterModelGeneration.equals(_lastCheckedModelGeneration)) {
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
    try {
      if (getDiskFailureDetectionStatus() != AnomalyDetectionStatus.READY) {
        return;
      }
      Map<Integer, Map<String, Long>> failedDisksByBroker = new HashMap<>();
      Set<Integer> aliveBrokers = _kafkaCruiseControl.kafkaCluster().nodes().stream().mapToInt(Node::id).boxed().collect(Collectors.toSet());
      _adminClient.describeLogDirs(aliveBrokers).descriptions().forEach((broker, future) -> {
        try {
          future.get(_config.getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), TimeUnit.MILLISECONDS).forEach((logdir, info) -> {
            if (info.error() != null) {
              failedDisksByBroker.putIfAbsent(broker, new HashMap<>());
              failedDisksByBroker.get(broker).put(logdir, _kafkaCruiseControl.timeMs());
            }
          });
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
          LOG.warn("Retrieving logdir information for broker {} encountered exception {}.", broker, e);
        }
      });
      reportDiskFailures(failedDisksByBroker);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    } finally {
      LOG.debug("Disk failure detection finished.");
    }
  }

  /**
   * Report the given failed disks. Kafka metadata tells which brokers have offline replicas, but not on which of their disks.
   * Hence, the failed disks of a broker are:
   * <ul>
   *   <li>Reported as fixable if the broker has offline replicas of partitions with a leader, which can be moved.</li>
   *   <li>Reported as unfixable if the broker only has offline replicas of partitions without a leader, which cannot be moved.</li>
   *   <li>Not reported if the broker has no offline replicas. Such disks are empty (e.g. their replicas have already been moved
   *   off them), but Kafka reports them as failed until they are removed from the log.dirs of the broker. Fixing them would only
   *   fail to find offline replicas to move.</li>
   * </ul>
   *
   * @param failedDisksByBroker Failed disks and their failure time in milliseconds, grouped by broker.
   */
  private void reportDiskFailures(Map<Integer, Map<String, Long>> failedDisksByBroker) {
    if (failedDisksByBroker.isEmpty()) {
      _emptyFailedDisks.clear();
      return;
    }
    Cluster cluster = _kafkaCruiseControl.loadMonitor().refreshClusterAndGeneration().cluster();
    Set<Integer> brokersWithMovableOfflineReplicas = brokersWithOfflineReplicas(cluster);
    Set<Integer> brokersWithOfflineReplicasOfLeaderlessPartitions = brokersWithOfflineReplicasOfLeaderlessPartitions(cluster);
    Map<Integer, Map<String, Long>> fixableFailedDisks = new HashMap<>();
    Map<Integer, Map<String, Long>> unfixableFailedDisks = new HashMap<>();
    Map<Integer, Set<String>> emptyFailedDisks = new HashMap<>();
    failedDisksByBroker.forEach((broker, failedDisks) -> {
      if (brokersWithMovableOfflineReplicas.contains(broker)) {
        fixableFailedDisks.put(broker, failedDisks);
      } else if (brokersWithOfflineReplicasOfLeaderlessPartitions.contains(broker)) {
        unfixableFailedDisks.put(broker, failedDisks);
      } else {
        emptyFailedDisks.put(broker, failedDisks.keySet());
      }
    });
    warnAboutNewEmptyFailedDisks(emptyFailedDisks);
    reportDiskFailures(fixableFailedDisks, true);
    reportDiskFailures(unfixableFailedDisks, false);
  }

  private void reportDiskFailures(Map<Integer, Map<String, Long>> failedDisksByBroker, boolean fixable) {
    if (failedDisksByBroker.isEmpty()) {
      return;
    }
    Map<String, Object> parameterConfigOverrides = new HashMap<>();
    parameterConfigOverrides.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, _kafkaCruiseControl);
    parameterConfigOverrides.put(FAILED_DISKS_OBJECT_CONFIG, failedDisksByBroker);
    parameterConfigOverrides.put(DISK_FAILURES_FIXABLE_CONFIG, fixable);
    parameterConfigOverrides.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, _kafkaCruiseControl.timeMs());
    DiskFailures diskFailures = _config.getConfiguredInstance(DISK_FAILURES_CLASS_CONFIG,
                                                              DiskFailures.class,
                                                              parameterConfigOverrides);
    _anomalies.add(diskFailures);
  }

  /**
   * Warn once about each failed disk that is not reported because its broker has no offline replicas.
   *
   * @param emptyFailedDisks Failed disks of brokers without offline replicas, grouped by broker.
   */
  private void warnAboutNewEmptyFailedDisks(Map<Integer, Set<String>> emptyFailedDisks) {
    Map<Integer, Set<String>> newEmptyFailedDisks = new HashMap<>();
    emptyFailedDisks.forEach((broker, logdirs) -> {
      Set<String> alreadyWarned = _emptyFailedDisks.getOrDefault(broker, Collections.emptySet());
      Set<String> newLogdirs = logdirs.stream().filter(logdir -> !alreadyWarned.contains(logdir)).collect(Collectors.toSet());
      if (!newLogdirs.isEmpty()) {
        newEmptyFailedDisks.put(broker, newLogdirs);
      }
    });
    if (!newEmptyFailedDisks.isEmpty()) {
      LOG.warn("Skipping self-healing for failed disks {} (by broker), because Kafka metadata shows no offline replicas on "
               + "their brokers to move. If these disks are empty, remove them from the log.dirs of their brokers.",
               newEmptyFailedDisks);
    }
    _emptyFailedDisks.clear();
    emptyFailedDisks.forEach((broker, logdirs) -> _emptyFailedDisks.put(broker, new HashSet<>(logdirs)));
  }
}
