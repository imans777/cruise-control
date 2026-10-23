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

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.FixOfflineReplicasRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.response.OptimizationResult;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.cruisecontrol.CruiseControlUtils.utcDateFor;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig.SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyUtils.extractKafkaCruiseControlObjectFromConfig;
import static com.linkedin.kafka.cruisecontrol.detector.DiskFailureDetector.DISK_FAILURES_FIXABLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.DiskFailureDetector.FAILED_DISKS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.DISK_FAILURE;

/**
 * The disk failures that have been detected.
 */
public class DiskFailures extends KafkaAnomaly {
  protected Map<Integer, Map<String, Long>> _failedDisksByBroker;
  protected FixOfflineReplicasRunnable _fixOfflineReplicasRunnable;
  protected boolean _fixable;

  public DiskFailures() {
  }

  /**
   * @return The failed disks and their failure time in millisecond grouped by broker.
   */
  public Map<Integer, Map<String, Long>> failedDisks() {
    return Collections.unmodifiableMap(_failedDisksByBroker);
  }

  /**
   * Whether detected disk failures are fixable or not.
   * Disk failures are unfixable if the offline replicas on their brokers all belong to partitions without a leader, which
   * cannot be moved and need human intervention.
   *
   * @return {@code true} if detected disk failures are fixable.
   */
  public boolean fixable() {
    return _fixable;
  }

  @Override
  public boolean fix() throws KafkaCruiseControlException {
    if (!_fixable) {
      return false;
    }
    // Fix the cluster by moving replicas off the dead disks.
    _optimizationResult = new OptimizationResult(_fixOfflineReplicasRunnable.computeResult(), null);
    boolean hasProposalsToFix = hasProposalsToFix();
    // Ensure that only the relevant response is cached to avoid memory pressure.
    _optimizationResult.discardIrrelevantAndCacheJsonAndPlaintext();
    return hasProposalsToFix;
  }

  @Override
  public AnomalyType anomalyType() {
    return DISK_FAILURE;
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("Self healing for %s: %s", DISK_FAILURE, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append(_fixable ? "" : "Unfixable ").append("{\n");
    _failedDisksByBroker.forEach((brokerId, failures) -> failures.forEach((logdir, eventTime) -> {
      sb.append(String.format("\tDisk %s on broker %d failed at %s%n ", logdir, brokerId, utcDateFor(eventTime)));
    }));
    sb.append("}");
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControl kafkaCruiseControl = extractKafkaCruiseControlObjectFromConfig(configs, DISK_FAILURE);
    _failedDisksByBroker = (Map<Integer, Map<String, Long>>) configs.get(FAILED_DISKS_OBJECT_CONFIG);
    if (_failedDisksByBroker == null || _failedDisksByBroker.isEmpty()) {
      throw new IllegalArgumentException("Unable to create disk failure anomaly with no failed disk specified.");
    }
    // Disk failures are fixable unless specified otherwise.
    Boolean fixable = (Boolean) configs.get(DISK_FAILURES_FIXABLE_CONFIG);
    _fixable = fixable == null || fixable;
    _optimizationResult = null;
    KafkaCruiseControlConfig config = kafkaCruiseControl.config();
    boolean allowCapacityEstimation = config.getBoolean(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    boolean excludeRecentlyDemotedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    boolean excludeRecentlyRemovedBrokers = config.getBoolean(SELF_HEALING_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _fixOfflineReplicasRunnable = new FixOfflineReplicasRunnable(kafkaCruiseControl,
                                                                 getSelfHealingGoalNames(config),
                                                                 allowCapacityEstimation,
                                                                 excludeRecentlyDemotedBrokers,
                                                                 excludeRecentlyRemovedBrokers,
                                                                 _anomalyId.toString(),
                                                                 reasonSupplier(),
                                                                 stopOngoingExecution());
  }
}
