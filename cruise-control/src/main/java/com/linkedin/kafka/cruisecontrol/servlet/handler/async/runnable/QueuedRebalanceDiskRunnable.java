/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;

/**
 * The disk rebalance that a rebalance request with {@code queue_rebalance_disk=true} queues after its own execution. It runs once
 * that execution has completed, while the executor is still reserved for the same user task:
 * <ol>
 *   <li>It waits until the load monitor meets the model completeness requirements of a disk rebalance (partition metrics are not
 *   sampled during an execution), for at most two partition metric windows.</li>
 *   <li>It generates the disk rebalance proposals from a fresh cluster model, exactly as a request with {@code rebalance_disk=true}
 *   and the same parameters would: the configured intra-broker goals, and the request's excluded and included topics, destination
 *   brokers, intra-broker concurrency, log dir throttle and broker-by-broker execution.</li>
 *   <li>It executes them with the same UUID, or finishes the user task if there is nothing to move.</li>
 * </ol>
 * A stop request, a failure, or a shutdown before the execution starts finishes the user task with an error. Its parameters are persisted
 * with the rebalance (see {@link #parameters()}), so that a Cruise Control restart that resumes the rebalance queues it again, or starts
 * it if the rebalance had completed (PR #15).
 */
public class QueuedRebalanceDiskRunnable implements Runnable {
  public static final String TYPE = "REBALANCE_DISK";
  private static final Logger LOG = LoggerFactory.getLogger(QueuedRebalanceDiskRunnable.class);
  private static final long LOAD_MONITOR_CHECK_INTERVAL_MS = 5000L;
  private static final String MIN_REQUIRED_NUM_WINDOWS = "minRequiredNumWindows";
  private static final String MIN_MONITORED_PARTITIONS_PERCENTAGE = "minMonitoredPartitionsPercentage";
  private static final String INCLUDE_ALL_TOPICS = "includeAllTopics";
  private static final String ALLOW_CAPACITY_ESTIMATION = "allowCapacityEstimation";
  private static final String EXCLUDED_TOPICS = "excludedTopics";
  private static final String INCLUDED_TOPICS = "includedTopics";
  private static final String EXCLUDE_RECENTLY_DEMOTED_BROKERS = "excludeRecentlyDemotedBrokers";
  private static final String EXCLUDE_RECENTLY_REMOVED_BROKERS = "excludeRecentlyRemovedBrokers";
  private static final String DESTINATION_BROKER_IDS = "destinationBrokerIds";
  private static final String FAST_MODE = "fastMode";
  private static final String CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS = "concurrentIntraBrokerPartitionMovements";
  private static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS = "executionProgressCheckIntervalMs";
  private static final String REPLICATION_THROTTLE = "replicationThrottle";
  private static final String LOG_DIR_THROTTLE = "logDirThrottle";
  private static final String REBALANCE_DISK_BROKER_BY_BROKER = "rebalanceDiskBrokerByBroker";
  private static final String TRIGGERED_BY_USER_REQUEST = "triggeredByUserRequest";
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final String _uuid;
  private final ModelCompletenessRequirements _modelCompletenessRequirements;
  private final boolean _allowCapacityEstimation;
  private final Pattern _excludedTopics;
  private final Pattern _includedTopics;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final Set<Integer> _destinationBrokerIds;
  private final boolean _fastMode;
  private final Integer _concurrentIntraBrokerPartitionMovements;
  private final Long _executionProgressCheckIntervalMs;
  private final Long _replicationThrottle;
  private final Long _logDirThrottle;
  private final boolean _isRebalanceDiskBrokerByBroker;
  private final boolean _isTriggeredByUserRequest;

  /**
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param rebalance The rebalance that queues this disk rebalance; this one inherits its applicable parameters.
   */
  QueuedRebalanceDiskRunnable(KafkaCruiseControl kafkaCruiseControl, RebalanceRunnable rebalance) {
    this(kafkaCruiseControl, rebalance._uuid, rebalance._modelCompletenessRequirements, rebalance._allowCapacityEstimation,
         rebalance._excludedTopics, rebalance._includedTopics, rebalance._excludeRecentlyDemotedBrokers,
         rebalance._excludeRecentlyRemovedBrokers, rebalance._destinationBrokerIds, rebalance._fastMode,
         rebalance._concurrentIntraBrokerPartitionMovements, rebalance._executionProgressCheckIntervalMs, rebalance._replicationThrottle,
         rebalance._logDirThrottle, rebalance._isRebalanceDiskBrokerByBroker, rebalance._isTriggeredByUserRequest);
  }

  @SuppressWarnings("ParameterNumber")
  private QueuedRebalanceDiskRunnable(KafkaCruiseControl kafkaCruiseControl,
                                      String uuid,
                                      ModelCompletenessRequirements modelCompletenessRequirements,
                                      boolean allowCapacityEstimation,
                                      Pattern excludedTopics,
                                      Pattern includedTopics,
                                      boolean excludeRecentlyDemotedBrokers,
                                      boolean excludeRecentlyRemovedBrokers,
                                      Set<Integer> destinationBrokerIds,
                                      boolean fastMode,
                                      Integer concurrentIntraBrokerPartitionMovements,
                                      Long executionProgressCheckIntervalMs,
                                      Long replicationThrottle,
                                      Long logDirThrottle,
                                      boolean isRebalanceDiskBrokerByBroker,
                                      boolean isTriggeredByUserRequest) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _uuid = uuid;
    _modelCompletenessRequirements = modelCompletenessRequirements;
    _allowCapacityEstimation = allowCapacityEstimation;
    _excludedTopics = excludedTopics;
    _includedTopics = includedTopics;
    _excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    _excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
    _destinationBrokerIds = destinationBrokerIds;
    _fastMode = fastMode;
    _concurrentIntraBrokerPartitionMovements = concurrentIntraBrokerPartitionMovements;
    _executionProgressCheckIntervalMs = executionProgressCheckIntervalMs;
    _replicationThrottle = replicationThrottle;
    _logDirThrottle = logDirThrottle;
    _isRebalanceDiskBrokerByBroker = isRebalanceDiskBrokerByBroker;
    _isTriggeredByUserRequest = isTriggeredByUserRequest;
  }

  /**
   * @return The parameters to rebuild this disk rebalance from after a Cruise Control restart (see {@link #fromParameters}).
   */
  public Map<String, String> parameters() {
    Map<String, String> parameters = new TreeMap<>();
    if (_modelCompletenessRequirements != null) {
      parameters.put(MIN_REQUIRED_NUM_WINDOWS, String.valueOf(_modelCompletenessRequirements.minRequiredNumWindows()));
      parameters.put(MIN_MONITORED_PARTITIONS_PERCENTAGE, String.valueOf(_modelCompletenessRequirements.minMonitoredPartitionsPercentage()));
      parameters.put(INCLUDE_ALL_TOPICS, String.valueOf(_modelCompletenessRequirements.includeAllTopics()));
    }
    parameters.put(ALLOW_CAPACITY_ESTIMATION, String.valueOf(_allowCapacityEstimation));
    putIfNotNull(parameters, EXCLUDED_TOPICS, _excludedTopics == null ? null : _excludedTopics.pattern());
    putIfNotNull(parameters, INCLUDED_TOPICS, _includedTopics == null ? null : _includedTopics.pattern());
    parameters.put(EXCLUDE_RECENTLY_DEMOTED_BROKERS, String.valueOf(_excludeRecentlyDemotedBrokers));
    parameters.put(EXCLUDE_RECENTLY_REMOVED_BROKERS, String.valueOf(_excludeRecentlyRemovedBrokers));
    putIfNotNull(parameters, DESTINATION_BROKER_IDS, _destinationBrokerIds == null ? null
        : _destinationBrokerIds.stream().sorted().map(String::valueOf).collect(Collectors.joining(",")));
    parameters.put(FAST_MODE, String.valueOf(_fastMode));
    putIfNotNull(parameters, CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS, _concurrentIntraBrokerPartitionMovements);
    putIfNotNull(parameters, EXECUTION_PROGRESS_CHECK_INTERVAL_MS, _executionProgressCheckIntervalMs);
    putIfNotNull(parameters, REPLICATION_THROTTLE, _replicationThrottle);
    putIfNotNull(parameters, LOG_DIR_THROTTLE, _logDirThrottle);
    parameters.put(REBALANCE_DISK_BROKER_BY_BROKER, String.valueOf(_isRebalanceDiskBrokerByBroker));
    parameters.put(TRIGGERED_BY_USER_REQUEST, String.valueOf(_isTriggeredByUserRequest));
    return parameters;
  }

  /**
   * Rebuild a disk rebalance queued before a Cruise Control restart from its persisted parameters.
   *
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param uuid UUID of the user task of the disk rebalance.
   * @param parameters The parameters of the disk rebalance (see {@link #parameters()}).
   * @return The disk rebalance.
   */
  public static QueuedRebalanceDiskRunnable fromParameters(KafkaCruiseControl kafkaCruiseControl, String uuid,
                                                           Map<String, String> parameters) {
    ModelCompletenessRequirements requirements = parameters.containsKey(MIN_REQUIRED_NUM_WINDOWS)
        ? new ModelCompletenessRequirements(Integer.parseInt(parameters.get(MIN_REQUIRED_NUM_WINDOWS)),
                                            Double.parseDouble(parameters.get(MIN_MONITORED_PARTITIONS_PERCENTAGE)),
                                            Boolean.parseBoolean(parameters.get(INCLUDE_ALL_TOPICS)))
        : null;
    String destinationBrokerIds = parameters.get(DESTINATION_BROKER_IDS);
    return new QueuedRebalanceDiskRunnable(
        kafkaCruiseControl, uuid, requirements, Boolean.parseBoolean(parameters.get(ALLOW_CAPACITY_ESTIMATION)),
        parameters.containsKey(EXCLUDED_TOPICS) ? Pattern.compile(parameters.get(EXCLUDED_TOPICS)) : null,
        parameters.containsKey(INCLUDED_TOPICS) ? Pattern.compile(parameters.get(INCLUDED_TOPICS)) : null,
        Boolean.parseBoolean(parameters.get(EXCLUDE_RECENTLY_DEMOTED_BROKERS)),
        Boolean.parseBoolean(parameters.get(EXCLUDE_RECENTLY_REMOVED_BROKERS)),
        destinationBrokerIds == null ? null : destinationBrokerIds.isEmpty() ? Collections.emptySet()
            : Arrays.stream(destinationBrokerIds.split(",")).map(Integer::valueOf).collect(Collectors.toSet()),
        Boolean.parseBoolean(parameters.get(FAST_MODE)),
        parameters.containsKey(CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS)
            ? Integer.valueOf(parameters.get(CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS)) : null,
        parameters.containsKey(EXECUTION_PROGRESS_CHECK_INTERVAL_MS) ? Long.valueOf(parameters.get(EXECUTION_PROGRESS_CHECK_INTERVAL_MS)) : null,
        parameters.containsKey(REPLICATION_THROTTLE) ? Long.valueOf(parameters.get(REPLICATION_THROTTLE)) : null,
        parameters.containsKey(LOG_DIR_THROTTLE) ? Long.valueOf(parameters.get(LOG_DIR_THROTTLE)) : null,
        Boolean.parseBoolean(parameters.get(REBALANCE_DISK_BROKER_BY_BROKER)),
        Boolean.parseBoolean(parameters.get(TRIGGERED_BY_USER_REQUEST)));
  }

  private static void putIfNotNull(Map<String, String> parameters, String name, Object value) {
    if (value != null) {
      parameters.put(name, String.valueOf(value));
    }
  }

  @Override
  public void run() {
    try {
      if (!waitForLoadMonitor()) {
        return;
      }
      List<String> goals = ParameterUtils.rebalanceDiskModeGoals(_kafkaCruiseControl.config());
      ProposalsRunnable proposalsRunnable =
          new ProposalsRunnable(_kafkaCruiseControl, new OperationFuture("Queued disk rebalance"), goals, _modelCompletenessRequirements,
                                _allowCapacityEstimation, _excludedTopics, _includedTopics, _excludeRecentlyDemotedBrokers,
                                _excludeRecentlyRemovedBrokers,
                                true, _destinationBrokerIds, true, true, false, _fastMode);
      OptimizerResult result = proposalsRunnable.computeResult();
      LOG.info("User task {}: Queued disk rebalance proposals: {}", _uuid, result.getProposalSummary());
      if (result.goalProposals().isEmpty()) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, false, "the disks are already balanced");
        return;
      }
      if (_kafkaCruiseControl.isQueuedExecutionCancelled(_uuid)) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, true, "a stop was requested before it started");
        return;
      }
      _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), false, null, null,
                                           _concurrentIntraBrokerPartitionMovements, null, null, _executionProgressCheckIntervalMs,
                                           null, _replicationThrottle, _logDirThrottle, _isTriggeredByUserRequest, _uuid,
                                           RebalanceRunnable.SKIP_AUTO_REFRESHING_CONCURRENCY, _isRebalanceDiskBrokerByBroker);
    } catch (InterruptedException ie) {
      _kafkaCruiseControl.finishQueuedExecution(_uuid, true, "Cruise Control is shutting down");
    } catch (Exception e) {
      LOG.warn("User task {}: The queued disk rebalance failed.", _uuid, e);
      _kafkaCruiseControl.finishQueuedExecution(_uuid, true, String.format("it failed: %s", e.getMessage()));
    }
  }

  /**
   * Wait until the load monitor meets the model completeness requirements of a disk rebalance, for at most two partition metric
   * windows, or until a stop is requested.
   *
   * @return {@code true} if the requirements are met, {@code false} if the queued disk rebalance is finished instead.
   */
  private boolean waitForLoadMonitor() throws InterruptedException {
    ModelCompletenessRequirements requirements =
        _kafkaCruiseControl.modelCompletenessRequirements(
            goalsByPriority(ParameterUtils.rebalanceDiskModeGoals(_kafkaCruiseControl.config()), _kafkaCruiseControl.config()))
                           .weaker(_modelCompletenessRequirements);
    long maxWaitMs = 2 * _kafkaCruiseControl.config().getLong(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
    long deadlineMs = System.currentTimeMillis() + maxWaitMs;
    boolean logged = false;
    while (!_kafkaCruiseControl.loadMonitor().meetCompletenessRequirements(requirements)) {
      if (_kafkaCruiseControl.loadMonitor().taskRunnerState() == LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.LOADING) {
        // E.g. right after a restart (PR #15): the wait starts once the load monitor has loaded the samples persisted before it.
        deadlineMs = System.currentTimeMillis() + maxWaitMs;
      }
      if (_kafkaCruiseControl.isQueuedExecutionCancelled(_uuid)) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, true, "a stop was requested before it started");
        return false;
      }
      if (System.currentTimeMillis() >= deadlineMs) {
        _kafkaCruiseControl.finishQueuedExecution(_uuid, true, String.format(
            "the load monitor did not meet the model completeness requirements %s within %d ms", requirements, maxWaitMs));
        return false;
      }
      if (!logged) {
        LOG.info("User task {}: The queued disk rebalance waits for the load monitor to meet {} (at most {} ms).", _uuid, requirements,
                 maxWaitMs);
        logged = true;
      }
      Thread.sleep(LOAD_MONITOR_CHECK_INTERVAL_MS);
    }
    return true;
  }
}
