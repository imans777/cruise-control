/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.executor.ReplicationThrottleHelper.CLIENT_REQUEST_TIMEOUT_MS;

/**
 * Throttles intra-broker replica movements -- i.e. moving replicas between the logdirs of a broker (see KIP-113), by setting
 * the dynamic broker config {@value #LOG_DIR_THROTTLE_CONFIG} on the brokers with intra-broker replica movements, and restores
 * the original per-broker value of the config (if any) once the execution finishes.
 *
 * <p>The throttle only limits copying replicas to their destination logdirs on a broker. Hence, it has no effect on a broker
 * without ongoing intra-broker replica movements, and the throttles are restored once at the end of the execution rather than
 * as the movements of each broker complete. Requests to the cluster are batched across brokers.</p>
 *
 * <p>The throttle rate of an ongoing execution can be updated dynamically (see {@link #updateThrottleRate(long, Collection)}).
 * This class is thread safe.</p>
 */
class LogDirThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(LogDirThrottleHelper.class);
  static final String LOG_DIR_THROTTLE_CONFIG = "replica.alter.log.dirs.io.max.bytes.per.second";
  static final int MAX_RESOURCES_PER_ADMIN_REQUEST = 500;
  static final int RETRIES = 30;
  static final long RETRY_BACKOFF_SCALE_MS = 500L;
  static final int RETRY_BACKOFF_BASE = 2;
  static final int MAX_RETRY_SLEEP_MS = (int) TimeUnit.SECONDS.toMillis(10);

  private final AdminClient _adminClient;
  private final int _retries;
  // Guarded by this. The throttle rate (null if no throttling is applied), which may be updated during the execution.
  private Long _throttleRate;
  // Guarded by this. Brokers throttled by this helper, mapped to their original per-broker value of the config (null if they had none).
  private final Map<Integer, String> _originalThrottleByBroker;
  // Guarded by this. Brokers throttled by this helper, mapped to the per-broker value of the config this helper last applied.
  private final Map<Integer, String> _appliedThrottleByBroker;
  // Guarded by this. Whether the throttles have been restored -- i.e. this helper must not throttle any broker afterwards.
  private boolean _closed;

  /**
   * @param adminClient The adminClient to describe and alter broker configs.
   * @param throttleRate The throttle (bytes/second) to apply to intra-broker replica movements (if null, no throttling is applied).
   */
  LogDirThrottleHelper(AdminClient adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  // for testing
  LogDirThrottleHelper(AdminClient adminClient, Long throttleRate, int retries) {
    _adminClient = adminClient;
    _throttleRate = throttleRate;
    _retries = retries;
    _originalThrottleByBroker = new HashMap<>();
    _appliedThrottleByBroker = new HashMap<>();
    _closed = false;
  }

  /**
   * @return The brokers throttled by this helper, whose throttle has not been restored yet.
   */
  synchronized Set<Integer> throttledBrokers() {
    return Collections.unmodifiableSet(new TreeSet<>(_originalThrottleByBroker.keySet()));
  }

  /**
   * Set the throttle on the brokers of the given intra-broker replica movement tasks that have not been throttled yet. The configs
   * of all such brokers are described, altered, and verified with a single batch of requests.
   *
   * @param intraBrokerReplicaMovementTasks Intra-broker replica movement tasks to be executed.
   * @throws IllegalStateException If the throttle cannot be set.
   */
  synchronized void setThrottles(Collection<ExecutionTask> intraBrokerReplicaMovementTasks) {
    if (_throttleRate == null || _closed) {
      return;
    }
    Set<Integer> brokersToThrottle = new TreeSet<>();
    for (ExecutionTask task : intraBrokerReplicaMovementTasks) {
      if (!_originalThrottleByBroker.containsKey(task.brokerId())) {
        brokersToThrottle.add(task.brokerId());
      }
    }
    if (brokersToThrottle.isEmpty()) {
      return;
    }

    LOG.info("Setting log dir throttle of {} bytes/sec on brokers {}.", _throttleRate, brokersToThrottle);
    String throttleRate = String.valueOf(_throttleRate);
    try {
      Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsInChunks(brokerResources(brokersToThrottle));
      Map<ConfigResource, AlterConfigOp> opByBroker = new HashMap<>();
      for (int brokerId : brokersToThrottle) {
        ConfigResource cf = brokerResource(brokerId);
        ConfigEntry currentThrottle = configFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(LOG_DIR_THROTTLE_CONFIG);
        boolean hasPerBrokerThrottle = isPerBrokerValue(currentThrottle);
        // Track the broker before altering its config so that the throttle is restored even if altering fails midway.
        _originalThrottleByBroker.put(brokerId, hasPerBrokerThrottle ? currentThrottle.value() : null);
        _appliedThrottleByBroker.put(brokerId, throttleRate);
        if (!hasPerBrokerThrottle || !throttleRate.equals(currentThrottle.value())) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, throttleRate), AlterConfigOp.OpType.SET));
        }
      }
      List<ConfigResource> failedToAlter = alterAndVerifyConfigs(opByBroker);
      if (!failedToAlter.isEmpty()) {
        throw new IllegalStateException(String.format("Failed to set %s to %d on %s.", LOG_DIR_THROTTLE_CONFIG, _throttleRate, failedToAlter));
      }
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException(String.format("Failed to set %s on brokers %s.", LOG_DIR_THROTTLE_CONFIG, brokersToThrottle), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(String.format("Interrupted while setting %s on brokers %s.", LOG_DIR_THROTTLE_CONFIG,
                                                    brokersToThrottle), e);
    }
  }

  /**
   * Update the throttle rate, and apply the new rate to (1) all brokers already throttled by this helper using a single batch
   * of requests, and (2) the brokers of the given in-progress intra-broker replica movement tasks that have not been throttled
   * yet -- e.g. if the execution started without a log dir throttle. Brokers of upcoming intra-broker replica movement tasks are
   * throttled with the new rate as their tasks start.
   *
   * @param throttleRate The new throttle (bytes/second) to apply to intra-broker replica movements.
   * @param inProgressIntraBrokerTasks In-progress intra-broker replica movement tasks.
   * @return {@code true} if the throttle rate has been updated, {@code false} if the throttles have already been restored.
   * @throws IllegalStateException If the new throttle cannot be applied.
   */
  synchronized boolean updateThrottleRate(long throttleRate, Collection<ExecutionTask> inProgressIntraBrokerTasks) {
    if (_closed) {
      LOG.info("Skip updating log dir throttle to {} bytes/sec, since the throttles have already been restored.", throttleRate);
      return false;
    }
    _throttleRate = throttleRate;
    String newThrottleRate = String.valueOf(throttleRate);
    Set<Integer> throttledBrokers = new TreeSet<>(_originalThrottleByBroker.keySet());
    if (!throttledBrokers.isEmpty()) {
      LOG.info("Updating log dir throttle to {} bytes/sec on brokers {}.", throttleRate, throttledBrokers);
      Map<ConfigResource, AlterConfigOp> opByBroker = new HashMap<>();
      for (int brokerId : throttledBrokers) {
        opByBroker.put(brokerResource(brokerId),
                       new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, newThrottleRate), AlterConfigOp.OpType.SET));
      }
      try {
        List<ConfigResource> failedToAlter = alterAndVerifyConfigs(opByBroker);
        if (!failedToAlter.isEmpty()) {
          throw new IllegalStateException(String.format("Failed to update %s to %d on %s.", LOG_DIR_THROTTLE_CONFIG, throttleRate,
                                                        failedToAlter));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(String.format("Interrupted while updating %s on brokers %s.", LOG_DIR_THROTTLE_CONFIG,
                                                      throttledBrokers), e);
      }
    }
    setThrottles(inProgressIntraBrokerTasks);
    return true;
  }

  /**
   * Restore the original per-broker value of the config (or remove the per-broker value if there was none) on all brokers
   * throttled by this helper, using a single batch of requests. If the config of a broker has been changed since this helper last
   * applied the throttle to it, the config of that broker is left as is. This helper does not throttle any broker afterwards.
   * This method is best-effort and does not throw.
   */
  synchronized void clearAllThrottles() {
    _closed = true;
    if (_originalThrottleByBroker.isEmpty()) {
      return;
    }
    Set<Integer> throttledBrokers = new TreeSet<>(_originalThrottleByBroker.keySet());
    try {
      Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsInChunks(brokerResources(throttledBrokers));
      Map<ConfigResource, AlterConfigOp> opByBroker = new HashMap<>();
      for (int brokerId : throttledBrokers) {
        ConfigResource cf = brokerResource(brokerId);
        ConfigEntry currentThrottle;
        try {
          currentThrottle = configFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS).get(LOG_DIR_THROTTLE_CONFIG);
        } catch (ExecutionException | TimeoutException e) {
          LOG.warn("Failed to describe the configs of broker {} to restore its {}.", brokerId, LOG_DIR_THROTTLE_CONFIG, e);
          continue;
        }
        String appliedThrottle = _appliedThrottleByBroker.get(brokerId);
        if (!isPerBrokerValue(currentThrottle) || !currentThrottle.value().equals(appliedThrottle)) {
          LOG.info("Skip restoring {} on broker {}, since it has been changed to {} during the execution.", LOG_DIR_THROTTLE_CONFIG,
                   brokerId, currentThrottle);
          continue;
        }
        String originalThrottle = _originalThrottleByBroker.get(brokerId);
        if (originalThrottle == null) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, null), AlterConfigOp.OpType.DELETE));
        } else if (!originalThrottle.equals(currentThrottle.value())) {
          opByBroker.put(cf, new AlterConfigOp(new ConfigEntry(LOG_DIR_THROTTLE_CONFIG, originalThrottle), AlterConfigOp.OpType.SET));
        }
      }
      LOG.info("Restoring {} on brokers {}.", LOG_DIR_THROTTLE_CONFIG, throttledBrokers);
      List<ConfigResource> failedToRestore = alterAndVerifyConfigs(opByBroker);
      if (!failedToRestore.isEmpty()) {
        LOG.warn("Failed to restore {} on {}. Please check the config of these brokers.", LOG_DIR_THROTTLE_CONFIG, failedToRestore);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while restoring {} on brokers {}.", LOG_DIR_THROTTLE_CONFIG, throttledBrokers, e);
    } catch (RuntimeException e) {
      LOG.warn("Failed to restore {} on brokers {}.", LOG_DIR_THROTTLE_CONFIG, throttledBrokers, e);
    } finally {
      _originalThrottleByBroker.clear();
      _appliedThrottleByBroker.clear();
    }
  }

  /**
   * Alter the given broker configs with a batch of requests, and wait until the cluster reflects the altered configs. Records
   * the value applied to each broker whose config is altered successfully.
   *
   * @param opByBroker The operation to apply to each broker config.
   * @return Broker configs that failed to be altered or that the cluster did not reflect within the time limit.
   */
  private List<ConfigResource> alterAndVerifyConfigs(Map<ConfigResource, AlterConfigOp> opByBroker) throws InterruptedException {
    List<ConfigResource> failed = new ArrayList<>();
    if (opByBroker.isEmpty()) {
      return failed;
    }
    Map<ConfigResource, KafkaFuture<Void>> alterFutures = new HashMap<>();
    List<ConfigResource> resources = new ArrayList<>(opByBroker.keySet());
    for (int i = 0; i < resources.size(); i += MAX_RESOURCES_PER_ADMIN_REQUEST) {
      Map<ConfigResource, Collection<AlterConfigOp>> chunk = new HashMap<>();
      resources.subList(i, Math.min(resources.size(), i + MAX_RESOURCES_PER_ADMIN_REQUEST))
               .forEach(cf -> chunk.put(cf, Collections.singletonList(opByBroker.get(cf))));
      alterFutures.putAll(_adminClient.incrementalAlterConfigs(chunk).values());
    }

    // Expected value of the config for each altered broker -- null for removing the per-broker value.
    Map<ConfigResource, String> pending = new HashMap<>();
    for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : alterFutures.entrySet()) {
      try {
        entry.getValue().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        String appliedValue = opByBroker.get(entry.getKey()).configEntry().value();
        _appliedThrottleByBroker.put(Integer.parseInt(entry.getKey().name()), appliedValue);
        pending.put(entry.getKey(), appliedValue);
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Failed to alter {} on {}.", LOG_DIR_THROTTLE_CONFIG, entry.getKey(), e);
        failed.add(entry.getKey());
      }
    }

    // Retry until the cluster reflects the altered configs.
    boolean verified = CruiseControlMetricsUtils.retry(() -> {
      Map<ConfigResource, KafkaFuture<Config>> configFutures = describeConfigsInChunks(new ArrayList<>(pending.keySet()));
      pending.entrySet().removeIf(entry -> isApplied(configFutures.get(entry.getKey()), entry.getValue()));
      return !pending.isEmpty();
    }, RETRY_BACKOFF_SCALE_MS, RETRY_BACKOFF_BASE, _retries, MAX_RETRY_SLEEP_MS);
    if (!verified) {
      LOG.warn("The cluster did not reflect the altered {} on {} within the time limit.", LOG_DIR_THROTTLE_CONFIG, pending.keySet());
      failed.addAll(pending.keySet());
    }
    return failed;
  }

  /**
   * @param configFuture Future of the described broker config.
   * @param expectedValue The expected per-broker value of the config, or null if the per-broker value is expected to be removed.
   * @return {@code true} if the described broker config reflects the expected per-broker value, {@code false} otherwise.
   */
  private static boolean isApplied(KafkaFuture<Config> configFuture, String expectedValue) {
    try {
      return isApplied(configFuture.get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS), expectedValue);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException | TimeoutException e) {
      LOG.debug("Failed to describe broker configs to verify {}, will retry.", LOG_DIR_THROTTLE_CONFIG, e);
      return false;
    }
  }

  static boolean isApplied(Config brokerConfig, String expectedValue) {
    ConfigEntry currentThrottle = brokerConfig.get(LOG_DIR_THROTTLE_CONFIG);
    if (expectedValue == null) {
      // Once the per-broker value is removed, the config resolves from a cluster-wide default, a static config, or Kafka default.
      return !isPerBrokerValue(currentThrottle);
    }
    return isPerBrokerValue(currentThrottle) && expectedValue.equals(currentThrottle.value());
  }

  private static boolean isPerBrokerValue(ConfigEntry configEntry) {
    return configEntry != null && configEntry.value() != null && configEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
  }

  private Map<ConfigResource, KafkaFuture<Config>> describeConfigsInChunks(List<ConfigResource> resources) {
    Map<ConfigResource, KafkaFuture<Config>> configFutures = new HashMap<>();
    for (int i = 0; i < resources.size(); i += MAX_RESOURCES_PER_ADMIN_REQUEST) {
      configFutures.putAll(_adminClient.describeConfigs(resources.subList(i, Math.min(resources.size(), i + MAX_RESOURCES_PER_ADMIN_REQUEST)))
                                       .values());
    }
    return configFutures;
  }

  private static List<ConfigResource> brokerResources(Collection<Integer> brokerIds) {
    List<ConfigResource> resources = new ArrayList<>(brokerIds.size());
    brokerIds.forEach(brokerId -> resources.add(brokerResource(brokerId)));
    return resources;
  }

  private static ConfigResource brokerResource(int brokerId) {
    return new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
  }
}
