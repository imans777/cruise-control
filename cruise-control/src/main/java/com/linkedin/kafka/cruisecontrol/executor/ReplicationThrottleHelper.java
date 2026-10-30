/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsUtils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * See https://kafka.apache.org/documentation/#rep-throttle
 *
 * <p>The throttle rate of an ongoing execution can be updated dynamically (see {@link #updateThrottleRate(long, Supplier)}).
 * This class is thread safe: its methods synchronize on this helper, so a caller can also hold the lock of this helper to make
 * a sequence of operations atomic with respect to a concurrent throttle rate update.</p>
 */
class ReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationThrottleHelper.class);
  static final String WILDCARD_ASTERISK = "*";
  static final String LEADER_REPLICATION_THROTTLED_RATE_CONFIG = "leader.replication.throttled.rate";
  static final String FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG = "follower.replication.throttled.rate";
  static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
  static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
  public static final long CLIENT_REQUEST_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
  static final int RETRIES = 30;
  // Backoff parameters for the config verification retry loop. The sleep before the n-th retry
  // (n >= 1) is scale * base^n (10s, 20s, 30s, 30s, ...), capped at MAX_RETRY_SLEEP_MS so that a
  // slow-to-verify config cannot park the executor thread for an unbounded exponential sleep.
  static final long RETRY_BACKOFF_SCALE_MS = TimeUnit.SECONDS.toMillis(5);
  static final int RETRY_BACKOFF_BASE = 2;
  static final int MAX_RETRY_SLEEP_MS = (int) TimeUnit.SECONDS.toMillis(30);
  // Sources of config values that deleting a dynamic broker or topic config does not remove, as they have lower precedence.
  // Seeing one of these during verification of a delete means the delete itself worked (but see configsEqual for a stale
  // DEFAULT_CONFIG read), and a delete is not even attempted for a value that comes from one of them.
  private static final Set<ConfigEntry.ConfigSource> LOWER_PRECEDENCE_CONFIG_SOURCES = Collections.unmodifiableSet(
      EnumSet.of(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                 ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG,
                 ConfigEntry.ConfigSource.DEFAULT_CONFIG));
  // Cap on the number of resources packed into a single AdminClient config request. Requests for
  // TOPIC resources are routed to a single broker, so an unbounded batch could produce an
  // oversized request on clusters with a very large number of topics.
  static final int MAX_RESOURCES_PER_ADMIN_REQUEST = 500;
  // The concurrency adjuster verifies its throttle rate updates while holding the lock of the helper, which the executor needs to
  // set and clear throttles: a small budget (~2 minutes with the backoff above) bounds how long an update can block them.
  static final int THROTTLE_RATE_UPDATE_RETRIES = 5;

  private final Admin _adminClient;
  // The throttle rate (null if no throttling is applied), which is used for brokers whose throttle rate has not been adjusted.
  // Written under the lock of this helper (it may be updated during the execution), volatile for the unsynchronized getters.
  private volatile Long _throttleRate;
  private final int _retries;
  private final Set<Integer> _deadBrokers;
  private final AppliedThrottles _appliedThrottles;
  // Leader and follower throttle rates of brokers that have been adjusted during the execution (e.g. by the concurrency adjuster).
  private final ConcurrentMap<Integer, Long> _leaderThrottleRateByBroker;
  private final ConcurrentMap<Integer, Long> _followerThrottleRateByBroker;
  // Brokers whose throttle rate has been set by this helper and not yet removed. Written under the lock of this helper, concurrent
  // for the unsynchronized read of the executor state (see #throttleRateByBroker()).
  private final Set<Integer> _throttledBrokers;

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate) {
    this(adminClient, throttleRate, RETRIES);
  }

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, RETRIES, deadBrokers);
  }

  // for testing
  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, int retries) {
    this(adminClient, throttleRate, retries, new HashSet<>());
  }

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, int retries, Set<Integer> deadBrokers) {
    this(adminClient, throttleRate, retries, deadBrokers, new AppliedThrottles());
  }

  ReplicationThrottleHelper(Admin adminClient, Long throttleRate, int retries, Set<Integer> deadBrokers,
                            AppliedThrottles appliedThrottles) {
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._retries = retries;
    this._deadBrokers = deadBrokers;
    this._appliedThrottles = appliedThrottles;
    this._leaderThrottleRateByBroker = new ConcurrentHashMap<>();
    this._followerThrottleRateByBroker = new ConcurrentHashMap<>();
    this._throttledBrokers = ConcurrentHashMap.newKeySet();
  }

  /**
   * Update the throttle rate, and apply the new rate to the in-progress inter-broker replica movements right away. Upcoming
   * inter-broker replica movements are throttled with the new rate as their throttles are set via {@link #setThrottles(List)}.
   *
   * <p>The in-progress replica movements are retrieved while holding the lock of this helper. Otherwise, the throttles of a
   * movement that completes concurrently might be removed by {@link #clearThrottles(List, List)} and then set again by this
   * update, which would leave them in place after the execution.</p>
   *
   * <p>The new rate applies to every broker: it replaces the rates adjusted per broker by
   * {@link #updateThrottleRates(Map, Map)}.</p>
   *
   * @param throttleRate The new replication throttle rate in bytes per second.
   * @param inProgressProposals Supplies the proposals of the in-progress inter-broker replica movements.
   */
  synchronized void updateThrottleRate(long throttleRate, Supplier<List<ExecutionProposal>> inProgressProposals)
  throws ExecutionException, InterruptedException, TimeoutException {
    _throttleRate = throttleRate;
    // The new rate replaces the rates that the concurrency adjuster set per broker, which would otherwise be written back by the
    // next batch of throttles (decision D2: the adjuster, while on, may then move the rate again).
    _leaderThrottleRateByBroker.clear();
    _followerThrottleRateByBroker.clear();
    setThrottles(inProgressProposals.get());
  }

  /**
   * Not synchronized: the executor state reads it while setting, clearing or updating the throttles may hold the lock of this
   * helper for as long as it takes to alter and verify the configs.
   *
   * @return The throttle rate used for brokers whose throttle rate has not been adjusted, or {@code null} if throttling is disabled.
   */
  Long throttleRate() {
    return _throttleRate;
  }

  /**
   * Remove the throttle of an ongoing execution: stop throttling the in-progress inter-broker replica movements and let the
   * upcoming ones run unthrottled. The in-progress proposals are retrieved while holding the lock of this helper, for the same
   * reason as in {@link #updateThrottleRate(long, Supplier)}.
   *
   * <p>This is what an {@code admin} request asking for a {@code null} throttle means. It is deliberately not the same as a
   * rate of 0, which Kafka reads as a quota of zero bytes per second -- i.e. a halt.</p>
   *
   * @param inProgressProposals Supplies the proposals of the in-progress inter-broker replica movements.
   * @return {@code true} if a throttle was removed, {@code false} if the execution was not throttled to begin with.
   */
  synchronized boolean removeThrottleRate(Supplier<List<ExecutionProposal>> inProgressProposals) throws InterruptedException {
    if (_throttleRate == null) {
      LOG.info("Skip removing the replication throttle, since the ongoing execution is not throttled.");
      return false;
    }
    // Clear the rate first, so that the upcoming task batches are not throttled even if the removal below fails midway.
    _throttleRate = null;
    // Removal-only moves (e.g. a replication factor decrease) are never throttled, as setThrottles skips them: their brokers'
    // rates are not this execution's to remove.
    List<ExecutionProposal> proposals = proposalsWithDataMovement(inProgressProposals.get());
    Set<Integer> participatingBrokers = getParticipatingBrokers(proposals);
    LOG.info("Removing the replication throttle of the ongoing execution from brokers {}.", participatingBrokers);
    // Through the batched removal, so that the record of applied throttles (and of throttled brokers) stays in line: what fails
    // to be removed stays recorded, and is removed later.
    removeThrottles(participatingBrokers, proposals);
    return true;
  }

  synchronized void setThrottles(List<ExecutionProposal> proposals)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (throttlingEnabled()) {
      List<ExecutionProposal> replicaMovementProposals = proposalsWithDataMovement(proposals);
      if (replicaMovementProposals.isEmpty()) {
        LOG.info("Skip setting a rebalance throttle, because none of the proposals adds a replica to a broker.");
        return;
      }
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals);
      LOG.info("Setting rebalance throttle of {} bytes/sec on {} brokers for {} topics ({} replica movements)",
          _throttleRate, participatingBrokers.size(), throttledReplicas.size(), replicaMovementProposals.size());
      // Record the throttles before setting them, so that the throttles are removed even if setting them fails halfway.
      _appliedThrottles._brokers.addAll(participatingBrokers);
      _appliedThrottles._proposals.addAll(replicaMovementProposals);

      // Batch set broker throttle rates. Reads are chunked like every other AdminClient call in
      // this class; each future is resolved eagerly and any failure is rethrown, preserving the
      // fail-fast semantics of the previous .all() call.
      if (!participatingBrokers.isEmpty()) {
        List<ConfigResource> brokerResources = participatingBrokers.stream()
            .map(id -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)))
            .collect(Collectors.toList());
        Map<ConfigResource, KafkaFuture<Config>> brokerFutures = describeConfigsInChunks(brokerResources);

        Map<ConfigResource, Collection<AlterConfigOp>> brokerOps = new HashMap<>();
        for (int brokerId : participatingBrokers) {
          ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
          Config config = brokerFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          List<AlterConfigOp> ops = throttledRateOps(brokerId, config);
          if (!ops.isEmpty()) {
            brokerOps.put(cf, ops);
          }
        }

        if (!brokerOps.isEmpty()) {
          LOG.info("Updating throttle rate on {} out of {} brokers", brokerOps.size(), participatingBrokers.size());
          batchAlterBrokerConfigs(brokerOps);
        }
        _throttledBrokers.addAll(participatingBrokers);
      }

      // Batch set topic throttled replicas.
      // Note: batching widens the read-modify-write window compared to the old per-topic approach.
      // This assumes Cruise Control is the sole writer of throttle configs during an operation.
      if (!throttledReplicas.isEmpty()) {
        Map<String, Config> topicConfigs = batchGetTopicConfigs(throttledReplicas.keySet());

        Map<ConfigResource, Collection<AlterConfigOp>> topicOps = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : throttledReplicas.entrySet()) {
          String topic = entry.getKey();
          Config config = topicConfigs.get(topic);
          if (config == null) {
            LOG.warn("Skip setting throttled replicas for topic {} since no configs can be read", topic);
            continue;
          }
          List<AlterConfigOp> ops = buildSetThrottledReplicasOps(config, entry.getValue());
          if (!ops.isEmpty()) {
            topicOps.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), ops);
          }
        }

        if (!topicOps.isEmpty()) {
          LOG.info("Setting throttled replicas on {} out of {} topics", topicOps.size(), throttledReplicas.size());
          batchAlterTopicConfigs(topicOps);
        }
      }

      LOG.info("Throttle setup complete for {} brokers and {} topics",
          participatingBrokers.size(), throttledReplicas.size());
    }
  }

  // Determines if a candidate task is ready to have its throttles removed.
  boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return
      // the task should not be in progress
      task.state() != ExecutionTaskState.IN_PROGRESS
      // the task should not be pending
      && task.state() != ExecutionTaskState.PENDING
      // replica throttles only apply to inter-broker replica movement
      && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // determines if a candidate task is in progress and related to inter-broker
  // replica movement.
  boolean taskIsInProgress(ExecutionTask task) {
    return task.state() == ExecutionTaskState.IN_PROGRESS && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // clear throttles for a specific list of execution tasks
  synchronized void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks) throws InterruptedException {
    if (throttlingEnabled()) {
      List<ExecutionProposal> completedProposals =
        completedTasks
          .stream()
          // Filter for completed tasks related to inter-broker replica movement
          .filter(this::shouldRemoveThrottleForTask)
          .map(ExecutionTask::proposal)
          // Throttles are not set for proposals without data movement
          .filter(ReplicationThrottleHelper::hasDataMovement)
          .collect(Collectors.toList());

      // These are the brokers which have completed a task with
      // inter-broker replica movement
      Set<Integer> participatingBrokers = getParticipatingBrokers(completedProposals);

      List<ExecutionProposal> inProgressProposals =
        inProgressTasks
          .stream()
          .filter(this::taskIsInProgress)
          .map(ExecutionTask::proposal)
          .filter(ReplicationThrottleHelper::hasDataMovement)
          .collect(Collectors.toList());

      // These are the brokers which currently have in-progress
      // inter-broker replica movement
      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(inProgressProposals);

      // Remove the brokers with in-progress replica moves from the brokers that have
      // completed inter-broker replica moves
      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(participatingBrokers);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing replica movement throttles from {} brokers in the cluster: {}",
          brokersToRemoveThrottlesFrom.size(), brokersToRemoveThrottlesFrom);
      removeThrottles(brokersToRemoveThrottlesFrom, completedProposals);
    }
  }

  /**
   * Removes the throttles that were set, but not removed yet -- e.g. because the execution that set them failed while
   * replicas were still moving, or a broker was unavailable when its throttle was to be removed. The throttles that the
   * given ongoing partition reassignments still need, and the throttled rates of unavailable brokers are retained, so that
   * a later call can remove them.
   *
   * @param partitionsBeingReassigned Partitions that are being reassigned.
   */
  synchronized void clearLeftoverThrottles(Set<TopicPartition> partitionsBeingReassigned)
  throws ExecutionException, InterruptedException, TimeoutException {
    if (_appliedThrottles.isEmpty()) {
      return;
    }
    List<ExecutionProposal> finishedProposals = new ArrayList<>();
    List<ExecutionProposal> inProgressProposals = new ArrayList<>();
    for (ExecutionProposal proposal : _appliedThrottles._proposals) {
      if (partitionsBeingReassigned.contains(proposal.topicPartition())) {
        inProgressProposals.add(proposal);
      } else {
        finishedProposals.add(proposal);
      }
    }
    Set<Integer> aliveBrokers = _adminClient.describeCluster().nodes().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                            .stream().map(Node::id).collect(Collectors.toSet());
    Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(_appliedThrottles._brokers);
    brokersToRemoveThrottlesFrom.retainAll(aliveBrokers);
    brokersToRemoveThrottlesFrom.removeAll(getParticipatingBrokers(inProgressProposals));
    if (!brokersToRemoveThrottlesFrom.isEmpty() || !finishedProposals.isEmpty()) {
      LOG.info("Removing leftover replica movement throttles from brokers {} and from the replicas of {} finished partition movements "
               + "(retaining the throttles of {} ongoing partition movements).", brokersToRemoveThrottlesFrom, finishedProposals.size(),
               inProgressProposals.size());
      removeThrottles(brokersToRemoveThrottlesFrom, finishedProposals);
    }
  }

  /**
   * Removes the throttled rate from the given brokers, and the throttled replicas of the given proposals from their topics,
   * in one batch for the brokers and one for the topics (the topics are cleared even if some brokers failed). A broker or a
   * topic is dropped from {@link #_appliedThrottles} once its removal is verified, or if it needed none (e.g. a deleted
   * topic). Failing to remove a throttle does not stop removing the others, but fails the call once the others are removed;
   * the throttles that could not be removed (or read, or verified) stay recorded, so that {@link #clearLeftoverThrottles(Set)}
   * removes them later.
   *
   * @param brokers Brokers to remove the throttled rate from.
   * @param proposals Proposals whose replicas are no longer to be throttled.
   */
  private void removeThrottles(Set<Integer> brokers, List<ExecutionProposal> proposals) throws InterruptedException {
    List<Exception> failures = new ArrayList<>();
    Set<Integer> failedBrokers = removeThrottledRateFromBrokers(brokers, failures);
    Map<String, List<ExecutionProposal>> proposalsByTopic = proposals.stream().collect(Collectors.groupingBy(ExecutionProposal::topic));
    Set<String> failedTopics = removeThrottledReplicasFromTopics(proposalsByTopic, failures);

    for (int broker : brokers) {
      if (!failedBrokers.contains(broker)) {
        _appliedThrottles._brokers.remove(broker);
        _throttledBrokers.remove(broker);
      }
    }
    proposalsByTopic.forEach((topic, topicProposals) -> {
      if (!failedTopics.contains(topic)) {
        _appliedThrottles._proposals.removeAll(topicProposals);
      }
    });
    if (!failedBrokers.isEmpty() || !failedTopics.isEmpty()) {
      throw new IllegalStateException(String.format("Failed to remove the replication throttles of brokers %s and topics %s.",
                                                    failedBrokers, failedTopics), failures.isEmpty() ? null : failures.get(0));
    }
  }

  /**
   * @param brokers Brokers to remove the throttled rate from.
   * @param failures Collects the failures.
   * @return The brokers whose throttled rate could not be read, removed or verified.
   */
  private Set<Integer> removeThrottledRateFromBrokers(Set<Integer> brokers, List<Exception> failures) throws InterruptedException {
    Set<Integer> failedBrokers = new TreeSet<>();
    if (brokers.isEmpty()) {
      return failedBrokers;
    }
    // Read configs via per-broker futures, so that a broker that became unreachable only fails (and stays recorded) itself.
    List<ConfigResource> brokerResources = brokers.stream()
        .map(id -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)))
        .collect(Collectors.toList());
    Map<ConfigResource, KafkaFuture<Config>> brokerFutures = describeConfigsInChunks(brokerResources);
    Map<ConfigResource, Collection<AlterConfigOp>> brokerOps = new HashMap<>();
    Map<ConfigResource, Map<String, String>> deletedValues = new HashMap<>();
    for (ConfigResource cf : brokerResources) {
      int brokerId = Integer.parseInt(cf.name());
      Config config;
      try {
        config = brokerFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Failed to read the configs of broker {} to remove its replication throttled rate; it is retried later.", brokerId, e);
        failedBrokers.add(brokerId);
        failures.add(e);
        continue;
      }
      Map<String, String> deleted = new HashMap<>();
      List<AlterConfigOp> ops = buildRemoveThrottleRateOps(config, brokerId, deleted);
      if (!ops.isEmpty()) {
        brokerOps.put(cf, ops);
        deletedValues.put(cf, deleted);
      }
    }
    for (ConfigResource cf : alterAndVerifyRemovals(brokerOps, deletedValues, failures)) {
      failedBrokers.add(Integer.parseInt(cf.name()));
    }
    return failedBrokers;
  }

  /**
   * @param proposalsByTopic The proposals whose replicas are no longer to be throttled, by topic.
   * @param failures Collects the failures.
   * @return The topics whose throttled replicas could not be read, removed or verified.
   */
  private Set<String> removeThrottledReplicasFromTopics(Map<String, List<ExecutionProposal>> proposalsByTopic, List<Exception> failures)
      throws InterruptedException {
    Set<String> failedTopics = new TreeSet<>();
    if (proposalsByTopic.isEmpty()) {
      return failedTopics;
    }
    List<ConfigResource> topicResources = proposalsByTopic.keySet().stream()
        .sorted()
        .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
        .collect(Collectors.toList());
    Map<ConfigResource, KafkaFuture<Config>> topicFutures = describeConfigsInChunks(topicResources);
    Map<ConfigResource, Collection<AlterConfigOp>> topicOps = new HashMap<>();
    Map<ConfigResource, Map<String, String>> deletedValues = new HashMap<>();
    // Fetched lazily and at most once, to resolve failures without issuing one listTopics per topic.
    Set<String> existingTopics = null;
    boolean topicListFetchAttempted = false;
    for (ConfigResource cf : topicResources) {
      String topic = cf.name();
      Config config;
      try {
        config = topicFutures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        if (!topicListFetchAttempted) {
          topicListFetchAttempted = true;
          existingTopics = tryListTopicNames();
        }
        if (existingTopics != null && !existingTopics.contains(topic)) {
          LOG.debug("Skip removing the throttled replicas of topic {} since it no longer exists", topic);
          continue;
        }
        // An unreadable topic (or one whose existence cannot be told) stays recorded, and is retried later.
        LOG.warn("Failed to read the configs of topic {} to remove its throttled replicas; it is retried later.", topic, e);
        failedTopics.add(topic);
        failures.add(e);
        continue;
      }
      Map<String, String> deleted = new HashMap<>();
      List<AlterConfigOp> ops = buildRemoveThrottledReplicasOps(config, topic,
                                                                getThrottledReplicasByTopic(proposalsByTopic.get(topic)).get(topic), deleted);
      if (!ops.isEmpty()) {
        topicOps.put(cf, ops);
        deletedValues.put(cf, deleted);
      }
    }
    for (ConfigResource cf : alterAndVerifyRemovals(topicOps, deletedValues, failures)) {
      failedTopics.add(cf.name());
    }
    return failedTopics;
  }

  /**
   * Batch-write the given removals with per-resource futures, then verify the written ones. A topic whose write fails because
   * it no longer exists needs no removal.
   *
   * @param ops The alter operations of each resource.
   * @param deletedValues The values of the deleted configs of each resource, as read before deleting them.
   * @param failures Collects the failures.
   * @return The resources whose removal failed or could not be verified.
   */
  private Set<ConfigResource> alterAndVerifyRemovals(Map<ConfigResource, Collection<AlterConfigOp>> ops,
                                                     Map<ConfigResource, Map<String, String>> deletedValues,
                                                     List<Exception> failures) throws InterruptedException {
    Set<ConfigResource> failed = new HashSet<>();
    if (ops.isEmpty()) {
      return failed;
    }
    Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>(ops.size());
    for (Map<ConfigResource, Collection<AlterConfigOp>> chunkOps : partitionOps(ops)) {
      futures.putAll(alterConfigs(chunkOps));
    }
    Map<ConfigResource, Collection<AlterConfigOp>> writtenOps = new HashMap<>();
    Set<String> existingTopics = null;
    boolean topicListFetchAttempted = false;
    for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : futures.entrySet()) {
      ConfigResource cf = entry.getKey();
      try {
        entry.getValue().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        writtenOps.put(cf, ops.get(cf));
      } catch (ExecutionException | TimeoutException e) {
        if (cf.type() == ConfigResource.Type.TOPIC) {
          if (!topicListFetchAttempted) {
            topicListFetchAttempted = true;
            existingTopics = tryListTopicNames();
          }
          if (existingTopics != null && !existingTopics.contains(cf.name())) {
            LOG.debug("Failed to remove the throttled replicas of topic {} since it does not exist", cf.name());
            continue;
          }
        }
        LOG.warn("Failed to remove the replication throttle configs {} of {}; they are retried later.", ops.get(cf), cf, e);
        failed.add(cf);
        failures.add(e);
      }
    }
    Set<ConfigResource> unverified = verifyBatchConfigs(writtenOps, deletedValues);
    if (!unverified.isEmpty()) {
      failures.add(new IllegalStateException("The removal of the following configs could not be verified within the time limit: "
                                             + unverified));
      failed.addAll(unverified);
    }
    return failed;
  }

  /**
   * Issue one chunk of alter-config operations. Overridden in tests to inject failures.
   *
   * @param chunkOps The alter operations of each resource, at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources.
   * @return The per-resource futures.
   */
  Map<ConfigResource, KafkaFuture<Void>> alterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> chunkOps) {
    return _adminClient.incrementalAlterConfigs(chunkOps).values();
  }

  // --- Ops builders extracted from the old single-resource methods ---

  /**
   * @param brokerId Broker id.
   * @param brokerConfigs Current configs of the broker.
   * @return The operations that set the leader and follower throttle rates of the given broker to the rates of this helper (adjusted
   * for the broker, if so), for the rates that differ from the current configs.
   */
  private List<AlterConfigOp> throttledRateOps(int brokerId, Config brokerConfigs) {
    if (_throttleRate == null) {
      throw new IllegalStateException("Throttle rate cannot be null");
    }
    Map<String, Long> throttleRateByConfigKey = Map.of(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, leaderThrottleRate(brokerId),
                                                       FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, followerThrottleRate(brokerId));
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleRateConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
      String throttleRate = String.valueOf(throttleRateByConfigKey.get(replicaThrottleRateConfigKey));
      ConfigEntry currThrottleRate = brokerConfigs.get(replicaThrottleRateConfigKey);
      if (currThrottleRate == null || !currThrottleRate.value().equals(throttleRate)) {
        LOG.debug("Setting {} to {} bytes/second for broker {}", replicaThrottleRateConfigKey, throttleRate, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleRateConfigKey, throttleRate), AlterConfigOp.OpType.SET));
      }
    }
    return ops;
  }

  private List<AlterConfigOp> buildSetThrottledReplicasOps(Config topicConfigs, Set<String> replicas) {
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String replicaThrottleConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
      ConfigEntry currThrottledReplicas = topicConfigs.get(replicaThrottleConfigKey);
      if (currThrottledReplicas != null && currThrottledReplicas.value().trim().equals(WILDCARD_ASTERISK)) {
        // The existing setup throttles all replica. So, nothing needs to be changed.
        continue;
      }

      // Merge new throttled replicas with existing configuration values.
      Set<String> newThrottledReplicas = new TreeSet<>(replicas);
      if (currThrottledReplicas != null && !currThrottledReplicas.value().isEmpty()) {
        newThrottledReplicas.addAll(Arrays.asList(currThrottledReplicas.value().split(",")));
      }
      ops.add(new AlterConfigOp(new ConfigEntry(replicaThrottleConfigKey, String.join(",", newThrottledReplicas)), AlterConfigOp.OpType.SET));
    }
    return ops;
  }

  /**
   * @param brokerConfigs The configs of the broker.
   * @param brokerId The broker id.
   * @param deletedValues Collects the values of the configs to delete, as read before deleting them.
   * @return The operations that remove the throttled rate set on the broker; a rate that comes from a lower-precedence source
   * (e.g. the cluster-wide default or the static config) is not set on the broker, and is left alone.
   */
  private List<AlterConfigOp> buildRemoveThrottleRateOps(Config brokerConfigs, int brokerId, Map<String, String> deletedValues) {
    List<AlterConfigOp> ops = new ArrayList<>();
    for (String throttleRateConfigKey : Arrays.asList(LEADER_REPLICATION_THROTTLED_RATE_CONFIG, FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
      ConfigEntry currThrottle = brokerConfigs.get(throttleRateConfigKey);
      if (currThrottle == null) {
        continue;
      }
      if (LOWER_PRECEDENCE_CONFIG_SOURCES.contains(currThrottle.source())) {
        LOG.debug("Skipping removal for {} not set on broker {}: {}", throttleRateConfigKey, brokerId, currThrottle);
      } else {
        LOG.debug("Removing {}: {} on broker {}", throttleRateConfigKey, currThrottle, brokerId);
        ops.add(new AlterConfigOp(new ConfigEntry(throttleRateConfigKey, null), AlterConfigOp.OpType.DELETE));
        deletedValues.put(throttleRateConfigKey, currThrottle.value());
      }
    }
    return ops;
  }

  private List<AlterConfigOp> buildRemoveThrottledReplicasOps(Config topicConfigs, String topic, Set<String> replicas,
                                                              Map<String, String> deletedValues) {
    List<AlterConfigOp> ops = new ArrayList<>();
    ConfigEntry currentLeaderThrottledReplicas = topicConfigs.get(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentLeaderThrottledReplicas != null) {
      if (currentLeaderThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all leader replicas. So, do not remove any leader replica throttle for topic {}", topic);
      } else {
        replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentLeaderThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
          deletedValues.put(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, currentLeaderThrottledReplicas.value());
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    ConfigEntry currentFollowerThrottledReplicas = topicConfigs.get(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
    if (currentFollowerThrottledReplicas != null) {
      if (currentFollowerThrottledReplicas.value().equals(WILDCARD_ASTERISK)) {
        LOG.debug("Existing config throttles all follower replicas. So, do not remove any follower replica throttle for topic {}", topic);
      } else {
        replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
        String newThrottledReplicas = removeReplicasFromConfig(currentFollowerThrottledReplicas.value(), replicas);
        if (newThrottledReplicas.isEmpty()) {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, null), AlterConfigOp.OpType.DELETE));
          deletedValues.put(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, currentFollowerThrottledReplicas.value());
        } else {
          ops.add(new AlterConfigOp(new ConfigEntry(FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, newThrottledReplicas), AlterConfigOp.OpType.SET));
        }
      }
    }
    return ops;
  }

  // --- Batch AdminClient helpers ---

  /**
   * Batch-read topic configs, gracefully handling non-existent topics by returning empty configs.
   * Uses describeConfigs().values() for per-topic error handling.
   *
   * @param topics the set of topic names to read configs for
   * @return a map from topic name to its config, with empty configs for non-existent topics
   */
  private Map<String, Config> batchGetTopicConfigs(Set<String> topics)
  throws ExecutionException, InterruptedException, TimeoutException {
    // Sort topic names for deterministic ordering in logs and diagnostics
    List<ConfigResource> resources = topics.stream()
        .sorted()
        .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
        .collect(Collectors.toList());

    Map<ConfigResource, KafkaFuture<Config>> futures = describeConfigsInChunks(resources);
    Map<String, Config> result = new HashMap<>();
    // Fetched lazily and at most once, to resolve failures without issuing one listTopics per topic.
    Set<String> existingTopics = null;

    for (ConfigResource cf : resources) {
      String topic = cf.name();
      try {
        result.put(topic, futures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));
      } catch (ExecutionException e) {
        if (existingTopics == null) {
          existingTopics = listTopicNames();
        }
        if (existingTopics.contains(topic)) {
          throw e;
        }
        result.put(topic, new Config(Collections.emptyList()));
      }
    }
    return result;
  }

  /**
   * Batch-write broker configs and wait for verification.
   * Uses .all() (fail-fast) rather than .values() (per-resource) because broker config failures are
   * not expected during normal operation -- unlike topics, brokers are not deleted mid-operation.
   * A broker failure here indicates a serious infrastructure issue that should fail the entire
   * throttle setup rather than partially applying configs.
   *
   * @param ops the map of broker config resources to their alter operations
   */
  private void batchAlterBrokerConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    for (Map<ConfigResource, Collection<AlterConfigOp>> chunkOps : partitionOps(ops)) {
      _adminClient.incrementalAlterConfigs(chunkOps)
          .all().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
    waitForBatchConfigs(ops);
  }

  /**
   * Batch-write topic configs using values() for per-topic error handling, then wait for
   * verification on successfully written topics.
   *
   * @param ops the map of topic config resources to their alter operations
   */
  private void batchAlterTopicConfigs(Map<ConfigResource, Collection<AlterConfigOp>> ops)
  throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>(ops.size());
    for (Map<ConfigResource, Collection<AlterConfigOp>> chunkOps : partitionOps(ops)) {
      futures.putAll(alterConfigs(chunkOps));
    }
    Map<ConfigResource, Collection<AlterConfigOp>> successfulOps = new HashMap<>();
    // Fetched lazily and at most once, to resolve failures without issuing one listTopics per topic.
    Set<String> existingTopics = null;

    for (Map.Entry<ConfigResource, KafkaFuture<Void>> entry : futures.entrySet()) {
      String topic = entry.getKey().name();
      try {
        entry.getValue().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        successfulOps.put(entry.getKey(), ops.get(entry.getKey()));
      } catch (ExecutionException e) {
        if (existingTopics == null) {
          existingTopics = listTopicNames();
        }
        if (existingTopics.contains(topic)) {
          throw e;
        }
        LOG.debug("Failed to change configs for topic {} since it does not exist", topic);
      }
    }

    if (!successfulOps.isEmpty()) {
      waitForBatchConfigs(successfulOps);
    }
  }

  /**
   * Batch verify configs by polling describeConfigs until all resources match expected values; fail if some do not.
   *
   * @param allOps the map of config resources to their expected alter operations
   * @see #verifyBatchConfigs(Map, Map)
   */
  void waitForBatchConfigs(Map<ConfigResource, Collection<AlterConfigOp>> allOps) {
    Set<ConfigResource> unverified = verifyBatchConfigs(allOps, Collections.emptyMap());
    // An interrupted verification is skipped, as before; the caller observes the interrupt.
    if (!unverified.isEmpty() && !Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("The following configs were not applied within the time limit: " + unverified);
    }
  }

  /**
   * Batch verify configs by polling describeConfigs until all resources match expected values.
   * Uses per-resource futures via values() so that verification of one resource does not depend on
   * the others. A topic that is confirmed deleted mid-verification is dropped from verification;
   * any other per-resource failure (e.g. a transient network error, or a broker read failure)
   * keeps the resource pending so that it is retried instead of silently passing unverified.
   *
   * @param allOps the map of config resources to their expected alter operations
   * @param deletedValuesByResource the values of the configs that the operations delete, as read before deleting them (if known)
   * @return the resources that could not be verified (all pending ones if the verification is interrupted)
   */
  Set<ConfigResource> verifyBatchConfigs(Map<ConfigResource, Collection<AlterConfigOp>> allOps,
                                         Map<ConfigResource, Map<String, String>> deletedValuesByResource) {
    return verifyBatchConfigs(allOps, deletedValuesByResource, _retries);
  }

  /**
   * @param allOps the map of config resources to their expected alter operations
   * @param deletedValuesByResource the values of the configs that the operations delete, as read before deleting them (if known)
   * @param retries the number of verification attempts
   * @return the resources that could not be verified (all pending ones if the verification is interrupted)
   * @see #verifyBatchConfigs(Map, Map)
   */
  private Set<ConfigResource> verifyBatchConfigs(Map<ConfigResource, Collection<AlterConfigOp>> allOps,
                                                 Map<ConfigResource, Map<String, String>> deletedValuesByResource,
                                                 int retries) {
    // Resources that still await verification. Verified resources and confirmed-deleted topics are
    // removed as verification progresses.
    Map<ConfigResource, Map<String, String>> pendingByResource = new HashMap<>();
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : allOps.entrySet()) {
      // Use HashMap::new instead of Collectors.toMap to allow inserting null values
      Map<String, String> expected = entry.getValue().stream()
          .collect(HashMap::new, (m, o) -> m.put(o.configEntry().name(), o.configEntry().value()), HashMap::putAll);
      pendingByResource.put(entry.getKey(), expected);
    }
    if (pendingByResource.isEmpty()) {
      return Collections.emptySet();
    }

    AtomicInteger attempts = new AtomicInteger();
    boolean verified = CruiseControlMetricsUtils.retry(() -> {
      try {
        if (pendingByResource.isEmpty()) {
          return false;
        }
        int attempt = attempts.incrementAndGet();
        Map<ConfigResource, KafkaFuture<Config>> futures = describeConfigsInChunks(new ArrayList<>(pendingByResource.keySet()));
        // Existing topic names, fetched lazily and at most once per verification attempt, shared
        // by all topics that fail verification in this attempt. Remains null if the listing fails,
        // in which case topics are conservatively treated as still existing and kept pending.
        Set<String> existingTopics = null;
        boolean topicListFetchAttempted = false;
        Iterator<Map.Entry<ConfigResource, Map<String, String>>> pendingIter = pendingByResource.entrySet().iterator();
        while (pendingIter.hasNext()) {
          Map.Entry<ConfigResource, Map<String, String>> entry = pendingIter.next();
          ConfigResource cf = entry.getKey();
          try {
            Config config = futures.get(cf).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (configsEqual(config, entry.getValue(), deletedValuesByResource.getOrDefault(cf, Collections.emptyMap()))) {
              pendingIter.remove();
            } else {
              LOG.warn("Attempt {}/{}: configs {} are not applied to {} yet, current values: {}. If this persists, check whether "
                       + "these configs are being changed outside of Cruise Control.", attempt, retries, entry.getValue(), cf,
                       currentValues(config, entry.getValue().keySet()));
            }
          } catch (ExecutionException | TimeoutException e) {
            if (cf.type() == ConfigResource.Type.TOPIC && !topicListFetchAttempted) {
              topicListFetchAttempted = true;
              existingTopics = tryListTopicNames();
            }
            if (cf.type() == ConfigResource.Type.TOPIC && existingTopics != null && !existingTopics.contains(cf.name())) {
              LOG.debug("Skipping config verification for topic {} since it no longer exists", cf.name());
              pendingIter.remove();
            } else {
              // Keep the resource pending: a transient failure must not let an unverified
              // (potentially unthrottled) config pass silently.
              LOG.warn("Failed to verify config for {}; will retry", cf, e);
            }
          }
        }
        return !pendingByResource.isEmpty();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during batch config verification for {} resources, skipping verification",
            pendingByResource.size(), e);
        Thread.currentThread().interrupt();
        return false;
      }
    }, RETRY_BACKOFF_SCALE_MS, RETRY_BACKOFF_BASE, retries, MAX_RETRY_SLEEP_MS);
    // Verified, unless the verification ran out of attempts or was interrupted with resources still pending.
    return verified && !Thread.currentThread().isInterrupted() ? Collections.emptySet() : new HashSet<>(pendingByResource.keySet());
  }

  private static String currentValues(Config configs, Set<String> configNames) {
    return configNames.stream().map(name -> {
      ConfigEntry configEntry = configs.get(name);
      return configEntry == null ? name + "=null" : String.format("%s=%s (%s)", name, configEntry.value(), configEntry.source());
    }).collect(Collectors.joining(", ", "{", "}"));
  }

  /**
   * Best-effort topic listing used during verification. A failure to list is surfaced as
   * {@code null} so that callers conservatively treat topics as still existing and keep
   * retrying instead of passing unverified.
   *
   * @return the set of existing topic names, or {@code null} if the listing failed
   */
  private Set<String> tryListTopicNames() {
    try {
      return listTopicNames();
    } catch (ExecutionException | TimeoutException e) {
      return null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Issue describeConfigs in chunks of at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources
   * and return the per-resource futures from all chunks.
   *
   * @param resources the config resources to describe
   * @return a map from each resource to its config future
   */
  private Map<ConfigResource, KafkaFuture<Config>> describeConfigsInChunks(List<ConfigResource> resources) {
    Map<ConfigResource, KafkaFuture<Config>> futures = new HashMap<>(resources.size());
    for (int i = 0; i < resources.size(); i += MAX_RESOURCES_PER_ADMIN_REQUEST) {
      List<ConfigResource> chunk = resources.subList(i, Math.min(resources.size(), i + MAX_RESOURCES_PER_ADMIN_REQUEST));
      futures.putAll(_adminClient.describeConfigs(chunk).values());
    }
    return futures;
  }

  /**
   * Partition an alter-config request into chunks of at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources.
   *
   * @param ops the full map of config resources to their alter operations
   * @return the input split into chunks, each holding at most {@link #MAX_RESOURCES_PER_ADMIN_REQUEST} resources
   */
  private static List<Map<ConfigResource, Collection<AlterConfigOp>>> partitionOps(Map<ConfigResource, Collection<AlterConfigOp>> ops) {
    List<Map<ConfigResource, Collection<AlterConfigOp>>> chunks = new ArrayList<>();
    Map<ConfigResource, Collection<AlterConfigOp>> currentChunk = new HashMap<>();
    for (Map.Entry<ConfigResource, Collection<AlterConfigOp>> entry : ops.entrySet()) {
      if (currentChunk.size() == MAX_RESOURCES_PER_ADMIN_REQUEST) {
        chunks.add(currentChunk);
        currentChunk = new HashMap<>();
      }
      currentChunk.put(entry.getKey(), entry.getValue());
    }
    if (!currentChunk.isEmpty()) {
      chunks.add(currentChunk);
    }
    return chunks;
  }

  boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  /**
   * @return A snapshot of the brokers whose throttle rate has been set by this helper and not yet removed.
   */
  synchronized Set<Integer> throttledBrokers() {
    return new TreeSet<>(_throttledBrokers);
  }

  /**
   * Not synchronized: the executor state reads it while setting, clearing or updating the throttles may hold the lock of this helper
   * for as long as it takes to alter and verify the broker configs.
   *
   * @return The leader and follower throttle rates (bytes/sec) of the brokers whose throttle rate has been set by this helper and not
   * yet removed, by broker id; empty if no broker is throttled.
   */
  SortedMap<Integer, ExecutorState.BrokerReplicationThrottle> throttleRateByBroker() {
    SortedMap<Integer, ExecutorState.BrokerReplicationThrottle> throttleRateByBroker = new TreeMap<>();
    for (int brokerId : _throttledBrokers) {
      Long leaderThrottleRate = leaderThrottleRate(brokerId);
      Long followerThrottleRate = followerThrottleRate(brokerId);
      if (leaderThrottleRate != null && followerThrottleRate != null) {
        throttleRateByBroker.put(brokerId, new ExecutorState.BrokerReplicationThrottle(leaderThrottleRate, followerThrottleRate));
      }
    }
    return throttleRateByBroker;
  }

  /**
   * @param brokerId Broker id.
   * @return The leader throttle rate (bytes/sec) that is (or will be) set on the given broker, or {@code null} if throttling is disabled.
   */
  Long leaderThrottleRate(int brokerId) {
    return _leaderThrottleRateByBroker.getOrDefault(brokerId, _throttleRate);
  }

  /**
   * @param brokerId Broker id.
   * @return The follower throttle rate (bytes/sec) that is (or will be) set on the given broker, or {@code null} if throttling is disabled.
   */
  Long followerThrottleRate(int brokerId) {
    return _followerThrottleRateByBroker.getOrDefault(brokerId, _throttleRate);
  }

  /**
   * Update the leader and follower throttle rates of the given broker -- see {@link #updateThrottleRates(Map, Map)}.
   *
   * @param brokerId Broker id.
   * @param leaderThrottleRate The new leader throttle rate (bytes/sec).
   * @param followerThrottleRate The new follower throttle rate (bytes/sec).
   */
  synchronized void updateThrottleRates(int brokerId, long leaderThrottleRate, long followerThrottleRate)
      throws ExecutionException, InterruptedException, TimeoutException {
    updateThrottleRates(Map.of(brokerId, leaderThrottleRate), Map.of(brokerId, followerThrottleRate));
  }

  /**
   * Update the leader and follower throttle rates of the given brokers. The new rates are applied right away to the brokers that
   * are currently throttled by this helper, in one batch (describe, alter, verify), with a small verification budget
   * ({@link #THROTTLE_RATE_UPDATE_RETRIES}) because the caller holds the lock of this helper. The rates of the other brokers are
   * applied once they participate in a subsequent batch of throttled replica movements -- i.e. the throttle is never set on a
   * broker whose throttle has been removed.
   *
   * @param leaderThrottleRateByBroker The new leader throttle rate (bytes/sec) by broker.
   * @param followerThrottleRateByBroker The new follower throttle rate (bytes/sec) by broker.
   */
  synchronized void updateThrottleRates(Map<Integer, Long> leaderThrottleRateByBroker, Map<Integer, Long> followerThrottleRateByBroker)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (!throttlingEnabled()) {
      throw new IllegalStateException("Cannot update throttle rates when throttling is disabled.");
    }
    _leaderThrottleRateByBroker.putAll(leaderThrottleRateByBroker);
    _followerThrottleRateByBroker.putAll(followerThrottleRateByBroker);
    Set<Integer> brokersToUpdate = new TreeSet<>(leaderThrottleRateByBroker.keySet());
    brokersToUpdate.addAll(followerThrottleRateByBroker.keySet());
    brokersToUpdate.retainAll(_throttledBrokers);
    if (brokersToUpdate.isEmpty()) {
      return;
    }
    List<ConfigResource> resources = brokersToUpdate.stream()
        .map(brokerId -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId)))
        .collect(Collectors.toList());
    // A broker that cannot be described or updated (e.g. it died during the execution) is skipped, so it does not block the others.
    Map<ConfigResource, KafkaFuture<Config>> configFutureByResource = describeConfigsInChunks(resources);
    Map<ConfigResource, Collection<AlterConfigOp>> opsByResource = new HashMap<>();
    for (ConfigResource resource : resources) {
      Config brokerConfigs;
      try {
        brokerConfigs = configFutureByResource.get(resource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Skip updating the replication throttle of broker {}: failed to describe its configs.", resource.name(), e);
        continue;
      }
      List<AlterConfigOp> ops = throttledRateOps(Integer.parseInt(resource.name()), brokerConfigs);
      if (!ops.isEmpty()) {
        opsByResource.put(resource, ops);
      }
    }
    if (opsByResource.isEmpty()) {
      return;
    }
    Map<ConfigResource, KafkaFuture<Void>> alterFutureByResource = new HashMap<>(opsByResource.size());
    for (Map<ConfigResource, Collection<AlterConfigOp>> chunkOps : partitionOps(opsByResource)) {
      alterFutureByResource.putAll(alterConfigs(chunkOps));
    }
    for (ConfigResource resource : new ArrayList<>(opsByResource.keySet())) {
      try {
        alterFutureByResource.get(resource).get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Failed to update the replication throttle of broker {}.", resource.name(), e);
        opsByResource.remove(resource);
      }
    }
    Set<ConfigResource> unverified = verifyBatchConfigs(opsByResource, Collections.emptyMap(), THROTTLE_RATE_UPDATE_RETRIES);
    if (!unverified.isEmpty() && !Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("The following replication throttle rate updates were not applied within the time limit: "
                                      + unverified);
    }
  }

  /**
   * A proposal has data movement if it adds at least one replica to a broker. Proposals that only remove replicas (e.g. to
   * decrease the replication factor) or reorder them do not require replicating data, hence they need no throttle. Throttling
   * such proposals would only slow down the catch-up of their out-of-sync replicas, which delays the completion of the
   * reassignment -- especially in a cluster under heavy load.
   *
   * @param proposal Execution proposal to check.
   * @return {@code true} if the given proposal adds at least one replica to a broker, {@code false} otherwise.
   */
  static boolean hasDataMovement(ExecutionProposal proposal) {
    return !proposal.replicasToAdd().isEmpty();
  }

  private static List<ExecutionProposal> proposalsWithDataMovement(List<ExecutionProposal> proposals) {
    return proposals.stream().filter(ReplicationThrottleHelper::hasDataMovement).collect(Collectors.toList());
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionProposal> replicaMovementProposals) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      participatingBrokers.addAll(proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
      participatingBrokers.addAll(proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
    }
    participatingBrokers.removeAll(_deadBrokers);
    return participatingBrokers;
  }

  private Map<String, Set<String>> getThrottledReplicasByTopic(List<ExecutionProposal> replicaMovementProposals) {
    Map<String, Set<String>> throttledReplicasByTopic = new HashMap<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      String topic = proposal.topic();
      int partitionId = proposal.partitionId();
      Stream<Integer> brokers = Stream.concat(
        proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId),
        proposal.replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId));
      Set<String> throttledReplicas = throttledReplicasByTopic
        .computeIfAbsent(topic, x -> new TreeSet<>());
      brokers.forEach(brokerId -> throttledReplicas.add(partitionId + ":" + brokerId));
    }
    return throttledReplicasByTopic;
  }

  private Set<String> listTopicNames() throws InterruptedException, TimeoutException, ExecutionException {
    try {
      return _adminClient.listTopics().names().get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOG.error("Unable to list topics due to {}", e.getMessage());
      throw e;
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    List<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  static boolean configsEqual(Config configs, Map<String, String> expectedValues) {
    return configsEqual(configs, expectedValues, Collections.emptyMap());
  }

  /**
   * @param configs The configs read from an entity.
   * @param expectedValues The expected values of the configs, {@code null} for a deleted config.
   * @param deletedValues The values of the deleted configs before they were deleted (if known).
   * @return {@code true} if the given configs have the expected values, {@code false} otherwise.
   */
  static boolean configsEqual(Config configs, Map<String, String> expectedValues, Map<String, String> deletedValues) {
    for (Map.Entry<String, String> entry : expectedValues.entrySet()) {
      ConfigEntry configEntry = configs.get(entry.getKey());
      if (configEntry == null || configEntry.value() == null || configEntry.value().isEmpty()) {
        if (entry.getValue() != null) {
          return false;
        }
      } else if (LOWER_PRECEDENCE_CONFIG_SOURCES.contains(configEntry.source()) && entry.getValue() == null) {
        if (configEntry.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG && configEntry.value().equals(deletedValues.get(entry.getKey()))) {
          // A broker may report the Kafka default as the source of a deleted config before it stops reporting the deleted value.
          return false;
        }
        // Deleting a dynamic config does not remove a value from a lower precedence source, e.g. the cluster-wide default
        // (e.g. when a cluster-wide default replication throttle is set via kafka-configs --entity-default).
        LOG.debug("Found {} config: {}, skipping comparison", configEntry.source(), configEntry);
      } else if (!Objects.equals(entry.getValue(), configEntry.value())) {
        return false;
      }
    }
    return true;
  }

  /**
   * The replication throttles that have been set, but not removed yet: the brokers whose throttled rate has been set, and
   * the inter-broker replica movements whose replicas have been throttled. The helpers of consecutive executions share an
   * instance, so that the throttles an execution could not remove are removed later via {@link #clearLeftoverThrottles(Set)}.
   */
  static final class AppliedThrottles {
    private final Set<Integer> _brokers = ConcurrentHashMap.newKeySet();
    private final Set<ExecutionProposal> _proposals = ConcurrentHashMap.newKeySet();

    /**
     * @return {@code true} if there is no throttle to remove, {@code false} otherwise.
     */
    boolean isEmpty() {
      return _brokers.isEmpty() && _proposals.isEmpty();
    }

    /**
     * @return Brokers whose throttled rate has been set, but not removed yet.
     */
    Set<Integer> brokers() {
      return Collections.unmodifiableSet(_brokers);
    }

    /**
     * @return Inter-broker replica movements whose replicas have been throttled, but not unthrottled yet.
     */
    Set<ExecutionProposal> proposals() {
      return Collections.unmodifiableSet(_proposals);
    }
  }
}
