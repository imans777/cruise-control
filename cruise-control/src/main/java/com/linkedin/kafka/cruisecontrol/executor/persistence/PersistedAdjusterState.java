/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


/**
 * What the concurrency adjuster has changed during an execution, as of a checkpoint, so that an execution resumed after a restart
 * continues from there rather than from the start: the replication throttles it set on each broker (PR #13), the brokers whose slow
 * start has ended, and the inter-broker partition movement concurrency of each broker.
 */
public final class PersistedAdjusterState {
  private final Map<Integer, Long> _leaderThrottleByBroker;
  private final Map<Integer, Long> _followerThrottleByBroker;
  private final Set<Integer> _leaderSlowStartEndedBrokers;
  private final Set<Integer> _followerSlowStartEndedBrokers;
  private final Map<Integer, Integer> _interBrokerConcurrencyByBroker;

  /**
   * @param leaderThrottleByBroker The leader replication throttle (bytes/sec) of each broker.
   * @param followerThrottleByBroker The follower replication throttle (bytes/sec) of each broker.
   * @param leaderSlowStartEndedBrokers Brokers whose leader throttle is past slow start.
   * @param followerSlowStartEndedBrokers Brokers whose follower throttle is past slow start.
   * @param interBrokerConcurrencyByBroker The inter-broker partition movement concurrency of each broker.
   */
  public PersistedAdjusterState(Map<Integer, Long> leaderThrottleByBroker,
                                Map<Integer, Long> followerThrottleByBroker,
                                Set<Integer> leaderSlowStartEndedBrokers,
                                Set<Integer> followerSlowStartEndedBrokers,
                                Map<Integer, Integer> interBrokerConcurrencyByBroker) {
    _leaderThrottleByBroker = Collections.unmodifiableMap(new TreeMap<>(validateNotNull(leaderThrottleByBroker, "leader throttles")));
    _followerThrottleByBroker = Collections.unmodifiableMap(new TreeMap<>(validateNotNull(followerThrottleByBroker,
                                                                                          "follower throttles")));
    _leaderSlowStartEndedBrokers = Collections.unmodifiableSet(new TreeSet<>(validateNotNull(leaderSlowStartEndedBrokers,
                                                                                             "leader slow start")));
    _followerSlowStartEndedBrokers = Collections.unmodifiableSet(new TreeSet<>(validateNotNull(followerSlowStartEndedBrokers,
                                                                                               "follower slow start")));
    _interBrokerConcurrencyByBroker = Collections.unmodifiableMap(new TreeMap<>(validateNotNull(interBrokerConcurrencyByBroker,
                                                                                                "concurrency")));
  }

  public Map<Integer, Long> leaderThrottleByBroker() {
    return _leaderThrottleByBroker;
  }

  public Map<Integer, Long> followerThrottleByBroker() {
    return _followerThrottleByBroker;
  }

  public Set<Integer> leaderSlowStartEndedBrokers() {
    return _leaderSlowStartEndedBrokers;
  }

  public Set<Integer> followerSlowStartEndedBrokers() {
    return _followerSlowStartEndedBrokers;
  }

  public Map<Integer, Integer> interBrokerConcurrencyByBroker() {
    return _interBrokerConcurrencyByBroker;
  }

  /**
   * @return {@code true} if there is nothing to restore.
   */
  public boolean isEmpty() {
    return _leaderThrottleByBroker.isEmpty() && _followerThrottleByBroker.isEmpty() && _leaderSlowStartEndedBrokers.isEmpty()
           && _followerSlowStartEndedBrokers.isEmpty() && _interBrokerConcurrencyByBroker.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PersistedAdjusterState)) {
      return false;
    }
    PersistedAdjusterState that = (PersistedAdjusterState) o;
    return _leaderThrottleByBroker.equals(that._leaderThrottleByBroker) && _followerThrottleByBroker.equals(that._followerThrottleByBroker)
           && _leaderSlowStartEndedBrokers.equals(that._leaderSlowStartEndedBrokers)
           && _followerSlowStartEndedBrokers.equals(that._followerSlowStartEndedBrokers)
           && _interBrokerConcurrencyByBroker.equals(that._interBrokerConcurrencyByBroker);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_leaderThrottleByBroker, _followerThrottleByBroker, _leaderSlowStartEndedBrokers, _followerSlowStartEndedBrokers,
                        _interBrokerConcurrencyByBroker);
  }

  @Override
  public String toString() {
    return String.format("{leaderThrottleByBroker: %s, followerThrottleByBroker: %s, leaderSlowStartEnded: %s, followerSlowStartEnded: %s, "
                         + "interBrokerConcurrencyByBroker: %s}", _leaderThrottleByBroker, _followerThrottleByBroker,
                         _leaderSlowStartEndedBrokers, _followerSlowStartEndedBrokers, _interBrokerConcurrencyByBroker);
  }
}
