/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState.BrokerReplicationThrottle;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;


public class ExecutorStateTest {
  private static final String ONGOING_REPLICATION_THROTTLE_BY_BROKER = "ongoingReplicationThrottleByBroker";

  @Test
  public void testOngoingReplicationThrottleByBroker() {
    ExecutorState executorState = ExecutorState.noTaskInProgress(Set.of(5), Set.of(6));

    // Not reported when no broker is throttled.
    assertSame(executorState, executorState.withReplicationThrottleByBroker(null));
    assertSame(executorState, executorState.withReplicationThrottleByBroker(new TreeMap<>()));
    assertFalse(executorState.getJsonStructure(false).containsKey(ONGOING_REPLICATION_THROTTLE_BY_BROKER));

    SortedMap<Integer, BrokerReplicationThrottle> throttleByBroker = new TreeMap<>(Map.of(2, new BrokerReplicationThrottle(1048576L, 1048576L),
                                                                                          1, new BrokerReplicationThrottle(10485760L, 20971520L)));
    ExecutorState throttledState = executorState.withReplicationThrottleByBroker(throttleByBroker);
    Map<String, Object> jsonStructure = throttledState.getJsonStructure(false);
    assertEquals("{\"1\":{\"leader\":10485760,\"follower\":20971520},\"2\":{\"leader\":1048576,\"follower\":1048576}}",
                 new Gson().toJson(jsonStructure.get(ONGOING_REPLICATION_THROTTLE_BY_BROKER)));
    // The rest of the state is kept.
    assertEquals(executorState.state(), throttledState.state());
    assertEquals(Set.of(5), jsonStructure.get("recentlyDemotedBrokers"));
    assertEquals(Set.of(6), jsonStructure.get("recentlyRemovedBrokers"));
    assertFalse(executorState.getJsonStructure(false).containsKey(ONGOING_REPLICATION_THROTTLE_BY_BROKER));
  }

  @Test
  public void testReplicationThrottleByBrokerAlongWithTheOtherThrottles() {
    ExecutorState executorState = ExecutorState.noTaskInProgress(Set.of(), Set.of());
    SortedMap<Integer, BrokerReplicationThrottle> throttleByBroker = new TreeMap<>(Map.of(1, new BrokerReplicationThrottle(3L, 4L)));
    // In either order, the state keeps the base rate and the log dir throttles as well as the per-broker rates.
    for (ExecutorState state : List.of(executorState.withThrottles(2L, Map.of(1, "5")).withReplicationThrottleByBroker(throttleByBroker),
                                       executorState.withReplicationThrottleByBroker(throttleByBroker).withThrottles(2L, Map.of(1, "5")))) {
      Map<String, Object> jsonStructure = state.getJsonStructure(false);
      assertEquals(2L, jsonStructure.get("ongoingReplicationThrottle"));
      assertEquals(Map.of(1, "5"), jsonStructure.get("ongoingLogDirThrottleByBroker"));
      assertEquals("{\"1\":{\"leader\":3,\"follower\":4}}", new Gson().toJson(jsonStructure.get(ONGOING_REPLICATION_THROTTLE_BY_BROKER)));
    }
  }
}
