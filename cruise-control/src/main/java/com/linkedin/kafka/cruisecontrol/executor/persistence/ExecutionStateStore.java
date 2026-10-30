/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.common.annotation.InterfaceStability;


/**
 * A pluggable store that persists the state of the ongoing execution, so that Cruise Control can recover it after a
 * restart (e.g. clean up leftover replication throttles or resume the remaining tasks).
 *
 * The executor calls {@link #save(PersistedExecutionState)} whenever the state of the ongoing execution changes -- i.e.
 * when the execution starts, before a batch of tasks is submitted to Kafka, and whenever a task finishes. Hence,
 * implementations are expected to be cheap and each save should atomically replace the previously saved state.
 *
 * At most one execution state is stored at a time.
 */
@InterfaceStability.Evolving
public interface ExecutionStateStore extends CruiseControlConfigurable {

  /**
   * Atomically replace the stored execution state with the given state.
   *
   * @param state Execution state to store.
   * @throws IOException If the state cannot be stored.
   */
  void save(PersistedExecutionState state) throws IOException;

  /**
   * @return The stored execution state, or {@link Optional#empty()} if there is no stored execution state.
   * @throws IOException If the stored state cannot be read.
   */
  Optional<PersistedExecutionState> load() throws IOException;

  /**
   * Delete the stored execution state (if any).
   *
   * @throws IOException If the stored state cannot be deleted.
   */
  void delete() throws IOException;

  /**
   * Atomically replace the stored retained replication throttles -- i.e. the throttles that executions set, but could not
   * remove yet (e.g. of an unavailable broker) -- with the given JSON document (see
   * {@link PersistedExecutionStateSerde#retainedThrottlesToJson}). They outlive the execution that set them, so they are stored
   * separately from the execution state. A store that does not persist them ignores the call.
   *
   * @param json Retained replication throttles to store, or {@code null} to delete the stored ones.
   * @throws IOException If the retained replication throttles cannot be stored.
   */
  default void saveRetainedThrottles(String json) throws IOException { }

  /**
   * @return The stored retained replication throttles (see {@link #saveRetainedThrottles(String)}), or {@link Optional#empty()}
   * if there are none.
   * @throws IOException If the stored retained replication throttles cannot be read.
   */
  default Optional<String> loadRetainedThrottles() throws IOException {
    return Optional.empty();
  }

  /**
   * Release any resources held by the store.
   */
  default void close() { }
}
