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

import com.linkedin.kafka.cruisecontrol.executor.concurrency.ExecutionConcurrencySummary;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.TaskType.LEADER_ACTION;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorState.State.STOPPING_EXECUTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the execution phases in {@link ExecutorState}.
 */
public class ExecutorStateTest {
  private static final ExecutionConcurrencySummary CONCURRENCY_SUMMARY =
      new ExecutionConcurrencySummary(true, Map.of(0, 1), Map.of(0, 1), Map.of(0, 1), 1);

  /**
   * @param numTasksByStateByType The number of tasks in each state (other than 0), by task type.
   * @param finishedInterBrokerDataMovementInMB Finished inter-broker data movement in MB.
   * @param remainingInterBrokerDataToMoveInMB Remaining inter-broker data to move in MB.
   * @param finishedIntraBrokerDataMovementInMB Finished intra-broker data movement in MB.
   * @param remainingIntraBrokerDataToMoveInMB Remaining intra-broker data to move in MB.
   * @return A summary of execution tasks with the given numbers.
   */
  private static ExecutionTasksSummary tasksSummary(Map<ExecutionTask.TaskType, Map<ExecutionTaskState, Integer>> numTasksByStateByType,
                                                    long finishedInterBrokerDataMovementInMB,
                                                    long remainingInterBrokerDataToMoveInMB,
                                                    long finishedIntraBrokerDataMovementInMB,
                                                    long remainingIntraBrokerDataToMoveInMB) {
    Map<ExecutionTask.TaskType, Map<ExecutionTaskState, Integer>> taskStat = new HashMap<>();
    for (ExecutionTask.TaskType type : ExecutionTask.TaskType.cachedValues()) {
      Map<ExecutionTaskState, Integer> numTasksByState = new EnumMap<>(ExecutionTaskState.class);
      for (ExecutionTaskState state : ExecutionTaskState.cachedValues()) {
        numTasksByState.put(state, numTasksByStateByType.getOrDefault(type, Map.of()).getOrDefault(state, 0));
      }
      taskStat.put(type, numTasksByState);
    }
    return new ExecutionTasksSummary(finishedInterBrokerDataMovementInMB, finishedIntraBrokerDataMovementInMB, 0L, 0L,
                                     remainingInterBrokerDataToMoveInMB, remainingIntraBrokerDataToMoveInMB, taskStat, new HashMap<>());
  }

  private static ExecutorState executorState(ExecutorState.State state, ExecutionTasksSummary tasksSummary) {
    return ExecutorState.operationInProgress(state, tasksSummary, CONCURRENCY_SUMMARY, "uuid", "reason", Set.of(), Set.of(), true);
  }

  private static Map<String, Object> phase(ExecutionTask.TaskType type, String status, int numFinished, int numTotal,
                                           Long finishedDataMovement, Long totalDataToMove) {
    Map<String, Object> phase = new HashMap<>();
    phase.put("phase", type);
    phase.put("status", status);
    phase.put("numFinishedMovements", numFinished);
    phase.put("numTotalMovements", numTotal);
    if (totalDataToMove != null) {
      phase.put("finishedDataMovement", finishedDataMovement);
      phase.put("totalDataToMove", totalDataToMove);
    }
    return phase;
  }

  @Test
  public void testExecutionPhasesShowUpcomingPhases() {
    // The execution is in its inter-broker phase. Its intra-broker and leadership phases have not started yet.
    ExecutionTasksSummary tasksSummary = tasksSummary(
        Map.of(INTER_BROKER_REPLICA_ACTION, Map.of(ExecutionTaskState.COMPLETED, 1, ExecutionTaskState.IN_PROGRESS, 1,
                                                   ExecutionTaskState.PENDING, 1),
               INTRA_BROKER_REPLICA_ACTION, Map.of(ExecutionTaskState.PENDING, 3),
               LEADER_ACTION, Map.of(ExecutionTaskState.PENDING, 2)),
        20L, 40L, 0L, 300L);
    ExecutorState executorState = executorState(INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, tasksSummary);

    assertEquals(List.of(phase(INTER_BROKER_REPLICA_ACTION, "IN_PROGRESS", 1, 3, 20L, 60L),
                         phase(INTRA_BROKER_REPLICA_ACTION, "PENDING", 0, 3, 0L, 300L),
                         phase(LEADER_ACTION, "PENDING", 0, 2, null, null)),
                 executorState.getJsonStructure(false).get("executionPhases"));
    assertTrue(executorState.getPlaintext(), executorState.getPlaintext().endsWith(
        ", executionPhases: [INTER_BROKER_REPLICA_ACTION: IN_PROGRESS 1/3 (20/60 MB), INTRA_BROKER_REPLICA_ACTION: PENDING 0/3 (0/300 MB), "
        + "LEADER_ACTION: PENDING 0/2]}"));
  }

  @Test
  public void testExecutionPhasesWhileMovingReplicasOffBlockedLogdirs() {
    // Inter-broker replica movements wait for space that intra-broker replica movements are freeing. A phase without movements
    // (here: leadership) is not listed.
    ExecutionTasksSummary tasksSummary = tasksSummary(
        Map.of(INTER_BROKER_REPLICA_ACTION, Map.of(ExecutionTaskState.COMPLETED, 2, ExecutionTaskState.PENDING, 1),
               INTRA_BROKER_REPLICA_ACTION, Map.of(ExecutionTaskState.IN_PROGRESS, 1, ExecutionTaskState.PENDING, 1)),
        40L, 20L, 0L, 200L);
    ExecutorState executorState = executorState(INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS, tasksSummary);

    assertEquals(List.of(phase(INTER_BROKER_REPLICA_ACTION, "IN_PROGRESS", 2, 3, 40L, 60L),
                         phase(INTRA_BROKER_REPLICA_ACTION, "IN_PROGRESS", 0, 2, 0L, 200L)),
                 executorState.getJsonStructure(false).get("executionPhases"));
  }

  @Test
  public void testExecutionPhasesWhileStopping() {
    // The inter-broker phase finished, the intra-broker phase is being stopped, and the leadership phase is cancelled.
    ExecutionTasksSummary tasksSummary = tasksSummary(
        Map.of(INTER_BROKER_REPLICA_ACTION, Map.of(ExecutionTaskState.COMPLETED, 1, ExecutionTaskState.DEAD, 1),
               INTRA_BROKER_REPLICA_ACTION, Map.of(ExecutionTaskState.IN_PROGRESS, 1, ExecutionTaskState.PENDING, 1),
               LEADER_ACTION, Map.of(ExecutionTaskState.PENDING, 2)),
        40L, 0L, 0L, 200L);
    ExecutorState executorState = executorState(STOPPING_EXECUTION, tasksSummary);

    assertEquals(List.of(phase(INTER_BROKER_REPLICA_ACTION, "FINISHED", 2, 2, 40L, 40L),
                         phase(INTRA_BROKER_REPLICA_ACTION, "IN_PROGRESS", 0, 2, 0L, 200L),
                         phase(LEADER_ACTION, "CANCELLED", 0, 2, null, null)),
                 executorState.getJsonStructure(false).get("executionPhases"));
  }
}
