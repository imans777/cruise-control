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

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_CAPACITY_ESTIMATION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_LEADER_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.GOALS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICATION_THROTTLE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICA_MOVEMENT_STRATEGIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ReassignPartitionsParametersTest {
  private static final String ENDPOINT = CruiseControlEndPoint.REASSIGN_PARTITIONS.toString();
  private static final String BODY = "{\"partitions\": [{\"topic\": \"t\", \"partition\": 0, \"leader\": 1},"
                                     + "{\"topic\": \"t\", \"partition\": 1, \"replicas\": [2, 1]}]}";
  private static final String USER_TASK_ID = "9e1d8a27-8e60-4c5a-9a9b-6c1f1f0f6e3b";

  @Test
  public void testDefaults() {
    StubRequestContext request = new StubRequestContext("POST", ENDPOINT, Collections.emptyMap(), BODY, null);
    ReassignPartitionsParameters parameters = new ReassignPartitionsParameters();
    assertFalse(request.errorMessages().toString(), request.parse(parameters));

    assertEquals(CruiseControlEndPoint.REASSIGN_PARTITIONS, parameters.endPoint());
    assertTrue(parameters.dryRun());
    // The reason is annotated with the client and date of the request.
    assertTrue(parameters.reason(), parameters.reason().startsWith("No reason provided (Client: "));
    assertNull(parameters.reviewId());
    assertFalse(parameters.stopOngoingExecution());
    assertTrue(parameters.goals().isEmpty());
    assertFalse(parameters.skipHardGoalCheck());
    assertTrue(parameters.allowCapacityEstimation());
    assertNull(parameters.concurrentInterBrokerPartitionMovements());
    assertNull(parameters.maxInterBrokerPartitionMovements());
    assertNull(parameters.concurrentIntraBrokerPartitionMovements());
    assertNull(parameters.clusterLeaderMovementConcurrency());
    assertNull(parameters.brokerLeaderMovementConcurrency());
    assertNull(parameters.executionProgressCheckIntervalMs());
    assertNull(parameters.replicationThrottle());
    List<RequestedPartitionReassignment> requested = parameters.requestedReassignments();
    assertEquals(2, requested.size());
    assertEquals(new TopicPartition("t", 0), requested.get(0).topicPartition());
    assertEquals(Integer.valueOf(1), requested.get(0).leader());
    assertEquals(List.of(2, 1), requested.get(1).replicas());
  }

  @Test
  public void testQueryParameters() {
    Map<String, String> queryParameters = Map.ofEntries(Map.entry(DRY_RUN_PARAM, "false"),
                                                        Map.entry(REASON_PARAM, "hot disk"),
                                                        Map.entry(STOP_ONGOING_EXECUTION_PARAM, "true"),
                                                        Map.entry(GOALS_PARAM, "RackAwareGoal,ReplicaCapacityGoal"),
                                                        Map.entry(SKIP_HARD_GOAL_CHECK_PARAM, "true"),
                                                        Map.entry(ALLOW_CAPACITY_ESTIMATION_PARAM, "false"),
                                                        Map.entry(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM, "3"),
                                                        Map.entry(MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM, "7"),
                                                        Map.entry(CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM, "2"),
                                                        Map.entry(CONCURRENT_LEADER_MOVEMENTS_PARAM, "100"),
                                                        Map.entry(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM, "10"),
                                                        Map.entry(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM, "6000"),
                                                        Map.entry(REPLICA_MOVEMENT_STRATEGIES_PARAM,
                                                                  PostponeUrpReplicaMovementStrategy.class.getSimpleName()),
                                                        Map.entry(REPLICATION_THROTTLE_PARAM, "1000"));
    StubRequestContext request = new StubRequestContext("POST", ENDPOINT, queryParameters, BODY, null);
    ReassignPartitionsParameters parameters = new ReassignPartitionsParameters();
    assertFalse(request.errorMessages().toString(), request.parse(parameters));

    assertFalse(parameters.dryRun());
    assertTrue(parameters.reason(), parameters.reason().startsWith("hot disk (Client: "));
    assertTrue(parameters.stopOngoingExecution());
    assertEquals(List.of("RackAwareGoal", "ReplicaCapacityGoal"), parameters.goals());
    assertTrue(parameters.skipHardGoalCheck());
    assertFalse(parameters.allowCapacityEstimation());
    assertEquals(Integer.valueOf(3), parameters.concurrentInterBrokerPartitionMovements());
    assertEquals(Integer.valueOf(7), parameters.maxInterBrokerPartitionMovements());
    assertEquals(Integer.valueOf(2), parameters.concurrentIntraBrokerPartitionMovements());
    assertEquals(Integer.valueOf(100), parameters.clusterLeaderMovementConcurrency());
    assertEquals(Integer.valueOf(10), parameters.brokerLeaderMovementConcurrency());
    assertEquals(Long.valueOf(6000L), parameters.executionProgressCheckIntervalMs());
    assertTrue(parameters.replicaMovementStrategy().name().contains(PostponeUrpReplicaMovementStrategy.class.getSimpleName()));
    assertEquals(Long.valueOf(1000L), parameters.replicationThrottle());
    assertEquals(2, parameters.requestedReassignments().size());
  }

  @Test
  public void testPollingWithoutBody() {
    StubRequestContext request = new StubRequestContext("POST", ENDPOINT, Map.of(DRY_RUN_PARAM, "false"), null, USER_TASK_ID);
    ReassignPartitionsParameters parameters = new ReassignPartitionsParameters();
    assertFalse(request.errorMessages().toString(), request.parse(parameters));
    assertNull(parameters.requestedReassignments());
  }

  @Test
  public void testParseFailures() {
    assertParseFailure(Collections.emptyMap(), null, "Missing request body.");
    assertParseFailure(Collections.emptyMap(), "{\"partitions\": [{\"topic\": \"t\", \"partition\": 0}]}",
                       "Invalid partition reassignment request (1 problem):\n- t-0: specify one of 'replicas', 'leader' or 'log_dirs'.");
    assertParseFailure(Map.of(STOP_ONGOING_EXECUTION_PARAM, "true"), BODY,
                       String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
  }

  private static void assertParseFailure(Map<String, String> queryParameters, String body, String expectedMessagePrefix) {
    StubRequestContext request = new StubRequestContext("POST", ENDPOINT, queryParameters, body, null);
    assertTrue(request.parse(new ReassignPartitionsParameters()));
    assertEquals(List.of(400), request.errorCodes());
    String message = request.errorMessages().get(0);
    assertTrue(message, message.startsWith(expectedMessagePrefix));
  }
}
