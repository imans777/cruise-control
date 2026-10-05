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

package com.linkedin.kafka.cruisecontrol;

import com.jayway.jsonpath.JsonPath;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.HttpResult;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaTestUtils;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlIntegrationTestUtils.KAFKA_CRUISE_CONTROL_BASE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


/**
 * Manual partition reassignments via the reassign_partitions endpoint on a cluster whose brokers have two log directories each.
 * The load monitor has no metric samples, hence the impact of reassignments on the cluster cannot be analyzed -- i.e. executions
 * require skip_hard_goal_check=true.
 */
@RunWith(Parameterized.class)
public class ReassignPartitionsIntegrationTest extends CruiseControlIntegrationTestHarness {
  private static final String TOPIC = "reassign-partitions-test";
  // Partitions on broker 1 only, to make one log directory of broker 1 host more replicas than the other.
  private static final String FILLER_TOPIC = "filler";
  private static final int NUM_FILLER_PARTITIONS = 20;
  private static final int KAFKA_CLUSTER_SIZE = 3;
  private static final String ENDPOINT = KAFKA_CRUISE_CONTROL_BASE_PATH + "reassign_partitions";
  private static final String EXECUTOR_STATE_ENDPOINT = KAFKA_CRUISE_CONTROL_BASE_PATH + "state?substates=executor&json=true";
  private static final String EXECUTE = "json=true&dryrun=false&skip_hard_goal_check=true";
  private static final Duration TIMEOUT = Duration.ofSeconds(120);
  private static final Duration BACKOFF = Duration.ofSeconds(1);
  private static final Duration STATE_BACKOFF = Duration.ofSeconds(UserTaskManager.USER_TASK_SCANNER_PERIOD_SECONDS);
  private final Boolean _vertxEnabled;
  private AdminClient _adminClient;

  public ReassignPartitionsIntegrationTest(Boolean vertxEnabled) {
    _vertxEnabled = vertxEnabled;
  }

  /**
   * Sets different parameters for test runs.
   * @return Parameters for the test runs.
   */
  @Parameterized.Parameters
  public static Collection<Boolean> data() {
    Boolean[] data = {true, false};
    return Arrays.asList(data);
  }

  /**
   * Start the cluster and Cruise Control, create the test topic with partitions [0, 1], [1, 2] and [2, 0], and move the replicas of
   * the filler topic to the same log directory of broker 1 -- so that broker 1 places new replicas on its other log directory.
   */
  @Before
  public void setup() throws Exception {
    super.start();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                                                                      broker(0).plaintextAddr()));
    Map<Integer, List<Integer>> fillerAssignment = IntStream.range(0, NUM_FILLER_PARTITIONS).boxed()
                                                            .collect(Collectors.toMap(partition -> partition, partition -> List.of(1)));
    _adminClient.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(0, 1), 1, List.of(1, 2), 2, List.of(2, 0))),
                                      new NewTopic(FILLER_TOPIC, fillerAssignment))).all().get();
    waitUntil(() -> IntStream.range(0, 3).allMatch(partition -> leader(new TopicPartition(TOPIC, partition)) >= 0)
                    && IntStream.range(0, NUM_FILLER_PARTITIONS).allMatch(partition -> leader(new TopicPartition(FILLER_TOPIC, partition)) == 1),
              "The partitions have no leader.");
    KafkaCruiseControlIntegrationTestUtils.produceRandomDataToTopic(TOPIC, 20, 1000, KafkaCruiseControlIntegrationTestUtils
        .getDefaultProducerProperties(bootstrapServers()));

    String fillerLogDir = logDirs(1).get(0);
    _adminClient.alterReplicaLogDirs(IntStream.range(0, NUM_FILLER_PARTITIONS).boxed().collect(Collectors.toMap(
        partition -> new TopicPartitionReplica(FILLER_TOPIC, partition, 1), partition -> fillerLogDir))).all().get();
    waitUntil(() -> IntStream.range(0, NUM_FILLER_PARTITIONS).allMatch(
        partition -> fillerLogDir.equals(logDir(new TopicPartition(FILLER_TOPIC, partition), 1))), "The filler replicas were not moved.");
  }

  /**
   * Stop Cruise Control and the cluster.
   */
  @After
  public void teardown() {
    if (_adminClient != null) {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    }
    super.stop();
  }

  @Override
  protected int clusterSize() {
    return KAFKA_CLUSTER_SIZE;
  }

  @Override
  public Map<Object, Object> overridingProps() {
    return Map.of(ServerLogConfigs.LOG_DIR_CONFIG, CCKafkaTestUtils.newTempDir().getAbsolutePath() + ","
                                                   + CCKafkaTestUtils.newTempDir().getAbsolutePath());
  }

  @Override
  protected Map<String, Object> withConfigs() {
    return Map.of(WebServerConfig.VERTX_ENABLED_CONFIG, String.valueOf(_vertxEnabled),
                  ExecutorConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG, "5000");
  }

  @Test
  public void testReassignPartitions() throws Exception {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    TopicPartition tp2 = new TopicPartition(TOPIC, 2);
    String promoteFollower = body(entry(0, "\"leader\": 1"));

    // A dry run shows the plan, but changes nothing.
    HttpResult result = post("json=true", promoteFollower, Collections.emptyMap());
    assertEquals(result.toString(), 200, result.statusCode());
    assertEquals(true, JsonPath.read(result.body(), "$.dryRun"));
    assertEquals(false, JsonPath.read(result.body(), "$.executionStarted"));
    assertEquals(List.of(1, 0), JsonPath.read(result.body(), "$.partitions[0].newReplicas"));
    assertEquals(List.of("REPLICA_ORDER_CHANGE", "LEADERSHIP_MOVEMENT"), JsonPath.read(result.body(), "$.partitions[0].actions"));
    assertEquals("UNAVAILABLE", JsonPath.read(result.body(), "$.impactAnalysis.status"));
    assertEquals(List.of(0, 1), replicas(tp0));

    // An execution whose impact on hard goals cannot be verified is refused.
    result = post("json=true&dryrun=false", promoteFollower, Collections.emptyMap());
    assertEquals(result.toString(), 400, result.statusCode());
    assertTrue(result.body(), JsonPath.<String>read(result.body(), "$.errorMessage").contains("Cannot verify the impact"));

    // Promote the follower of partition 0.
    result = post(EXECUTE, promoteFollower, Collections.emptyMap());
    assertEquals(result.toString(), 200, result.statusCode());
    assertEquals(true, JsonPath.read(result.body(), "$.executionStarted"));
    waitUntil(() -> replicas(tp0).equals(List.of(1, 0)) && leader(tp0) == 1, "The follower of " + tp0 + " was not promoted.");
    waitForExecutionToFinish();

    // Move the replica of partition 1 on broker 2 to the other log directory of broker 2.
    List<String> logDirsOfBroker2 = logDirs(2);
    String originalLogDir = logDir(tp1, 2);
    String otherLogDir = logDirsOfBroker2.stream().filter(logDir -> !logDir.equals(originalLogDir)).findFirst().orElseThrow();
    result = post(EXECUTE, body(entry(1, String.format("\"log_dirs\": {\"2\": \"%s\"}", otherLogDir))), Collections.emptyMap());
    assertEquals(result.toString(), 200, result.statusCode());
    assertEquals(List.of("INTRA_BROKER_REPLICA_MOVEMENT"), JsonPath.read(result.body(), "$.partitions[0].actions"));
    waitUntil(() -> otherLogDir.equals(logDir(tp1, 2)), "The replica of " + tp1 + " on broker 2 was not moved to " + otherLogDir);
    waitForExecutionToFinish();
    assertEquals(List.of(1, 2), replicas(tp1));

    // Move the replica of partition 2 from broker 0 to the log directory of broker 1 that hosts more replicas -- i.e. not the one
    // that broker 1 would pick by itself.
    String targetLogDir = logDirWithMostReplicas(1);
    result = post(EXECUTE, body(entry(2, String.format("\"replicas\": [2, 1], \"log_dirs\": [\"any\", \"%s\"]", targetLogDir))),
                  Collections.emptyMap());
    assertEquals(result.toString(), 200, result.statusCode());
    assertEquals(List.of("REPLICA_SET_CHANGE"), JsonPath.read(result.body(), "$.partitions[0].actions"));
    waitUntil(() -> replicas(tp2).equals(List.of(2, 1)), "The replica of " + tp2 + " was not moved to broker 1.");
    waitForExecutionToFinish();
    assertEquals(targetLogDir, logDir(tp2, 1));

    // Moving existing replicas between log directories cannot be combined with replica order changes -- all problems are reported.
    result = post("json=true", body(entry(0, "\"leader\": 0"), entry(1, String.format("\"log_dirs\": {\"2\": \"%s\"}", originalLogDir))),
                  Collections.emptyMap());
    assertEquals(result.toString(), 400, result.statusCode());
    String errorMessage = JsonPath.read(result.body(), "$.errorMessage");
    assertTrue(errorMessage, errorMessage.contains(String.format("Submit the replica set / order changes first [%s], and once that "
                                                                 + "execution finishes, submit the log directory moves [%s].", tp0, tp1)));

    // Requests with the same URL in the same session are distinct user tasks if their bodies differ.
    result = post("json=true", body(entry(0, "\"leader\": 0")), Collections.emptyMap());
    assertEquals(result.toString(), 200, result.statusCode());
    String cookie = result.header("Set-Cookie");
    // The servlet always maintains a session, whereas the session of Vert.x may not be sent to the client.
    assertTrue(result.toString(), _vertxEnabled || cookie != null);
    Map<String, String> sessionHeaders = cookie == null ? Collections.emptyMap() : Map.of("Cookie", cookie.split(";")[0]);
    HttpResult secondResult = post("json=true", body(entry(1, "\"leader\": 2")), sessionHeaders);
    assertEquals(secondResult.toString(), 200, secondResult.statusCode());
    assertNotEquals(result.header(UserTaskManager.USER_TASK_HEADER_NAME), secondResult.header(UserTaskManager.USER_TASK_HEADER_NAME));
    Integer partition = JsonPath.read(secondResult.body(), "$.partitions[0].partition");
    assertEquals(Integer.valueOf(1), partition);
  }

  private static String entry(int partition, String reassignment) {
    return String.format("{\"topic\": \"%s\", \"partition\": %d, %s}", TOPIC, partition, reassignment);
  }

  private static String body(String... entries) {
    return String.format("{\"version\": 1, \"partitions\": [%s]}", String.join(", ", entries));
  }

  /**
   * Send the given request to the reassign_partitions endpoint, and poll its user task until it finishes.
   *
   * @param query Query parameters of the request.
   * @param body JSON body of the request.
   * @param headers Headers of the request.
   * @return The response to the request.
   */
  private HttpResult post(String query, String body, Map<String, String> headers) throws InterruptedException {
    HttpResult result = KafkaCruiseControlIntegrationTestUtils.callCruiseControlPost(_app.serverUrl(), ENDPOINT + "?" + query, body,
                                                                                    headers);
    long deadlineMs = System.currentTimeMillis() + TIMEOUT.toMillis();
    while (result.statusCode() == 202 && System.currentTimeMillis() < deadlineMs) {
      Thread.sleep(BACKOFF.toMillis());
      Map<String, String> pollHeaders = new HashMap<>(headers);
      pollHeaders.put(UserTaskManager.USER_TASK_HEADER_NAME, result.header(UserTaskManager.USER_TASK_HEADER_NAME));
      result = KafkaCruiseControlIntegrationTestUtils.callCruiseControlPost(_app.serverUrl(), ENDPOINT + "?" + query, null, pollHeaders);
    }
    return result;
  }

  private static void waitUntil(BooleanSupplier condition, String message) {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(condition, TIMEOUT, BACKOFF, new AssertionError(message));
  }

  private void waitForExecutionToFinish() {
    // Each request to the state endpoint is a user task, which counts towards max.active.user.tasks until the next scan of user tasks.
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> "NO_TASK_IN_PROGRESS".equals(JsonPath.read(
        KafkaCruiseControlIntegrationTestUtils.callCruiseControl(_app.serverUrl(), EXECUTOR_STATE_ENDPOINT), "$.ExecutorState.state")),
                                                                TIMEOUT, STATE_BACKOFF, new AssertionError("The execution did not finish."));
  }

  private TopicPartitionInfo partitionInfo(TopicPartition tp) {
    try {
      TopicDescription description = _adminClient.describeTopics(List.of(tp.topic())).allTopicNames().get().get(tp.topic());
      return description.partitions().get(tp.partition());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private List<Integer> replicas(TopicPartition tp) {
    return partitionInfo(tp).replicas().stream().map(Node::id).collect(Collectors.toList());
  }

  private int leader(TopicPartition tp) {
    Node leader = partitionInfo(tp).leader();
    return leader == null ? -1 : leader.id();
  }

  private Map<String, LogDirDescription> logDirDescriptions(int broker) {
    try {
      return new TreeMap<>(_adminClient.describeLogDirs(List.of(broker)).allDescriptions().get().get(broker));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private List<String> logDirs(int broker) {
    return List.copyOf(logDirDescriptions(broker).keySet());
  }

  /**
   * @param tp Topic partition.
   * @param broker Broker.
   * @return The log directory of the current replica of the given partition on the given broker, or {@code null} if the broker has
   * no such replica -- or the replica is being moved between log directories.
   */
  private String logDir(TopicPartition tp, int broker) {
    String currentLogDir = null;
    for (Map.Entry<String, LogDirDescription> entry : logDirDescriptions(broker).entrySet()) {
      ReplicaInfo replicaInfo = entry.getValue().replicaInfos().get(tp);
      if (replicaInfo != null) {
        if (replicaInfo.isFuture()) {
          return null;
        }
        currentLogDir = entry.getKey();
      }
    }
    return currentLogDir;
  }

  private String logDirWithMostReplicas(int broker) {
    return logDirDescriptions(broker).entrySet().stream().max(Map.Entry.comparingByValue(
        (a, b) -> Integer.compare(a.replicaInfos().size(), b.replicaInfos().size()))).orElseThrow().getKey();
  }
}
