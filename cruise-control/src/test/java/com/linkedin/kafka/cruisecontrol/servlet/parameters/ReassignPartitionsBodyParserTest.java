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

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class ReassignPartitionsBodyParserTest {
  private static final String HEADER = "Invalid partition reassignment request";

  /**
   * Parse the given JSON the same way the request contexts do -- i.e. numbers are parsed into doubles.
   *
   * @param json JSON to parse.
   * @return The parsed JSON object.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> json(String json) {
    return new Gson().fromJson(json, Map.class);
  }

  private static String problemsOf(String body) {
    return assertThrows(UserRequestException.class, () -> ReassignPartitionsBodyParser.parse(json(body))).getMessage();
  }

  @Test
  public void testParseAllForms() {
    List<RequestedPartitionReassignment> requested = ReassignPartitionsBodyParser.parse(json(
        "{\"version\": 1, \"partitions\": ["
        + "{\"topic\": \"orders\", \"partition\": 3, \"leader\": 102},"
        + "{\"topic\": \"orders\", \"partition\": 7, \"log_dirs\": {\"101\": \" /data/d2 \", \"103\": \"/data/d1\"}},"
        + "{\"topic\": \"events\", \"partition\": 0, \"replicas\": [104, 105, 106], \"log_dirs\": [\"any\", \"ANY\", \"/data/d3\"]},"
        + "{\"topic\": \"events\", \"partition\": 1, \"replicas\": [106, 104]}]}"));

    assertEquals(4, requested.size());
    RequestedPartitionReassignment leader = requested.get(0);
    assertEquals(new TopicPartition("orders", 3), leader.topicPartition());
    assertEquals(RequestedPartitionReassignment.Form.LEADER, leader.form());
    assertEquals(Integer.valueOf(102), leader.leader());
    assertNull(leader.replicas());
    assertNull(leader.logDirs());
    assertNull(leader.logDirByBroker());

    RequestedPartitionReassignment logDirs = requested.get(1);
    assertEquals(new TopicPartition("orders", 7), logDirs.topicPartition());
    assertEquals(RequestedPartitionReassignment.Form.LOG_DIRS, logDirs.form());
    assertEquals(Map.of(101, "/data/d2", 103, "/data/d1"), logDirs.logDirByBroker());
    // The order of the request is kept.
    assertEquals(Arrays.asList(101, 103), new ArrayList<>(logDirs.logDirByBroker().keySet()));
    assertNull(logDirs.replicas());
    assertNull(logDirs.leader());

    RequestedPartitionReassignment replicasWithLogDirs = requested.get(2);
    assertEquals(new TopicPartition("events", 0), replicasWithLogDirs.topicPartition());
    assertEquals(RequestedPartitionReassignment.Form.REPLICAS, replicasWithLogDirs.form());
    assertEquals(Arrays.asList(104, 105, 106), replicasWithLogDirs.replicas());
    assertEquals(Arrays.asList("any", "ANY", "/data/d3"), replicasWithLogDirs.logDirs());
    assertNull(replicasWithLogDirs.leader());

    RequestedPartitionReassignment replicas = requested.get(3);
    assertEquals(RequestedPartitionReassignment.Form.REPLICAS, replicas.form());
    assertEquals(Arrays.asList(106, 104), replicas.replicas());
    assertNull(replicas.logDirs());
  }

  @Test
  public void testVersionIsOptional() {
    List<RequestedPartitionReassignment> requested =
        ReassignPartitionsBodyParser.parse(json("{\"partitions\": [{\"topic\": \"t\", \"partition\": 0, \"leader\": 1}]}"));
    assertEquals(1, requested.size());
    assertEquals(new TopicPartition("t", 0), requested.get(0).topicPartition());
  }

  @Test
  public void testIntegralNumbersInAnyNotation() {
    List<RequestedPartitionReassignment> requested = ReassignPartitionsBodyParser.parse(
        json("{\"version\": 1.0, \"partitions\": [{\"topic\": \"t\", \"partition\": 2.0, \"replicas\": [1e0, 2]}]}"));
    assertEquals(new TopicPartition("t", 2), requested.get(0).topicPartition());
    assertEquals(Arrays.asList(1, 2), requested.get(0).replicas());
  }

  @Test
  public void testInvalidTopLevel() {
    String message = problemsOf("{\"version\": 2, \"partition\": [], \"partitions\": [{\"topic\": \"t\", \"partition\": 0, \"leader\": 1}]}");
    assertTrue(message, message.startsWith(HEADER + " (2 problems):"));
    assertTrue(message, message.contains("Unknown field 'partition'"));
    assertTrue(message, message.contains("Unsupported version 2.0 (supported: 1)."));

    message = problemsOf("{\"version\": 1}");
    assertTrue(message, message.contains("'partitions' must be a non-empty array of partition reassignments."));
    message = problemsOf("{\"partitions\": []}");
    assertTrue(message, message.contains("'partitions' must be a non-empty array of partition reassignments."));
    message = problemsOf("{\"partitions\": {\"topic\": \"t\", \"partition\": 0, \"leader\": 1}}");
    assertTrue(message, message.contains("'partitions' must be a non-empty array of partition reassignments."));
  }

  @Test
  public void testInvalidEntriesAreReportedTogether() {
    String message = problemsOf("{\"partitions\": ["
                                + "\"t-0\","
                                + "{\"topic\": \"t\", \"partition\": 1, \"leader\": 1, \"replica\": [1]},"
                                + "{\"topic\": \" \", \"partition\": 2, \"leader\": 1},"
                                + "{\"topic\": \"t\", \"partition\": -3, \"leader\": 1},"
                                + "{\"topic\": \"t\", \"partition\": 4.5, \"leader\": 1},"
                                + "{\"topic\": \"t\", \"partition\": 5, \"leader\": 1, \"replicas\": [1, 2]},"
                                + "{\"topic\": \"t\", \"partition\": 6, \"leader\": \"1\"},"
                                + "{\"topic\": \"t\", \"partition\": 7},"
                                + "{\"topic\": \"t\", \"partition\": 8, \"leader\": 1},"
                                + "{\"topic\": \"t\", \"partition\": 8, \"leader\": 2}]}");
    assertTrue(message, message.startsWith(HEADER + " (9 problems):"));
    assertTrue(message, message.contains("- partitions[0] must be a JSON object."));
    assertTrue(message, message.contains("- partitions[1]: unknown field 'replica'"));
    assertTrue(message, message.contains("- partitions[2]: 'topic' must be a non-blank string."));
    assertTrue(message, message.contains("- partitions[3]: 'partition' must be a non-negative integer (got -3.0)."));
    assertTrue(message, message.contains("- partitions[4]: 'partition' must be a non-negative integer (got 4.5)."));
    assertTrue(message, message.contains("- t-5: 'leader' cannot be combined with 'replicas' or 'log_dirs'."));
    assertTrue(message, message.contains("- t-6: 'leader' must be a non-negative broker id (got 1)."));
    assertTrue(message, message.contains("- t-7: specify one of 'replicas', 'leader' or 'log_dirs'."));
    assertTrue(message, message.contains("- Duplicate entries for t-8 (partitions[8] and partitions[9])."));
  }

  @Test
  public void testInvalidReplicas() {
    String message = problemsOf("{\"partitions\": ["
                                + "{\"topic\": \"t\", \"partition\": 0, \"replicas\": []},"
                                + "{\"topic\": \"t\", \"partition\": 1, \"replicas\": 1},"
                                + "{\"topic\": \"t\", \"partition\": 2, \"replicas\": [1, -2]},"
                                + "{\"topic\": \"t\", \"partition\": 3, \"replicas\": [1, 2, 1, 2]},"
                                + "{\"topic\": \"t\", \"partition\": 4, \"replicas\": [1, 9999999999]}]}");
    assertTrue(message, message.startsWith(HEADER + " (5 problems):"));
    assertTrue(message, message.contains("- t-0: 'replicas' must be a non-empty array of broker ids."));
    assertTrue(message, message.contains("- t-1: 'replicas' must be a non-empty array of broker ids."));
    assertTrue(message, message.contains("- t-2: 'replicas' must contain non-negative broker ids (got -2.0)."));
    assertTrue(message, message.contains("- t-3: 'replicas' contains duplicate broker ids [1, 2]."));
    assertTrue(message, message.contains("- t-4: 'replicas' must contain non-negative broker ids (got 9.999999999E9)."));
  }

  @Test
  public void testInvalidLogDirs() {
    String message = problemsOf("{\"partitions\": ["
                                + "{\"topic\": \"t\", \"partition\": 0, \"replicas\": [1, 2], \"log_dirs\": [\"any\"]},"
                                + "{\"topic\": \"t\", \"partition\": 1, \"replicas\": [1, 2], \"log_dirs\": [\"any\", \" \"]},"
                                + "{\"topic\": \"t\", \"partition\": 2, \"replicas\": [1, 2], \"log_dirs\": {\"1\": \"/d1\"}},"
                                + "{\"topic\": \"t\", \"partition\": 3, \"log_dirs\": [\"/d1\"]},"
                                + "{\"topic\": \"t\", \"partition\": 4, \"log_dirs\": {}},"
                                + "{\"topic\": \"t\", \"partition\": 5, \"log_dirs\": {\"b1\": \"/d1\", \"-1\": \"/d1\"}},"
                                + "{\"topic\": \"t\", \"partition\": 6, \"log_dirs\": {\"1\": \"any\", \"2\": 3, \"3\": \"\"}}]}");
    assertTrue(message, message.startsWith(HEADER + " (10 problems):"));
    assertTrue(message, message.contains("- t-0: 'log_dirs' has 1 entries but 'replicas' has 2."));
    assertTrue(message, message.contains("- t-1: 'log_dirs' must contain non-blank log directories or \"any\" (got  )."));
    assertTrue(message, message.contains("- t-2: 'log_dirs' must be an array with one log directory (or \"any\") per replica"));
    assertTrue(message, message.contains("- t-3: 'log_dirs' must be an object mapping broker id to log directory when 'replicas' is not"));
    assertTrue(message, message.contains("- t-4: 'log_dirs' must map at least one broker id to a log directory."));
    assertTrue(message, message.contains("- t-5: 'log_dirs' keys must be non-negative broker ids (got \"b1\")."));
    assertTrue(message, message.contains("- t-5: 'log_dirs' keys must be non-negative broker ids (got \"-1\")."));
    assertTrue(message, message.contains("- t-6: 'log_dirs' must map broker 1 to a log directory (got any)."));
    assertTrue(message, message.contains("- t-6: 'log_dirs' must map broker 2 to a log directory (got 3.0)."));
    assertTrue(message, message.contains("- t-6: 'log_dirs' must map broker 3 to a log directory (got )."));
  }

  @Test
  public void testNumberOfReportedProblemsIsCapped() {
    int numProblems = ReassignPartitionsBodyParser.MAX_REPORTED_PROBLEMS + 5;
    StringBuilder body = new StringBuilder("{\"partitions\": [");
    for (int i = 0; i < numProblems; i++) {
      body.append(i == 0 ? "" : ",").append("{\"topic\": \"t\", \"partition\": ").append(i).append('}');
    }
    String message = problemsOf(body.append("]}").toString());
    assertTrue(message, message.startsWith(String.format("%s (%d problems):", HEADER, numProblems)));
    assertEquals(ReassignPartitionsBodyParser.MAX_REPORTED_PROBLEMS + 1, message.split("\n- ").length - 1);
    assertTrue(message, message.endsWith("\n- ... and 5 more."));
  }

  @Test
  public void testSingleProblemMessage() {
    UserRequestException exception = ReassignPartitionsBodyParser.problems("Header", List.of("first"));
    assertEquals("Header (1 problem):\n- first", exception.getMessage());
  }
}
