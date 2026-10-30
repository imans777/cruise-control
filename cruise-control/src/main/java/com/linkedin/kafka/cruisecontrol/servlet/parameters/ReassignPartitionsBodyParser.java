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

import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.RequestedPartitionReassignment.ANY_LOG_DIR;


/**
 * Parses the JSON body of a manual partition reassignment request (see {@link ReassignPartitionsParameters}). The body uses the
 * format of kafka-reassign-partitions.sh, plus two shorthands that are expanded from the live cluster state upon execution:
 * <pre><code>
 * {
 *   "version": 1,
 *   "partitions": [
 *     {"topic": "t", "partition": 0, "replicas": [2, 1, 3], "log_dirs": ["any", "/data/d2", "any"]},
 *     {"topic": "t", "partition": 1, "leader": 2},
 *     {"topic": "t", "partition": 2, "log_dirs": {"1": "/data/d2"}}
 *   ]
 * }
 * </code></pre>
 *
 * All structural problems are collected and reported together in a single {@link UserRequestException}.
 */
public final class ReassignPartitionsBodyParser {
  public static final String VERSION = "version";
  public static final String PARTITIONS = "partitions";
  public static final String TOPIC = "topic";
  public static final String PARTITION = "partition";
  public static final String REPLICAS = "replicas";
  public static final String LOG_DIRS = "log_dirs";
  public static final String LEADER = "leader";
  public static final int SUPPORTED_VERSION = 1;
  public static final int MAX_REPORTED_PROBLEMS = 100;
  private static final Set<String> TOP_LEVEL_KEYS = Set.of(VERSION, PARTITIONS);
  private static final Set<String> ENTRY_KEYS = Set.of(TOPIC, PARTITION, REPLICAS, LOG_DIRS, LEADER);

  private ReassignPartitionsBodyParser() {
  }

  /**
   * Parse the given JSON body of a manual partition reassignment request.
   *
   * @param body The JSON body, as parsed by {@link com.linkedin.cruisecontrol.http.CruiseControlRequestContext#getJson()}.
   * @return The requested partition reassignments in the order of the request.
   */
  @SuppressWarnings("unchecked")
  public static List<RequestedPartitionReassignment> parse(Map<String, Object> body) {
    List<String> problems = new ArrayList<>();
    for (String key : body.keySet()) {
      if (!TOP_LEVEL_KEYS.contains(key)) {
        problems.add(String.format("Unknown field '%s' (supported: %s, %s).", key, VERSION, PARTITIONS));
      }
    }
    if (body.containsKey(VERSION)) {
      Integer version = toInteger(body.get(VERSION));
      if (version == null || version != SUPPORTED_VERSION) {
        problems.add(String.format("Unsupported %s %s (supported: %d).", VERSION, body.get(VERSION), SUPPORTED_VERSION));
      }
    }
    Object partitions = body.get(PARTITIONS);
    if (!(partitions instanceof List) || ((List<Object>) partitions).isEmpty()) {
      problems.add(String.format("'%s' must be a non-empty array of partition reassignments.", PARTITIONS));
      throw problems("Invalid partition reassignment request", problems);
    }

    List<RequestedPartitionReassignment> requested = new ArrayList<>();
    Map<TopicPartition, Integer> indexByTopicPartition = new HashMap<>();
    List<Object> entries = (List<Object>) partitions;
    for (int i = 0; i < entries.size(); i++) {
      String location = String.format("%s[%d]", PARTITIONS, i);
      if (!(entries.get(i) instanceof Map)) {
        problems.add(String.format("%s must be a JSON object.", location));
        continue;
      }
      RequestedPartitionReassignment reassignment = parseEntry((Map<String, Object>) entries.get(i), location, problems);
      if (reassignment != null) {
        Integer previousIndex = indexByTopicPartition.putIfAbsent(reassignment.topicPartition(), i);
        if (previousIndex != null) {
          problems.add(String.format("Duplicate entries for %s (%s[%d] and %s[%d]).", reassignment.topicPartition(), PARTITIONS,
                                     previousIndex, PARTITIONS, i));
        } else {
          requested.add(reassignment);
        }
      }
    }
    if (!problems.isEmpty()) {
      throw problems("Invalid partition reassignment request", problems);
    }
    return requested;
  }

  /**
   * Create an exception that reports all the given problems -- capped at {@link #MAX_REPORTED_PROBLEMS} -- under the given header.
   *
   * @param header Header of the message.
   * @param problems Problems to report.
   * @return A user request exception to report the given problems.
   */
  public static UserRequestException problems(String header, List<String> problems) {
    StringBuilder sb = new StringBuilder(String.format("%s (%d problem%s):", header, problems.size(), problems.size() == 1 ? "" : "s"));
    int numToReport = Math.min(problems.size(), MAX_REPORTED_PROBLEMS);
    for (int i = 0; i < numToReport; i++) {
      sb.append("\n- ").append(problems.get(i));
    }
    if (problems.size() > numToReport) {
      sb.append("\n- ... and ").append(problems.size() - numToReport).append(" more.");
    }
    return new UserRequestException(sb.toString());
  }

  @SuppressWarnings("unchecked")
  private static RequestedPartitionReassignment parseEntry(Map<String, Object> entry, String location, List<String> problems) {
    int numProblemsBefore = problems.size();
    for (String key : entry.keySet()) {
      if (!ENTRY_KEYS.contains(key)) {
        problems.add(String.format("%s: unknown field '%s' (supported: %s, %s, %s, %s, %s).", location, key, TOPIC, PARTITION, REPLICAS,
                                   LOG_DIRS, LEADER));
      }
    }
    Object topic = entry.get(TOPIC);
    if (!(topic instanceof String) || ((String) topic).trim().isEmpty()) {
      problems.add(String.format("%s: '%s' must be a non-blank string.", location, TOPIC));
    }
    Integer partition = toNonNegativeInteger(entry.get(PARTITION));
    if (partition == null) {
      problems.add(String.format("%s: '%s' must be a non-negative integer (got %s).", location, PARTITION, entry.get(PARTITION)));
    }
    if (problems.size() != numProblemsBefore) {
      return null;
    }
    TopicPartition tp = new TopicPartition((String) topic, partition);
    boolean hasReplicas = entry.containsKey(REPLICAS);
    boolean hasLogDirs = entry.containsKey(LOG_DIRS);
    if (entry.containsKey(LEADER)) {
      if (hasReplicas || hasLogDirs) {
        problems.add(String.format("%s: '%s' cannot be combined with '%s' or '%s'.", tp, LEADER, REPLICAS, LOG_DIRS));
        return null;
      }
      Integer leader = toNonNegativeInteger(entry.get(LEADER));
      if (leader == null) {
        problems.add(String.format("%s: '%s' must be a non-negative broker id (got %s).", tp, LEADER, entry.get(LEADER)));
        return null;
      }
      return RequestedPartitionReassignment.withLeader(tp, leader);
    }
    if (hasReplicas) {
      List<Integer> replicas = parseReplicas(entry.get(REPLICAS), tp, problems);
      List<String> logDirs = null;
      if (hasLogDirs) {
        if (entry.get(LOG_DIRS) instanceof List) {
          logDirs = parseLogDirList((List<Object>) entry.get(LOG_DIRS), tp, problems);
        } else {
          problems.add(String.format("%s: '%s' must be an array with one log directory (or \"%s\") per replica when '%s' is given.",
                                     tp, LOG_DIRS, ANY_LOG_DIR, REPLICAS));
        }
        if (replicas != null && logDirs != null && logDirs.size() != replicas.size()) {
          problems.add(String.format("%s: '%s' has %d entries but '%s' has %d.", tp, LOG_DIRS, logDirs.size(), REPLICAS, replicas.size()));
        }
      }
      return problems.size() == numProblemsBefore ? RequestedPartitionReassignment.withReplicas(tp, replicas, logDirs) : null;
    }
    if (hasLogDirs) {
      if (!(entry.get(LOG_DIRS) instanceof Map)) {
        problems.add(String.format("%s: '%s' must be an object mapping broker id to log directory when '%s' is not given.",
                                   tp, LOG_DIRS, REPLICAS));
        return null;
      }
      Map<Integer, String> logDirByBroker = parseLogDirMap((Map<String, Object>) entry.get(LOG_DIRS), tp, problems);
      return problems.size() == numProblemsBefore ? RequestedPartitionReassignment.withLogDirs(tp, logDirByBroker) : null;
    }
    problems.add(String.format("%s: specify one of '%s', '%s' or '%s'.", tp, REPLICAS, LEADER, LOG_DIRS));
    return null;
  }

  @SuppressWarnings("unchecked")
  private static List<Integer> parseReplicas(Object replicasObject, TopicPartition tp, List<String> problems) {
    if (!(replicasObject instanceof List) || ((List<Object>) replicasObject).isEmpty()) {
      problems.add(String.format("%s: '%s' must be a non-empty array of broker ids.", tp, REPLICAS));
      return null;
    }
    List<Integer> replicas = new ArrayList<>();
    Set<Integer> duplicates = new HashSet<>();
    for (Object replicaObject : (List<Object>) replicasObject) {
      Integer replica = toNonNegativeInteger(replicaObject);
      if (replica == null) {
        problems.add(String.format("%s: '%s' must contain non-negative broker ids (got %s).", tp, REPLICAS, replicaObject));
        return null;
      }
      if (replicas.contains(replica)) {
        duplicates.add(replica);
      }
      replicas.add(replica);
    }
    if (!duplicates.isEmpty()) {
      problems.add(String.format("%s: '%s' contains duplicate broker ids %s.", tp, REPLICAS, duplicates));
      return null;
    }
    return replicas;
  }

  private static List<String> parseLogDirList(List<Object> logDirObjects, TopicPartition tp, List<String> problems) {
    List<String> logDirs = new ArrayList<>(logDirObjects.size());
    for (Object logDirObject : logDirObjects) {
      if (!(logDirObject instanceof String) || ((String) logDirObject).trim().isEmpty()) {
        problems.add(String.format("%s: '%s' must contain non-blank log directories or \"%s\" (got %s).", tp, LOG_DIRS, ANY_LOG_DIR,
                                   logDirObject));
        return null;
      }
      logDirs.add(((String) logDirObject).trim());
    }
    return logDirs;
  }

  private static Map<Integer, String> parseLogDirMap(Map<String, Object> logDirObjectByBroker, TopicPartition tp, List<String> problems) {
    if (logDirObjectByBroker.isEmpty()) {
      problems.add(String.format("%s: '%s' must map at least one broker id to a log directory.", tp, LOG_DIRS));
      return null;
    }
    Map<Integer, String> logDirByBroker = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : logDirObjectByBroker.entrySet()) {
      Integer broker = parseBrokerId(entry.getKey());
      Object logDirObject = entry.getValue();
      if (broker == null) {
        problems.add(String.format("%s: '%s' keys must be non-negative broker ids (got \"%s\").", tp, LOG_DIRS, entry.getKey()));
      } else if (!(logDirObject instanceof String) || ((String) logDirObject).trim().isEmpty()
                 || ((String) logDirObject).trim().equalsIgnoreCase(ANY_LOG_DIR)) {
        problems.add(String.format("%s: '%s' must map broker %d to a log directory (got %s).", tp, LOG_DIRS, broker, logDirObject));
      } else {
        logDirByBroker.put(broker, ((String) logDirObject).trim());
      }
    }
    return logDirByBroker;
  }

  private static Integer parseBrokerId(String brokerId) {
    try {
      int id = Integer.parseInt(brokerId.trim());
      return id < 0 ? null : id;
    } catch (NumberFormatException nfe) {
      return null;
    }
  }

  /**
   * Gson parses JSON numbers into {@link Double}, hence accept any integral {@link Number} within the integer range.
   *
   * @param value Value to convert.
   * @return The integer value, or {@code null} if the given value is not an integral number within the integer range.
   */
  private static Integer toInteger(Object value) {
    if (!(value instanceof Number)) {
      return null;
    }
    double doubleValue = ((Number) value).doubleValue();
    if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue) || Double.compare(doubleValue, Math.rint(doubleValue)) != 0
        || doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
      return null;
    }
    return (int) doubleValue;
  }

  private static Integer toNonNegativeInteger(Object value) {
    Integer integer = toInteger(value);
    return integer == null || integer < 0 ? null : integer;
  }
}
