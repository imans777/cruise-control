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

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T1;
import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.T2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PartitionLoadStateTest {
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  // T1-0 [0, 2], T1-1 [1, 0], T2-0 [1, 2] -- the leader is first. The log directory of the follower of T1-1 is unknown.
  private static final Map<TopicPartition, Map<Integer, String>> LOG_DIR_BY_REPLICA =
      Map.of(new TopicPartition(T1, 0), Map.of(0, "/a", 2, "/b"), new TopicPartition(T1, 1), Map.of(1, "/a"));

  /**
   * @return Partitions T1-0 [0, 2], T1-1 [1, 0] and T2-0 [1, 2] -- the leader is first -- with load for all metrics of partition_load.
   */
  private static List<Partition> partitions() {
    ClusterModel clusterModel =
        DeterministicCluster.getHomogeneousCluster(DeterministicCluster.RACK_BY_BROKER, TestConstants.BROKER_CAPACITY, null);
    Map<TopicPartition, List<Integer>> replicasByPartition = Map.of(new TopicPartition(T1, 0), List.of(0, 2),
                                                                    new TopicPartition(T1, 1), List.of(1, 0),
                                                                    new TopicPartition(T2, 0), List.of(1, 2));
    replicasByPartition.forEach((tp, replicas) -> {
      for (int i = 0; i < replicas.size(); i++) {
        String rack = DeterministicCluster.RACK_BY_BROKER.get(replicas.get(i)).toString();
        clusterModel.createReplica(rack, replicas.get(i), tp, i, i == 0);
        AggregatedMetricValues load = KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues(1.0, 2.0, i == 0 ? 3.0 : 0.0, 4.0);
        MetricValues messageInRate = new MetricValues(1);
        messageInRate.set(0, 5.0);
        load.add(KafkaMetricDef.commonMetricDefId(KafkaMetricDef.MESSAGE_IN_RATE), messageInRate);
        clusterModel.setReplicaLoad(rack, replicas.get(i), tp, load, Collections.singletonList(1L));
      }
    });
    return clusterModel.getPartitionsByTopic().values().stream().flatMap(Collection::stream)
                       .sorted(Comparator.comparing(p -> p.topicPartition().toString())).collect(Collectors.toList());
  }

  private static String response(Map<TopicPartition, Map<Integer, String>> logDirByReplica, boolean json) {
    PartitionLoadState state = new PartitionLoadState(partitions(), false, false, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE,
                                                      null, 10, CONFIG, logDirByReplica);
    CruiseControlParameters parameters = EasyMock.createNiceMock(CruiseControlParameters.class);
    EasyMock.expect(parameters.json()).andStubReturn(json);
    EasyMock.replay(parameters);
    state.discardIrrelevantResponse(parameters);
    return state.cachedResponse();
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> records(String json) {
    return (List<Map<String, Object>>) new Gson().fromJson(json, Map.class).get("records");
  }

  @Test
  public void testJsonWithoutLogDirs() {
    List<Map<String, Object>> records = records(response(null, true));
    assertEquals(3, records.size());
    for (Map<String, Object> record : records) {
      assertFalse(record.containsKey("leaderLogdir"));
      assertFalse(record.containsKey("followerLogdirs"));
    }
  }

  @Test
  public void testJsonWithLogDirs() {
    List<Map<String, Object>> records = records(response(LOG_DIR_BY_REPLICA, true));
    assertEquals(3, records.size());
    Map<String, Map<String, Object>> recordByPartition = records.stream().collect(Collectors.toMap(
        r -> r.get("topic") + "-" + ((Double) r.get("partition")).intValue(), r -> r));
    assertEquals("/a", recordByPartition.get("T1-0").get("leaderLogdir"));
    assertEquals(List.of("/b"), recordByPartition.get("T1-0").get("followerLogdirs"));
    assertEquals("/a", recordByPartition.get("T1-1").get("leaderLogdir"));
    assertEquals(List.of("unknown"), recordByPartition.get("T1-1").get("followerLogdirs"));
    assertEquals("unknown", recordByPartition.get(T2 + "-0").get("leaderLogdir"));
    assertEquals(List.of("unknown"), recordByPartition.get(T2 + "-0").get("followerLogdirs"));
  }

  @Test
  public void testPlaintext() {
    String plaintext = response(null, false);
    assertFalse(plaintext, plaintext.contains("LOGDIR"));

    plaintext = response(LOG_DIR_BY_REPLICA, false);
    String[] lines = plaintext.split(System.lineSeparator());
    assertEquals(4, lines.length);
    assertTrue(lines[0], lines[0].endsWith(String.format("  %-30s%s", "LEADER_LOGDIR", "FOLLOWER_LOGDIRS")));
    String t10 = List.of(lines).stream().filter(line -> line.trim().startsWith("T1-0")).findFirst().orElseThrow();
    assertTrue(t10, t10.endsWith(String.format("  %-30s%s", "/a", "[/b]")));
  }
}
