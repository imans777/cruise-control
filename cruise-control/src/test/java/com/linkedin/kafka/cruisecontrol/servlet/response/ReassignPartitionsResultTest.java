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
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.GoalImpact;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.PartitionReassignmentDetails;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentAction;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentImpact;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ReassignPartitionsResultTest {
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  private static final String WARNING = "The new replicas are not rack-aware: they share racks {r0=[0, 3]}.";

  private static List<PartitionReassignmentDetails> partitions() {
    return List.of(new PartitionReassignmentDetails(new TopicPartition("t", 0), List.of(0, 1, 2), 0, List.of("/a", "/a", "/b"),
                                                    List.of(3, 1, 0), 3, List.of("any", "/a", "/a"),
                                                    EnumSet.of(ReassignmentAction.REPLICA_SET_CHANGE, ReassignmentAction.LEADERSHIP_MOVEMENT),
                                                    10L, 1, 0, List.of(WARNING)),
                   new PartitionReassignmentDetails(new TopicPartition("t", 1), List.of(1, 2), 1, List.of("/a", "unknown"),
                                                    List.of(1, 2), 1, List.of("/b", "unknown"),
                                                    EnumSet.of(ReassignmentAction.INTRA_BROKER_REPLICA_MOVEMENT), 5L, 0, 1,
                                                    Collections.emptyList()),
                   new PartitionReassignmentDetails(new TopicPartition("t", 2), List.of(2, 0), 2, List.of("/a", "/a"),
                                                    List.of(2, 0), 2, List.of("/a", "/a"), EnumSet.noneOf(ReassignmentAction.class), 7L, 0,
                                                    0, Collections.emptyList()));
  }

  private static ReassignmentImpact completedImpact() {
    ClusterModel clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);
    BalancingConstraint balancingConstraint = new BalancingConstraint(CONFIG);
    return ReassignmentImpact.completed(List.of("A note."), List.of("RackAwareGoal", "ReplicaDistributionGoal"), 90.0, 80.0,
                                        List.of(new GoalImpact("RackAwareGoal", true, false, true),
                                                new GoalImpact("ReplicaDistributionGoal", false, true, true)),
                                        clusterModel.getClusterStats(balancingConstraint), clusterModel.getClusterStats(balancingConstraint),
                                        clusterModel.brokerStats(CONFIG), clusterModel.brokerStats(CONFIG), List.of(new TopicPartition("t", 3)));
  }

  private static String response(ReassignPartitionsResult result, boolean json) {
    CruiseControlParameters parameters = EasyMock.createNiceMock(CruiseControlParameters.class);
    EasyMock.expect(parameters.json()).andStubReturn(json);
    EasyMock.replay(parameters);
    result.discardIrrelevantResponse(parameters);
    return result.cachedResponse();
  }

  @Test
  public void testSummary() {
    ReassignPartitionsResult result = new ReassignPartitionsResult(partitions(), ReassignmentImpact.unavailable("not ready"), true, false,
                                                                   CONFIG);
    ReassignPartitionsResult.ReassignPartitionsSummary summary = result.summary();
    assertEquals(3, summary.numPartitionsRequested());
    assertEquals(1, summary.numPartitionsUnchanged());
    assertEquals(1, summary.numInterBrokerReplicaMovements());
    assertEquals(0, summary.numReplicaOrderChanges());
    assertEquals(1, summary.numIntraBrokerReplicaMovements());
    assertEquals(1, summary.numLeaderMovements());
    assertEquals(0, summary.numReplicationFactorChanges());
    assertEquals(10L, summary.interBrokerDataToMoveInMB());
    assertEquals(5L, summary.intraBrokerDataToMoveInMB());
    assertEquals(EnumSet.of(ReassignmentAction.NO_CHANGE), result.partitions().get(2).actions());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testJsonWithUnavailableImpact() {
    String json = response(new ReassignPartitionsResult(partitions(), ReassignmentImpact.unavailable("not ready"), true, false, CONFIG),
                           true);
    Map<String, Object> response = new Gson().fromJson(json, Map.class);
    assertEquals(Boolean.TRUE, response.get("dryRun"));
    assertEquals(Boolean.FALSE, response.get("executionStarted"));
    assertEquals(1.0, response.get("version"));
    assertEquals(9, ((Map<String, Object>) response.get("summary")).size());
    List<Map<String, Object>> partitions = (List<Map<String, Object>>) response.get("partitions");
    assertEquals(3, partitions.size());
    Map<String, Object> first = partitions.get(0);
    assertEquals("t", first.get("topic"));
    assertEquals(0.0, first.get("partition"));
    assertEquals(List.of(0.0, 1.0, 2.0), first.get("currentReplicas"));
    assertEquals(List.of(3.0, 1.0, 0.0), first.get("newReplicas"));
    assertEquals(3.0, first.get("newLeader"));
    assertEquals(List.of("any", "/a", "/a"), first.get("newLogDirs"));
    assertEquals(List.of("REPLICA_SET_CHANGE", "LEADERSHIP_MOVEMENT"), first.get("actions"));
    assertEquals(10.0, first.get("dataToMoveMB"));
    assertEquals(List.of(WARNING), first.get("warnings"));
    // Warnings are omitted if there are none.
    assertFalse(partitions.get(1).containsKey("warnings"));
    assertEquals(5.0, partitions.get(1).get("dataToMoveMB"));
    assertEquals(List.of("NO_CHANGE"), partitions.get(2).get("actions"));
    assertEquals(Map.of("status", "UNAVAILABLE", "reason", "not ready"), response.get("impactAnalysis"));
    // The response has no nulls, hence its schema can be generated.
    assertFalse(json.contains("null"));
    ResponseUtils.getJsonSchema(json);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testJsonWithCompletedImpact() {
    String json = response(new ReassignPartitionsResult(partitions(), completedImpact(), false, true, CONFIG), true);
    Map<String, Object> impact = (Map<String, Object>) new Gson().fromJson(json, Map.class).get("impactAnalysis");
    assertEquals("COMPLETED", impact.get("status"));
    assertFalse(impact.containsKey("reason"));
    assertEquals(List.of("A note."), impact.get("notes"));
    assertEquals(List.of("RackAwareGoal", "ReplicaDistributionGoal"), impact.get("goals"));
    assertEquals(90.0, impact.get("onDemandBalancednessScoreBefore"));
    assertEquals(80.0, impact.get("onDemandBalancednessScoreAfter"));
    List<Map<String, Object>> goalSummary = (List<Map<String, Object>>) impact.get("goalSummary");
    assertEquals(Map.of("goal", "RackAwareGoal", "hardGoal", true, "violatedBefore", false, "violatedAfter", true,
                        "status", "VIOLATION_INTRODUCED"), goalSummary.get(0));
    assertEquals("STILL_VIOLATED", goalSummary.get(1).get("status"));
    assertTrue(impact.containsKey("clusterModelStatsBefore"));
    assertTrue(impact.containsKey("clusterModelStatsAfter"));
    assertTrue(((Map<String, Object>) impact.get("loadBeforeReassignment")).containsKey("brokers"));
    assertTrue(((Map<String, Object>) impact.get("loadAfterReassignment")).containsKey("brokers"));
    assertEquals(List.of("t-3"), impact.get("partitionsNotModeled"));
    assertFalse(json.contains("null"));
    ResponseUtils.getJsonSchema(json);
  }

  @Test
  public void testPlaintext() {
    String nl = System.lineSeparator();
    String plaintext = response(new ReassignPartitionsResult(partitions(), ReassignmentImpact.unavailable("not ready"), true, false,
                                                             CONFIG), false);
    assertTrue(plaintext, plaintext.startsWith("Dry run of the partition reassignment (nothing is executed, use dryrun=false to execute)."
                                               + nl + "Partitions: 3 requested, 1 unchanged."));
    List<String> lines = List.of(plaintext.split(nl));
    String header = lines.stream().filter(line -> line.startsWith("PARTITION")).findFirst().orElseThrow();
    String row = lines.stream().filter(line -> line.startsWith("t-0")).findFirst().orElseThrow();
    assertEquals("PARTITION CURRENT_REPLICAS CURRENT_LEADER NEW_REPLICAS NEW_LEADER NEW_LOG_DIRS ACTIONS", squeeze(header));
    assertEquals("t-0 [0, 1, 2] 0 [3, 1, 0] 3 [any, /a, /a] REPLICA_SET_CHANGE,LEADERSHIP_MOVEMENT", squeeze(row));
    // Columns are aligned.
    assertEquals(header.indexOf("NEW_LOG_DIRS"), row.indexOf("[any"));
    assertEquals(header.indexOf("ACTIONS"), row.indexOf("REPLICA_SET_CHANGE"));
    assertTrue(plaintext, plaintext.contains(nl + "WARNING (t-0): " + WARNING + nl));
    assertTrue(plaintext, plaintext.endsWith(nl + "Impact analysis: UNAVAILABLE (not ready)" + nl));

    plaintext = response(new ReassignPartitionsResult(partitions(), completedImpact(), false, true, CONFIG), false);
    assertTrue(plaintext, plaintext.startsWith("Started executing the partition reassignment"));
    assertTrue(plaintext, plaintext.contains(nl + "Impact analysis: COMPLETED" + nl + "NOTE: A note." + nl));
    assertTrue(plaintext, plaintext.contains(nl + "Partitions not applied to the load model (the model is out of date): [t-3]" + nl));
    assertTrue(plaintext, plaintext.contains("On-demand balancedness score before: 90.000, after: 80.000."));
    assertTrue(plaintext, squeeze(plaintext).contains(nl + "GOAL HARD VIOLATED_BEFORE VIOLATED_AFTER STATUS" + nl
                                                      + "RackAwareGoal true false true VIOLATION_INTRODUCED" + nl
                                                      + "ReplicaDistributionGoal false true true STILL_VIOLATED" + nl));
    assertTrue(plaintext, plaintext.contains(nl + "Cluster load before the reassignment:" + nl));
    assertTrue(plaintext, plaintext.contains(nl + "Cluster load after the reassignment:" + nl));

    plaintext = response(new ReassignPartitionsResult(partitions().subList(2, 3), ReassignmentImpact.notNeeded(), false, false, CONFIG),
                         false);
    assertTrue(plaintext, plaintext.startsWith("Nothing to execute: all requested partitions already have the requested assignment."));
  }

  private static String squeeze(String text) {
    return text.replaceAll(" {2,}", " ");
  }
}
