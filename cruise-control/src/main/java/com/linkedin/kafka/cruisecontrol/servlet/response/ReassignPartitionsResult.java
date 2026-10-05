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
import com.google.gson.GsonBuilder;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;


/**
 * The response of {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#REASSIGN_PARTITIONS}: The per-partition
 * plan of a manual partition reassignment, its summary and the analysis of its impact on the cluster load and goals.
 */
@JsonResponseClass
public class ReassignPartitionsResult extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String DRY_RUN = "dryRun";
  @JsonResponseField
  protected static final String EXECUTION_STARTED = "executionStarted";
  @JsonResponseField
  protected static final String SUMMARY = "summary";
  @JsonResponseField
  protected static final String PARTITIONS = "partitions";
  @JsonResponseField
  protected static final String IMPACT_ANALYSIS = "impactAnalysis";
  private static final String NL = System.lineSeparator();
  protected final boolean _dryRun;
  protected final boolean _executionStarted;
  protected ReassignPartitionsSummary _summary;
  protected List<PartitionReassignmentDetails> _partitions;
  protected ReassignmentImpact _impactAnalysis;

  /**
   * @param partitions Details of each requested partition reassignment, in the order of the request.
   * @param impactAnalysis The analysis of the impact of the reassignment on the cluster load and goals.
   * @param dryRun {@code true} if the request is a dry run, {@code false} otherwise.
   * @param executionStarted {@code true} if the execution of the reassignment has been started, {@code false} otherwise.
   * @param config The configurations for Cruise Control.
   */
  public ReassignPartitionsResult(List<PartitionReassignmentDetails> partitions,
                                  ReassignmentImpact impactAnalysis,
                                  boolean dryRun,
                                  boolean executionStarted,
                                  KafkaCruiseControlConfig config) {
    super(config);
    _partitions = partitions;
    _summary = new ReassignPartitionsSummary(partitions);
    _impactAnalysis = impactAnalysis;
    _dryRun = dryRun;
    _executionStarted = executionStarted;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public boolean executionStarted() {
    return _executionStarted;
  }

  public ReassignPartitionsSummary summary() {
    return _summary;
  }

  public List<PartitionReassignmentDetails> partitions() {
    return Collections.unmodifiableList(_partitions);
  }

  public ReassignmentImpact impactAnalysis() {
    return _impactAnalysis;
  }

  protected String getJsonString() {
    Map<String, Object> jsonStructure = new HashMap<>();
    jsonStructure.put(DRY_RUN, _dryRun);
    jsonStructure.put(EXECUTION_STARTED, _executionStarted);
    jsonStructure.put(SUMMARY, _summary.getJsonStructure());
    jsonStructure.put(PARTITIONS, _partitions.stream().map(PartitionReassignmentDetails::getJsonStructure).collect(Collectors.toList()));
    jsonStructure.put(IMPACT_ANALYSIS, _impactAnalysis.getJsonStructure());
    jsonStructure.put(VERSION, JSON_VERSION);
    Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().create();
    return gson.toJson(jsonStructure);
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    if (_executionStarted) {
      sb.append("Started executing the partition reassignment (track the progress via the state endpoint).");
    } else if (_dryRun) {
      sb.append("Dry run of the partition reassignment (nothing is executed, use dryrun=false to execute).");
    } else {
      sb.append("Nothing to execute: all requested partitions already have the requested assignment.");
    }
    sb.append(NL).append(_summary).append(NL).append(NL);
    writePartitionTable(sb);
    sb.append(NL);
    _impactAnalysis.writePlaintext(sb);
    return sb.toString();
  }

  private void writePartitionTable(StringBuilder sb) {
    List<String[]> rows = new ArrayList<>();
    rows.add(new String[]{"PARTITION", "CURRENT_REPLICAS", "CURRENT_LEADER", "NEW_REPLICAS", "NEW_LEADER", "NEW_LOG_DIRS", "ACTIONS"});
    for (PartitionReassignmentDetails details : _partitions) {
      rows.add(new String[]{details.topicPartition().toString(), details.currentReplicas().toString(),
                            String.valueOf(details.currentLeader()), details.newReplicas().toString(),
                            String.valueOf(details.newLeader()), details.newLogDirs().toString(),
                            details.actions().stream().map(Enum::name).collect(Collectors.joining(","))});
    }
    writeTable(sb, rows);
    for (PartitionReassignmentDetails details : _partitions) {
      for (String warning : details.warnings()) {
        sb.append("WARNING (").append(details.topicPartition()).append("): ").append(warning).append(NL);
      }
    }
  }

  /**
   * Write the given rows as a table with left-aligned columns to the given string builder.
   *
   * @param sb String builder to write the table to.
   * @param rows Rows of the table, the first being the header.
   */
  static void writeTable(StringBuilder sb, List<String[]> rows) {
    int[] widths = new int[rows.get(0).length];
    for (String[] row : rows) {
      for (int i = 0; i < row.length; i++) {
        widths[i] = Math.max(widths[i], row[i].length());
      }
    }
    for (String[] row : rows) {
      for (int i = 0; i < row.length; i++) {
        sb.append(row[i]);
        if (i < row.length - 1) {
          sb.append(" ".repeat(widths[i] - row[i].length() + 2));
        }
      }
      sb.append(NL);
    }
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
    // Discard irrelevant response.
    _partitions = null;
    _summary = null;
    _impactAnalysis = null;
  }

  /**
   * The actions of a partition reassignment.
   */
  public enum ReassignmentAction {
    NO_CHANGE, REPLICA_SET_CHANGE, REPLICATION_FACTOR_CHANGE, REPLICA_ORDER_CHANGE, INTRA_BROKER_REPLICA_MOVEMENT, LEADERSHIP_MOVEMENT
  }

  /**
   * The status of the impact analysis of a partition reassignment.
   */
  public enum ImpactAnalysisStatus {
    // The impact on the cluster load and goals has been analyzed.
    COMPLETED,
    // The impact could not be analyzed -- e.g. the load monitor is not ready yet.
    UNAVAILABLE,
    // There is nothing to analyze because the request changes nothing.
    NOT_NEEDED
  }

  /**
   * The impact of a partition reassignment on a goal.
   */
  public enum GoalImpactStatus {
    // Satisfied before and after the reassignment.
    OK,
    // Satisfied before, but violated after the reassignment.
    VIOLATION_INTRODUCED,
    // Violated before, but satisfied after the reassignment.
    VIOLATION_FIXED,
    // Violated before and after the reassignment.
    STILL_VIOLATED
  }

  /**
   * The details of a requested partition reassignment.
   */
  @JsonResponseClass
  public static final class PartitionReassignmentDetails {
    @JsonResponseField
    protected static final String TOPIC = "topic";
    @JsonResponseField
    protected static final String PARTITION = "partition";
    @JsonResponseField
    protected static final String CURRENT_REPLICAS = "currentReplicas";
    @JsonResponseField
    protected static final String CURRENT_LEADER = "currentLeader";
    @JsonResponseField
    protected static final String CURRENT_LOG_DIRS = "currentLogDirs";
    @JsonResponseField
    protected static final String NEW_REPLICAS = "newReplicas";
    @JsonResponseField
    protected static final String NEW_LEADER = "newLeader";
    @JsonResponseField
    protected static final String NEW_LOG_DIRS = "newLogDirs";
    @JsonResponseField
    protected static final String ACTIONS = "actions";
    @JsonResponseField
    protected static final String DATA_TO_MOVE_MB = "dataToMoveMB";
    @JsonResponseField(required = false)
    protected static final String WARNINGS = "warnings";
    private final TopicPartition _topicPartition;
    private final List<Integer> _currentReplicas;
    private final int _currentLeader;
    private final List<String> _currentLogDirs;
    private final List<Integer> _newReplicas;
    private final int _newLeader;
    private final List<String> _newLogDirs;
    private final Set<ReassignmentAction> _actions;
    private final long _partitionSizeInMB;
    private final int _numReplicasToAdd;
    private final int _numIntraBrokerReplicaMovements;
    private final List<String> _warnings;

    /**
     * @param topicPartition Topic partition to reassign.
     * @param currentReplicas Current ordered replica list.
     * @param currentLeader Current leader.
     * @param currentLogDirs Current log directory of each current replica.
     * @param newReplicas New ordered replica list.
     * @param newLeader New leader.
     * @param newLogDirs Log directory of each new replica after the reassignment.
     * @param actions Actions to reach the new assignment.
     * @param partitionSizeInMB Size of the partition in MB.
     * @param numReplicasToAdd Number of replicas to add to (i.e. move to) new brokers.
     * @param numIntraBrokerReplicaMovements Number of replicas to move between disks of the same broker.
     * @param warnings Warnings about the reassignment.
     */
    public PartitionReassignmentDetails(TopicPartition topicPartition,
                                        List<Integer> currentReplicas,
                                        int currentLeader,
                                        List<String> currentLogDirs,
                                        List<Integer> newReplicas,
                                        int newLeader,
                                        List<String> newLogDirs,
                                        Set<ReassignmentAction> actions,
                                        long partitionSizeInMB,
                                        int numReplicasToAdd,
                                        int numIntraBrokerReplicaMovements,
                                        List<String> warnings) {
      _topicPartition = topicPartition;
      _currentReplicas = List.copyOf(currentReplicas);
      _currentLeader = currentLeader;
      _currentLogDirs = List.copyOf(currentLogDirs);
      _newReplicas = List.copyOf(newReplicas);
      _newLeader = newLeader;
      _newLogDirs = List.copyOf(newLogDirs);
      _actions = actions.isEmpty() ? EnumSet.of(ReassignmentAction.NO_CHANGE) : EnumSet.copyOf(actions);
      _partitionSizeInMB = partitionSizeInMB;
      _numReplicasToAdd = numReplicasToAdd;
      _numIntraBrokerReplicaMovements = numIntraBrokerReplicaMovements;
      _warnings = List.copyOf(warnings);
    }

    public TopicPartition topicPartition() {
      return _topicPartition;
    }

    public List<Integer> currentReplicas() {
      return _currentReplicas;
    }

    public int currentLeader() {
      return _currentLeader;
    }

    public List<String> currentLogDirs() {
      return _currentLogDirs;
    }

    public List<Integer> newReplicas() {
      return _newReplicas;
    }

    public int newLeader() {
      return _newLeader;
    }

    public List<String> newLogDirs() {
      return _newLogDirs;
    }

    public Set<ReassignmentAction> actions() {
      return Collections.unmodifiableSet(_actions);
    }

    public boolean hasChange() {
      return !_actions.contains(ReassignmentAction.NO_CHANGE);
    }

    public long interBrokerDataToMoveInMB() {
      return _numReplicasToAdd * _partitionSizeInMB;
    }

    public long intraBrokerDataToMoveInMB() {
      return _numIntraBrokerReplicaMovements * _partitionSizeInMB;
    }

    public int numReplicasToAdd() {
      return _numReplicasToAdd;
    }

    public int numIntraBrokerReplicaMovements() {
      return _numIntraBrokerReplicaMovements;
    }

    public List<String> warnings() {
      return _warnings;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
      Map<String, Object> jsonStructure = new HashMap<>();
      jsonStructure.put(TOPIC, _topicPartition.topic());
      jsonStructure.put(PARTITION, _topicPartition.partition());
      jsonStructure.put(CURRENT_REPLICAS, _currentReplicas);
      jsonStructure.put(CURRENT_LEADER, _currentLeader);
      jsonStructure.put(CURRENT_LOG_DIRS, _currentLogDirs);
      jsonStructure.put(NEW_REPLICAS, _newReplicas);
      jsonStructure.put(NEW_LEADER, _newLeader);
      jsonStructure.put(NEW_LOG_DIRS, _newLogDirs);
      jsonStructure.put(ACTIONS, _actions.stream().map(Enum::name).collect(Collectors.toList()));
      jsonStructure.put(DATA_TO_MOVE_MB, interBrokerDataToMoveInMB() + intraBrokerDataToMoveInMB());
      if (!_warnings.isEmpty()) {
        jsonStructure.put(WARNINGS, _warnings);
      }
      return jsonStructure;
    }
  }

  /**
   * The summary of a partition reassignment.
   */
  @JsonResponseClass
  public static final class ReassignPartitionsSummary {
    @JsonResponseField
    protected static final String NUM_PARTITIONS_REQUESTED = "numPartitionsRequested";
    @JsonResponseField
    protected static final String NUM_PARTITIONS_UNCHANGED = "numPartitionsUnchanged";
    @JsonResponseField
    protected static final String NUM_INTER_BROKER_REPLICA_MOVEMENTS = "numInterBrokerReplicaMovements";
    @JsonResponseField
    protected static final String NUM_REPLICA_ORDER_CHANGES = "numReplicaOrderChanges";
    @JsonResponseField
    protected static final String NUM_INTRA_BROKER_REPLICA_MOVEMENTS = "numIntraBrokerReplicaMovements";
    @JsonResponseField
    protected static final String NUM_LEADER_MOVEMENTS = "numLeaderMovements";
    @JsonResponseField
    protected static final String NUM_REPLICATION_FACTOR_CHANGES = "numReplicationFactorChanges";
    @JsonResponseField
    protected static final String INTER_BROKER_DATA_TO_MOVE_MB = "interBrokerDataToMoveMB";
    @JsonResponseField
    protected static final String INTRA_BROKER_DATA_TO_MOVE_MB = "intraBrokerDataToMoveMB";
    private final int _numPartitionsRequested;
    private final int _numPartitionsUnchanged;
    private final int _numInterBrokerReplicaMovements;
    private final int _numReplicaOrderChanges;
    private final int _numIntraBrokerReplicaMovements;
    private final int _numLeaderMovements;
    private final int _numReplicationFactorChanges;
    private final long _interBrokerDataToMoveInMB;
    private final long _intraBrokerDataToMoveInMB;

    /**
     * @param partitions Details of each requested partition reassignment.
     */
    public ReassignPartitionsSummary(List<PartitionReassignmentDetails> partitions) {
      _numPartitionsRequested = partitions.size();
      _numPartitionsUnchanged = (int) partitions.stream().filter(p -> !p.hasChange()).count();
      _numInterBrokerReplicaMovements = partitions.stream().mapToInt(PartitionReassignmentDetails::numReplicasToAdd).sum();
      _numReplicaOrderChanges = countWithAction(partitions, ReassignmentAction.REPLICA_ORDER_CHANGE);
      _numIntraBrokerReplicaMovements = partitions.stream().mapToInt(PartitionReassignmentDetails::numIntraBrokerReplicaMovements).sum();
      _numLeaderMovements = countWithAction(partitions, ReassignmentAction.LEADERSHIP_MOVEMENT);
      _numReplicationFactorChanges = countWithAction(partitions, ReassignmentAction.REPLICATION_FACTOR_CHANGE);
      _interBrokerDataToMoveInMB = partitions.stream().mapToLong(PartitionReassignmentDetails::interBrokerDataToMoveInMB).sum();
      _intraBrokerDataToMoveInMB = partitions.stream().mapToLong(PartitionReassignmentDetails::intraBrokerDataToMoveInMB).sum();
    }

    private static int countWithAction(List<PartitionReassignmentDetails> partitions, ReassignmentAction action) {
      return (int) partitions.stream().filter(p -> p.actions().contains(action)).count();
    }

    public int numPartitionsRequested() {
      return _numPartitionsRequested;
    }

    public int numPartitionsUnchanged() {
      return _numPartitionsUnchanged;
    }

    public int numInterBrokerReplicaMovements() {
      return _numInterBrokerReplicaMovements;
    }

    public int numReplicaOrderChanges() {
      return _numReplicaOrderChanges;
    }

    public int numIntraBrokerReplicaMovements() {
      return _numIntraBrokerReplicaMovements;
    }

    public int numLeaderMovements() {
      return _numLeaderMovements;
    }

    public int numReplicationFactorChanges() {
      return _numReplicationFactorChanges;
    }

    public long interBrokerDataToMoveInMB() {
      return _interBrokerDataToMoveInMB;
    }

    public long intraBrokerDataToMoveInMB() {
      return _intraBrokerDataToMoveInMB;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
      Map<String, Object> jsonStructure = new HashMap<>();
      jsonStructure.put(NUM_PARTITIONS_REQUESTED, _numPartitionsRequested);
      jsonStructure.put(NUM_PARTITIONS_UNCHANGED, _numPartitionsUnchanged);
      jsonStructure.put(NUM_INTER_BROKER_REPLICA_MOVEMENTS, _numInterBrokerReplicaMovements);
      jsonStructure.put(NUM_REPLICA_ORDER_CHANGES, _numReplicaOrderChanges);
      jsonStructure.put(NUM_INTRA_BROKER_REPLICA_MOVEMENTS, _numIntraBrokerReplicaMovements);
      jsonStructure.put(NUM_LEADER_MOVEMENTS, _numLeaderMovements);
      jsonStructure.put(NUM_REPLICATION_FACTOR_CHANGES, _numReplicationFactorChanges);
      jsonStructure.put(INTER_BROKER_DATA_TO_MOVE_MB, _interBrokerDataToMoveInMB);
      jsonStructure.put(INTRA_BROKER_DATA_TO_MOVE_MB, _intraBrokerDataToMoveInMB);
      return jsonStructure;
    }

    @Override
    public String toString() {
      return String.format("Partitions: %d requested, %d unchanged. Replica movements: %d inter-broker (%d MB), %d intra-broker (%d MB). "
                           + "Replica order changes: %d. Leadership movements: %d. Replication factor changes: %d.",
                           _numPartitionsRequested, _numPartitionsUnchanged, _numInterBrokerReplicaMovements, _interBrokerDataToMoveInMB,
                           _numIntraBrokerReplicaMovements, _intraBrokerDataToMoveInMB, _numReplicaOrderChanges, _numLeaderMovements,
                           _numReplicationFactorChanges);
    }
  }

  /**
   * The impact of a partition reassignment on the cluster load and goals.
   */
  @JsonResponseClass
  public static final class ReassignmentImpact {
    @JsonResponseField
    protected static final String STATUS = "status";
    @JsonResponseField(required = false)
    protected static final String REASON = "reason";
    @JsonResponseField(required = false)
    protected static final String NOTES = "notes";
    @JsonResponseField(required = false)
    protected static final String GOALS = "goals";
    @JsonResponseField(required = false)
    protected static final String ON_DEMAND_BALANCEDNESS_SCORE_BEFORE = "onDemandBalancednessScoreBefore";
    @JsonResponseField(required = false)
    protected static final String ON_DEMAND_BALANCEDNESS_SCORE_AFTER = "onDemandBalancednessScoreAfter";
    @JsonResponseField(required = false)
    protected static final String GOAL_SUMMARY = "goalSummary";
    @JsonResponseField(required = false)
    protected static final String CLUSTER_MODEL_STATS_BEFORE = "clusterModelStatsBefore";
    @JsonResponseField(required = false)
    protected static final String CLUSTER_MODEL_STATS_AFTER = "clusterModelStatsAfter";
    @JsonResponseField(required = false)
    protected static final String LOAD_BEFORE_REASSIGNMENT = "loadBeforeReassignment";
    @JsonResponseField(required = false)
    protected static final String LOAD_AFTER_REASSIGNMENT = "loadAfterReassignment";
    @JsonResponseField(required = false)
    protected static final String PARTITIONS_NOT_MODELED = "partitionsNotModeled";
    private final ImpactAnalysisStatus _status;
    private final String _reason;
    private final List<String> _notes;
    private final List<String> _goals;
    private final double _onDemandBalancednessScoreBefore;
    private final double _onDemandBalancednessScoreAfter;
    private final List<GoalImpact> _goalSummary;
    private final ClusterModelStats _clusterModelStatsBefore;
    private final ClusterModelStats _clusterModelStatsAfter;
    private final BrokerStats _loadBeforeReassignment;
    private final BrokerStats _loadAfterReassignment;
    private final List<TopicPartition> _partitionsNotModeled;

    private ReassignmentImpact(ImpactAnalysisStatus status,
                               String reason,
                               List<String> notes,
                               List<String> goals,
                               double onDemandBalancednessScoreBefore,
                               double onDemandBalancednessScoreAfter,
                               List<GoalImpact> goalSummary,
                               ClusterModelStats clusterModelStatsBefore,
                               ClusterModelStats clusterModelStatsAfter,
                               BrokerStats loadBeforeReassignment,
                               BrokerStats loadAfterReassignment,
                               List<TopicPartition> partitionsNotModeled) {
      _status = status;
      _reason = reason;
      _notes = notes;
      _goals = goals;
      _onDemandBalancednessScoreBefore = onDemandBalancednessScoreBefore;
      _onDemandBalancednessScoreAfter = onDemandBalancednessScoreAfter;
      _goalSummary = goalSummary;
      _clusterModelStatsBefore = clusterModelStatsBefore;
      _clusterModelStatsAfter = clusterModelStatsAfter;
      _loadBeforeReassignment = loadBeforeReassignment;
      _loadAfterReassignment = loadAfterReassignment;
      _partitionsNotModeled = partitionsNotModeled;
    }

    /**
     * @param reason The reason why the impact could not be analyzed.
     * @return An impact analysis indicating that the impact could not be analyzed.
     */
    public static ReassignmentImpact unavailable(String reason) {
      return new ReassignmentImpact(ImpactAnalysisStatus.UNAVAILABLE, reason, Collections.emptyList(), null, 0.0, 0.0, null, null, null, null,
                                    null, Collections.emptyList());
    }

    /**
     * @return An impact analysis indicating that there is nothing to analyze.
     */
    public static ReassignmentImpact notNeeded() {
      return new ReassignmentImpact(ImpactAnalysisStatus.NOT_NEEDED, "The request does not change any partition.", Collections.emptyList(),
                                    null, 0.0, 0.0, null, null, null, null, null, Collections.emptyList());
    }

    /**
     * @param notes Caveats of the analysis.
     * @param goals Names of the analyzed goals by priority.
     * @param onDemandBalancednessScoreBefore On-demand balancedness score before the reassignment.
     * @param onDemandBalancednessScoreAfter On-demand balancedness score after the reassignment.
     * @param goalSummary The impact of the reassignment on each analyzed goal.
     * @param clusterModelStatsBefore Cluster model stats before the reassignment.
     * @param clusterModelStatsAfter Cluster model stats after the reassignment.
     * @param loadBeforeReassignment Broker load before the reassignment.
     * @param loadAfterReassignment Broker load after the reassignment.
     * @param partitionsNotModeled Changed partitions that could not be applied to the load model.
     * @return A completed impact analysis.
     */
    public static ReassignmentImpact completed(List<String> notes,
                                               List<String> goals,
                                               double onDemandBalancednessScoreBefore,
                                               double onDemandBalancednessScoreAfter,
                                               List<GoalImpact> goalSummary,
                                               ClusterModelStats clusterModelStatsBefore,
                                               ClusterModelStats clusterModelStatsAfter,
                                               BrokerStats loadBeforeReassignment,
                                               BrokerStats loadAfterReassignment,
                                               List<TopicPartition> partitionsNotModeled) {
      return new ReassignmentImpact(ImpactAnalysisStatus.COMPLETED, null, List.copyOf(notes), List.copyOf(goals),
                                    onDemandBalancednessScoreBefore, onDemandBalancednessScoreAfter, List.copyOf(goalSummary),
                                    clusterModelStatsBefore, clusterModelStatsAfter, loadBeforeReassignment, loadAfterReassignment,
                                    List.copyOf(partitionsNotModeled));
    }

    public ImpactAnalysisStatus status() {
      return _status;
    }

    public String reason() {
      return _reason;
    }

    public List<String> notes() {
      return _notes;
    }

    public List<GoalImpact> goalSummary() {
      return _goalSummary == null ? Collections.emptyList() : _goalSummary;
    }

    public List<TopicPartition> partitionsNotModeled() {
      return _partitionsNotModeled;
    }

    public BrokerStats loadBeforeReassignment() {
      return _loadBeforeReassignment;
    }

    public BrokerStats loadAfterReassignment() {
      return _loadAfterReassignment;
    }

    /**
     * @return Names of the hard goals that are satisfied before, but violated after the reassignment.
     */
    public List<String> introducedHardGoalViolations() {
      return goalSummary().stream().filter(g -> g.hardGoal() && g.status() == GoalImpactStatus.VIOLATION_INTRODUCED)
                          .map(GoalImpact::goal).collect(Collectors.toList());
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
      Map<String, Object> jsonStructure = new HashMap<>();
      jsonStructure.put(STATUS, _status.name());
      if (_reason != null) {
        jsonStructure.put(REASON, _reason);
      }
      if (_status == ImpactAnalysisStatus.COMPLETED) {
        if (!_notes.isEmpty()) {
          jsonStructure.put(NOTES, _notes);
        }
        jsonStructure.put(GOALS, _goals);
        jsonStructure.put(ON_DEMAND_BALANCEDNESS_SCORE_BEFORE, _onDemandBalancednessScoreBefore);
        jsonStructure.put(ON_DEMAND_BALANCEDNESS_SCORE_AFTER, _onDemandBalancednessScoreAfter);
        jsonStructure.put(GOAL_SUMMARY, _goalSummary.stream().map(GoalImpact::getJsonStructure).collect(Collectors.toList()));
        jsonStructure.put(CLUSTER_MODEL_STATS_BEFORE, _clusterModelStatsBefore.getJsonStructure());
        jsonStructure.put(CLUSTER_MODEL_STATS_AFTER, _clusterModelStatsAfter.getJsonStructure());
        jsonStructure.put(LOAD_BEFORE_REASSIGNMENT, _loadBeforeReassignment.getJsonStructure());
        jsonStructure.put(LOAD_AFTER_REASSIGNMENT, _loadAfterReassignment.getJsonStructure());
        jsonStructure.put(PARTITIONS_NOT_MODELED, _partitionsNotModeled.stream().map(TopicPartition::toString).collect(Collectors.toList()));
      }
      return jsonStructure;
    }

    /**
     * Write the plaintext version of this impact analysis to the given string builder.
     *
     * @param sb String builder to write the impact analysis to.
     */
    void writePlaintext(StringBuilder sb) {
      sb.append("Impact analysis: ").append(_status);
      if (_reason != null) {
        sb.append(" (").append(_reason).append(")");
      }
      sb.append(NL);
      if (_status != ImpactAnalysisStatus.COMPLETED) {
        return;
      }
      for (String note : _notes) {
        sb.append("NOTE: ").append(note).append(NL);
      }
      if (!_partitionsNotModeled.isEmpty()) {
        sb.append("Partitions not applied to the load model (the model is out of date): ").append(_partitionsNotModeled).append(NL);
      }
      sb.append(String.format("On-demand balancedness score before: %.3f, after: %.3f.", _onDemandBalancednessScoreBefore,
                              _onDemandBalancednessScoreAfter)).append(NL).append(NL);
      List<String[]> rows = new ArrayList<>();
      rows.add(new String[]{"GOAL", "HARD", "VIOLATED_BEFORE", "VIOLATED_AFTER", "STATUS"});
      for (GoalImpact goalImpact : _goalSummary) {
        rows.add(new String[]{goalImpact.goal(), String.valueOf(goalImpact.hardGoal()), String.valueOf(goalImpact.violatedBefore()),
                              String.valueOf(goalImpact.violatedAfter()), goalImpact.status().name()});
      }
      writeTable(sb, rows);
      sb.append(NL).append("Cluster load before the reassignment:").append(NL).append(_loadBeforeReassignment);
      sb.append(NL).append("Cluster load after the reassignment:").append(NL).append(_loadAfterReassignment);
    }
  }

  /**
   * The impact of a partition reassignment on a goal.
   */
  @JsonResponseClass
  public static final class GoalImpact {
    @JsonResponseField
    protected static final String GOAL = "goal";
    @JsonResponseField
    protected static final String HARD_GOAL = "hardGoal";
    @JsonResponseField
    protected static final String VIOLATED_BEFORE = "violatedBefore";
    @JsonResponseField
    protected static final String VIOLATED_AFTER = "violatedAfter";
    @JsonResponseField
    protected static final String STATUS = "status";
    private final String _goal;
    private final boolean _hardGoal;
    private final boolean _violatedBefore;
    private final boolean _violatedAfter;

    /**
     * @param goal Name of the goal.
     * @param hardGoal {@code true} if the goal is a hard goal, {@code false} otherwise.
     * @param violatedBefore {@code true} if the goal is violated before the reassignment, {@code false} otherwise.
     * @param violatedAfter {@code true} if the goal is violated after the reassignment, {@code false} otherwise.
     */
    public GoalImpact(String goal, boolean hardGoal, boolean violatedBefore, boolean violatedAfter) {
      _goal = goal;
      _hardGoal = hardGoal;
      _violatedBefore = violatedBefore;
      _violatedAfter = violatedAfter;
    }

    public String goal() {
      return _goal;
    }

    public boolean hardGoal() {
      return _hardGoal;
    }

    public boolean violatedBefore() {
      return _violatedBefore;
    }

    public boolean violatedAfter() {
      return _violatedAfter;
    }

    /**
     * @return The impact of the reassignment on the goal.
     */
    public GoalImpactStatus status() {
      if (_violatedBefore) {
        return _violatedAfter ? GoalImpactStatus.STILL_VIOLATED : GoalImpactStatus.VIOLATION_FIXED;
      }
      return _violatedAfter ? GoalImpactStatus.VIOLATION_INTRODUCED : GoalImpactStatus.OK;
    }

    /**
     * @return An object that can be further used to encode into JSON.
     */
    public Map<String, Object> getJsonStructure() {
      return Map.of(GOAL, _goal, HARD_GOAL, _hardGoal, VIOLATED_BEFORE, _violatedBefore, VIOLATED_AFTER, _violatedAfter,
                    STATUS, status().name());
    }
  }
}
