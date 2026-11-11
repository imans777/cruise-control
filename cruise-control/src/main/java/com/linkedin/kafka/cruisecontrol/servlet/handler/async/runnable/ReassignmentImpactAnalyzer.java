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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.GoalImpact;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentImpact;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.goalsByPriority;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckLoadMonitorReadiness;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.computeOptimizationOptions;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;


/**
 * Analyzes the impact of a manual partition reassignment on the cluster load and goals, similar to the summary of a goal-based
 * rebalance. It applies the reassignment to a cluster model from the load monitor, and compares the broker load and goal
 * violations before and after the reassignment. A goal is considered violated if the goal optimizer would change the cluster to
 * satisfy it -- as reported in the goal summary of a rebalance.
 *
 * <ul>
 *   <li>Replica set / order and leadership changes are analyzed against the requested (or default) goals on a broker-level model.</li>
 *   <li>Moving replicas between log directories is analyzed against the intra-broker goals on a disk-level model, which requires the
 *   capacity of each log directory in the broker capacity config. Otherwise, the analysis falls back to a broker-level model, whose
 *   load does not change by moving replicas between log directories.</li>
 * </ul>
 */
final class ReassignmentImpactAnalyzer {
  static final List<String> INTRA_BROKER_GOALS = List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName(),
                                                         IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());
  private static final Logger LOG = LoggerFactory.getLogger(ReassignmentImpactAnalyzer.class);
  private static final boolean FAST_MODE = true;
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final OperationProgress _operationProgress;
  private final boolean _allowCapacityEstimation;

  /**
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param operationProgress The progress of the operation.
   * @param allowCapacityEstimation {@code true} to allow capacity estimation in the cluster model, {@code false} otherwise.
   */
  ReassignmentImpactAnalyzer(KafkaCruiseControl kafkaCruiseControl, OperationProgress operationProgress, boolean allowCapacityEstimation) {
    _kafkaCruiseControl = kafkaCruiseControl;
    _operationProgress = operationProgress;
    _allowCapacityEstimation = allowCapacityEstimation;
  }

  /**
   * Analyze the impact of the given proposals on the cluster load and goals. Failures to analyze are reported as an
   * {@link ReassignmentImpact#unavailable(String) unavailable} analysis, rather than exceptions.
   *
   * @param proposals Execution proposals of the partition reassignment.
   * @param hasIntraBrokerReplicaMovements {@code true} if the proposals move replicas between log directories, {@code false} otherwise.
   * @param requestedGoals Goals requested by the user, or an empty list to use the default goals.
   * @return The impact of the given proposals on the cluster load and goals.
   * @throws InterruptedException If the analysis is interrupted -- e.g. the operation is cancelled.
   */
  ReassignmentImpact analyze(Collection<ExecutionProposal> proposals, boolean hasIntraBrokerReplicaMovements, List<String> requestedGoals)
      throws InterruptedException {
    if (proposals.isEmpty()) {
      return ReassignmentImpact.notNeeded();
    }
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    List<String> notes = new ArrayList<>();
    boolean diskLevel = hasIntraBrokerReplicaMovements;
    List<String> goalNames = requestedGoals.isEmpty() && diskLevel ? INTRA_BROKER_GOALS : requestedGoals;
    try {
      ModelCompletenessRequirements requirements = _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority(goalNames, config));
      sanityCheckLoadMonitorReadiness(requirements, _kafkaCruiseControl.getLoadMonitorTaskRunnerState());
      ClusterModel clusterModel;
      try (AutoCloseable ignored = _kafkaCruiseControl.acquireForModelGeneration(_operationProgress)) {
        try {
          clusterModel = clusterModel(requirements, diskLevel);
        } catch (IllegalStateException ise) {
          if (!diskLevel) {
            throw ise;
          }
          // The broker capacity config has no capacity per log directory -- fall back to a broker-level model.
          notes.add(String.format("Disk-level analysis is unavailable (%s) -- showing the broker-level analysis instead; moving "
                                  + "replicas between log directories of a broker does not change its broker-level load.",
                                  ise.getMessage()));
          diskLevel = false;
          goalNames = requestedGoals;
          requirements = _kafkaCruiseControl.modelCompletenessRequirements(goalsByPriority(goalNames, config));
          clusterModel = clusterModel(requirements, false);
        }
        return analyze(clusterModel, requirements, proposals, diskLevel, goalNames, notes);
      }
    } catch (InterruptedException ie) {
      throw ie;
    } catch (Exception e) {
      LOG.warn("Failed to analyze the impact of the partition reassignment.", e);
      return ReassignmentImpact.unavailable(String.format("Failed to analyze the impact of the partition reassignment: %s",
                                                          e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
    }
  }

  private ReassignmentImpact analyze(ClusterModel clusterModel,
                                     ModelCompletenessRequirements requirements,
                                     Collection<ExecutionProposal> proposals,
                                     boolean diskLevel,
                                     List<String> goalNames,
                                     List<String> notes) throws Exception {
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    BalancingConstraint balancingConstraint = new BalancingConstraint(config);
    BrokerStats loadBefore = clusterModel.brokerStats(config);
    ClusterModelStats statsBefore = clusterModel.getClusterStats(balancingConstraint);
    List<TopicPartition> partitionsNotModeled = applyPlan(clusterModel, proposals, diskLevel);
    BrokerStats loadAfter = clusterModel.brokerStats(config);
    ClusterModelStats statsAfter = clusterModel.getClusterStats(balancingConstraint);
    if (!partitionsNotModeled.isEmpty()) {
      notes.add(String.format("The load model is not up to date for partitions %s, hence the analysis excludes them.",
                              partitionsNotModeled));
    }

    // Goals violated after the reassignment.
    List<Goal> goalsAfter = goalsByPriority(goalNames, config);
    OptimizerResult resultAfter = _kafkaCruiseControl.optimizations(clusterModel, goalsAfter, _operationProgress, null,
                                                                    optimizationOptions(clusterModel));
    // Goals violated before the reassignment.
    ClusterModel clusterModelBefore = clusterModel(requirements, diskLevel);
    List<Goal> goalsBefore = goalsByPriority(goalNames, config);
    OptimizerResult resultBefore = _kafkaCruiseControl.optimizations(clusterModelBefore, goalsBefore, _operationProgress, null,
                                                                     optimizationOptions(clusterModelBefore));

    Set<String> hardGoals = config.getList(AnalyzerConfig.HARD_GOALS_CONFIG).stream()
                                  .map(goal -> goal.substring(goal.lastIndexOf('.') + 1)).collect(Collectors.toSet());
    List<GoalImpact> goalImpacts = new ArrayList<>(goalsAfter.size());
    for (Goal goal : goalsAfter) {
      goalImpacts.add(new GoalImpact(goal.name(), hardGoals.contains(goal.name()),
                                     resultBefore.violatedGoalsBeforeOptimization().contains(goal.name()),
                                     resultAfter.violatedGoalsBeforeOptimization().contains(goal.name())));
    }
    if (!diskLevel && proposals.stream().anyMatch(p -> !p.replicasToMoveBetweenDisksByBroker().isEmpty()
                                                       || p.replicasToAdd().stream().anyMatch(r -> r.logdir() != null))) {
      notes.add("The placement of replicas on log directories is not modeled at broker level.");
    }
    return ReassignmentImpact.completed(notes, goalsAfter.stream().map(Goal::name).collect(Collectors.toList()),
                                        resultBefore.onDemandBalancednessScoreBefore(), resultAfter.onDemandBalancednessScoreBefore(),
                                        goalImpacts, statsBefore, statsAfter, loadBefore, loadAfter, partitionsNotModeled);
  }

  private ClusterModel clusterModel(ModelCompletenessRequirements requirements, boolean populateReplicaPlacementInfo) throws Exception {
    return _kafkaCruiseControl.clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL, _kafkaCruiseControl.timeMs(), requirements,
                                            populateReplicaPlacementInfo, _allowCapacityEstimation, _operationProgress);
  }

  private OptimizationOptions optimizationOptions(ClusterModel clusterModel) {
    // Analysis is always a dry run -- i.e. it must not update the recently removed / demoted brokers.
    return computeOptimizationOptions(clusterModel, false, _kafkaCruiseControl, Collections.emptySet(), true, false, false, null,
                                      Collections.emptySet(), false, FAST_MODE);
  }

  /**
   * Apply the given proposals to the given cluster model. Partitions whose replicas in the cluster model differ from the current
   * replicas of the proposal (e.g. the model is out of date), or whose new placement does not exist in the model are not applied.
   *
   * @param clusterModel The cluster model to apply the given proposals to.
   * @param proposals Execution proposals to apply.
   * @param diskLevel {@code true} to also apply replica movements between log directories, {@code false} otherwise.
   * @return Partitions that could not be applied to the cluster model.
   */
  static List<TopicPartition> applyPlan(ClusterModel clusterModel, Collection<ExecutionProposal> proposals, boolean diskLevel) {
    List<TopicPartition> partitionsNotModeled = new ArrayList<>();
    boolean replicasDeleted = false;
    for (ExecutionProposal proposal : proposals) {
      TopicPartition tp = proposal.topicPartition();
      if (!canApply(clusterModel, proposal, diskLevel)) {
        partitionsNotModeled.add(tp);
        continue;
      }
      List<Integer> oldReplicas = brokerIds(proposal.oldReplicas());
      List<Integer> newReplicas = brokerIds(proposal.newReplicas());
      List<Integer> replicasToAdd = newReplicas.stream().filter(b -> !oldReplicas.contains(b)).collect(Collectors.toList());
      List<Integer> replicasToRemove = oldReplicas.stream().filter(b -> !newReplicas.contains(b)).collect(Collectors.toList());
      int numRelocations = Math.min(replicasToAdd.size(), replicasToRemove.size());
      for (int i = 0; i < numRelocations; i++) {
        clusterModel.relocateReplica(tp, replicasToRemove.get(i), replicasToAdd.get(i));
      }
      // A replication factor increase adds follower replicas with the load of a follower of the partition.
      for (int i = numRelocations; i < replicasToAdd.size(); i++) {
        Broker broker = clusterModel.broker(replicasToAdd.get(i));
        Load followerLoad = clusterModel.partition(tp).leader().getFollowerLoadFromLeader();
        clusterModel.createReplica(broker.rack().id(), broker.id(), tp, clusterModel.partition(tp).replicas().size(), false, false, null,
                                   true);
        clusterModel.setReplicaLoad(broker.rack().id(), broker.id(), tp, followerLoad.loadByWindows(), followerLoad.windows());
      }
      int leader = clusterModel.partition(tp).leader().broker().id();
      if (leader != newReplicas.get(0)) {
        clusterModel.relocateLeadership(tp, leader, newReplicas.get(0));
      }
      // A replication factor decrease removes replicas after the leadership moved to the new leader.
      for (int i = numRelocations; i < replicasToRemove.size(); i++) {
        clusterModel.deleteReplica(tp, replicasToRemove.get(i));
        replicasDeleted = true;
      }
      if (diskLevel) {
        for (Map.Entry<Integer, ReplicaPlacementInfo> entry : proposal.replicasToMoveBetweenDisksByBroker().entrySet()) {
          clusterModel.relocateReplica(tp, entry.getKey(), entry.getValue().logdir());
        }
      }
    }
    if (replicasDeleted) {
      clusterModel.refreshClusterMaxReplicationFactor();
    }
    return partitionsNotModeled;
  }

  private static boolean canApply(ClusterModel clusterModel, ExecutionProposal proposal, boolean diskLevel) {
    Partition partition = clusterModel.partition(proposal.topicPartition());
    if (partition == null || partition.leader() == null) {
      return false;
    }
    Set<Integer> modelReplicas = partition.replicas().stream().map(r -> r.broker().id()).collect(Collectors.toSet());
    if (!modelReplicas.equals(new HashSet<>(brokerIds(proposal.oldReplicas())))) {
      return false;
    }
    for (ReplicaPlacementInfo replica : proposal.newReplicas()) {
      Broker broker = clusterModel.broker(replica.brokerId());
      if (broker == null || !broker.isAlive()) {
        return false;
      }
    }
    if (diskLevel) {
      for (Map.Entry<Integer, ReplicaPlacementInfo> entry : proposal.replicasToMoveBetweenDisksByBroker().entrySet()) {
        Broker broker = clusterModel.broker(entry.getKey());
        if (broker.disk(entry.getValue().logdir()) == null || broker.replica(proposal.topicPartition()).disk() == null) {
          return false;
        }
      }
    }
    return true;
  }

  private static List<Integer> brokerIds(List<ReplicaPlacementInfo> replicas) {
    return replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList());
  }
}
