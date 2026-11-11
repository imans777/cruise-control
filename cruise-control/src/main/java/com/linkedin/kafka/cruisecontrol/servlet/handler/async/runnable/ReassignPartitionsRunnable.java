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
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.ReassignPartitionsProposalBuilder.ReassignPartitionsPlan;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReassignPartitionsParameters;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RequestedPartitionReassignment;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult;
import com.linkedin.kafka.cruisecontrol.servlet.response.ReassignPartitionsResult.ReassignmentImpact;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckGoals;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckNonExistingGoal;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.maybeStopOngoingExecutionToModifyAndWait;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;


/**
 * The async runnable to execute a manual partition reassignment -- see
 * {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#REASSIGN_PARTITIONS}.
 *
 * <ol>
 *   <li>Fetch a fresh view of the requested partitions, alive brokers, ongoing reassignments and log directories from Kafka -- i.e.
 *   without relying on the (possibly stale) metadata cache or the readiness of the load monitor.</li>
 *   <li>Validate the request against this view and build the execution proposals
 *   (see {@link ReassignPartitionsProposalBuilder}).</li>
 *   <li>Analyze the impact of the reassignment on the cluster load and goals (see {@link ReassignmentImpactAnalyzer}).</li>
 *   <li>Unless dry run, refuse to execute a reassignment that violates hard goals -- or whose impact on hard goals cannot be
 *   verified -- unless the hard goal check is skipped. Then, execute the proposals via the executor.</li>
 * </ol>
 */
public class ReassignPartitionsRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ReassignPartitionsRunnable.class);
  protected static final boolean IS_KAFKA_ASSIGNER_MODE = false;
  protected static final boolean IS_TRIGGERED_BY_USER_REQUEST = true;
  protected static final boolean SKIP_AUTO_REFRESHING_CONCURRENCY = true;
  protected final String _uuid;
  protected final ReassignPartitionsParameters _parameters;

  /**
   * @param kafkaCruiseControl The Kafka Cruise Control instance.
   * @param future The future of the operation.
   * @param uuid UUID of the user task.
   * @param parameters Parameters of the request.
   */
  public ReassignPartitionsRunnable(KafkaCruiseControl kafkaCruiseControl,
                                    OperationFuture future,
                                    String uuid,
                                    ReassignPartitionsParameters parameters) {
    super(kafkaCruiseControl, future);
    _uuid = uuid;
    _parameters = parameters;
  }

  @Override
  protected ReassignPartitionsResult getResult() throws Exception {
    List<RequestedPartitionReassignment> requested = _parameters.requestedReassignments();
    if (requested == null || requested.isEmpty()) {
      throw new UserRequestException("Missing partition reassignment in the request body.");
    }
    KafkaCruiseControlConfig config = _kafkaCruiseControl.config();
    if (!_parameters.goals().isEmpty()) {
      try {
        // Unknown goals are rejected even if the hard goal check is skipped -- otherwise, the impact could not be analyzed.
        sanityCheckNonExistingGoal(_parameters.goals(), AnalyzerUtils.getCaseInsensitiveGoalsByName(config));
        sanityCheckGoals(_parameters.goals(), _parameters.skipHardGoalCheck(), config);
      } catch (IllegalArgumentException iae) {
        throw new UserRequestException(iae.getMessage());
      }
    }
    boolean dryRun = _parameters.dryRun();
    _kafkaCruiseControl.sanityCheckDryRun(dryRun, _parameters.stopOngoingExecution());
    if (_parameters.stopOngoingExecution()) {
      maybeStopOngoingExecutionToModifyAndWait(_kafkaCruiseControl, _future.operationProgress());
    }
    if (!dryRun) {
      // Prevent other executions from starting while the reassignment is planned and analyzed.
      _kafkaCruiseControl.setGeneratingProposalsForExecution(_uuid, _parameters::reason, IS_TRIGGERED_BY_USER_REQUEST);
    }
    boolean executionStarted = false;
    try {
      ReassignPartitionsPlan plan = plan(requested, config);
      ReassignmentImpact impact = new ReassignmentImpactAnalyzer(_kafkaCruiseControl, _future.operationProgress(),
                                                                 _parameters.allowCapacityEstimation())
          .analyze(plan.proposals().values(), plan.hasIntraBrokerReplicaMovements(), _parameters.goals());
      if (!dryRun && !plan.proposals().isEmpty()) {
        sanityCheckHardGoals(impact);
        requestLogDirsOfReplicasToAdd(plan.logDirHints(), config);
        _kafkaCruiseControl.executeProposals(plan.proposalsToExecute(),
                                             Collections.emptySet(),
                                             IS_KAFKA_ASSIGNER_MODE,
                                             _parameters.concurrentInterBrokerPartitionMovements(),
                                             _parameters.maxInterBrokerPartitionMovements(),
                                             _parameters.concurrentIntraBrokerPartitionMovements(),
                                             _parameters.clusterLeaderMovementConcurrency(),
                                             _parameters.brokerLeaderMovementConcurrency(),
                                             _parameters.executionProgressCheckIntervalMs(),
                                             _parameters.replicaMovementStrategy(),
                                             _parameters.replicationThrottle(),
                                             IS_TRIGGERED_BY_USER_REQUEST,
                                             _uuid,
                                             SKIP_AUTO_REFRESHING_CONCURRENCY);
        executionStarted = true;
      }
      ReassignPartitionsResult result = new ReassignPartitionsResult(plan.partitions(), impact, dryRun, executionStarted, config);
      LOG.info("User task {}: {} partition reassignment (reason: {}): {} Impact analysis: {}.", _uuid,
               executionStarted ? "Started executing the" : (dryRun ? "Dry run of the" : "Nothing to execute for the"),
               _parameters.reason(), result.summary(), impact.status());
      return result;
    } finally {
      if (!dryRun && !executionStarted) {
        _kafkaCruiseControl.failGeneratingProposalsForExecution(_uuid);
      }
    }
  }

  /**
   * Validate the given requested reassignments against a fresh view of the cluster and build their plan.
   *
   * @param requested Requested partition reassignments.
   * @param config The configurations for Cruise Control.
   * @return The plan of the requested partition reassignments.
   */
  private ReassignPartitionsPlan plan(List<RequestedPartitionReassignment> requested, KafkaCruiseControlConfig config)
      throws InterruptedException, ExecutionException, TimeoutException {
    Admin adminClient = _kafkaCruiseControl.adminClient();
    long timeoutMs = config.getInt(ExecutorConfig.ADMIN_CLIENT_REQUEST_TIMEOUT_MS_CONFIG);
    Set<String> requestedTopics = requested.stream().map(r -> r.topicPartition().topic()).collect(Collectors.toCollection(TreeSet::new));
    Cluster cluster = freshCluster(adminClient, requestedTopics, timeoutMs);
    Set<TopicPartition> existingPartitions = requested.stream().map(RequestedPartitionReassignment::topicPartition)
                                                      .filter(tp -> cluster.partition(tp) != null).collect(Collectors.toSet());
    Set<TopicPartition> partitionsBeingReassigned = existingPartitions.isEmpty()
        ? Collections.emptySet()
        : adminClient.listPartitionReassignments(existingPartitions).reassignments().get(timeoutMs, TimeUnit.MILLISECONDS).keySet();
    Map<Integer, String> logDirErrorByBroker = new HashMap<>();
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker =
        RunnableUtils.describeLogDirs(adminClient, ReassignPartitionsProposalBuilder.brokersToDescribe(requested, cluster),
                                      config.getLong(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), logDirErrorByBroker);
    return ReassignPartitionsProposalBuilder.build(requested, cluster, partitionsBeingReassigned, logDirsByBroker, logDirErrorByBroker);
  }

  /**
   * Get the current metadata of the given topics and the alive brokers directly from Kafka. Topics that do not exist are omitted.
   *
   * @param adminClient The admin client to retrieve the metadata with.
   * @param topics Topics of interest.
   * @param timeoutMs Timeout of each request.
   * @return The current metadata of the given topics and the alive brokers.
   */
  static Cluster freshCluster(Admin adminClient, Set<String> topics, long timeoutMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    Set<String> existingTopics = new TreeSet<>(adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names()
                                                          .get(timeoutMs, TimeUnit.MILLISECONDS));
    existingTopics.retainAll(topics);
    DescribeClusterResult describeClusterResult = adminClient.describeCluster();
    Collection<Node> aliveBrokers = describeClusterResult.nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
    String clusterId = describeClusterResult.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
    Set<Integer> aliveBrokerIds = aliveBrokers.stream().map(Node::id).collect(Collectors.toSet());
    List<PartitionInfo> partitionInfos = new ArrayList<>();
    if (!existingTopics.isEmpty()) {
      Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(existingTopics).allTopicNames()
                                                                   .get(timeoutMs, TimeUnit.MILLISECONDS);
      for (TopicDescription description : topicDescriptions.values()) {
        for (TopicPartitionInfo info : description.partitions()) {
          Node[] replicas = info.replicas().toArray(new Node[0]);
          Node[] offlineReplicas = Arrays.stream(replicas).filter(node -> !aliveBrokerIds.contains(node.id())).toArray(Node[]::new);
          // A leader that is not among the alive brokers (e.g. it has just failed) is unknown -- the cluster requires it to be alive.
          Node leader = info.leader() != null && aliveBrokerIds.contains(info.leader().id()) ? info.leader() : null;
          partitionInfos.add(new PartitionInfo(description.name(), info.partition(), leader, replicas, info.isr().toArray(new Node[0]),
                                               offlineReplicas));
        }
      }
    }
    return new Cluster(clusterId, aliveBrokers, partitionInfos, Collections.emptySet(), Collections.emptySet());
  }

  /**
   * Refuse to execute a reassignment that would violate a hard goal, or whose impact on hard goals cannot be verified -- unless
   * the hard goal check is skipped.
   *
   * @param impact The impact of the reassignment on the cluster load and goals.
   */
  private void sanityCheckHardGoals(ReassignmentImpact impact) {
    if (_parameters.skipHardGoalCheck()) {
      return;
    }
    switch (impact.status()) {
      case COMPLETED:
        if (!impact.partitionsNotModeled().isEmpty()) {
          throw new UserRequestException(String.format("Cannot verify the impact of reassigning partitions %s on hard goals, because the "
                                                       + "load model is not up to date for them. Retry later, or set %s=true to execute "
                                                       + "anyway.", impact.partitionsNotModeled(), SKIP_HARD_GOAL_CHECK_PARAM));
        }
        List<String> violatedHardGoals = impact.introducedHardGoalViolations();
        if (!violatedHardGoals.isEmpty()) {
          throw new UserRequestException(String.format("The partition reassignment would violate hard goals %s. Review its impact with "
                                                       + "%s=true, or set %s=true to execute anyway.", violatedHardGoals, DRY_RUN_PARAM,
                                                       SKIP_HARD_GOAL_CHECK_PARAM));
        }
        break;
      case UNAVAILABLE:
        throw new UserRequestException(String.format("Cannot verify the impact of the partition reassignment on hard goals (%s). Retry "
                                                     + "later, or set %s=true to execute anyway.", impact.reason(),
                                                     SKIP_HARD_GOAL_CHECK_PARAM));
      default:
        break;
    }
  }

  /**
   * Ask the brokers to create the given replicas -- which do not exist yet -- in the given log directories once they are added by
   * the reassignment. This is the same mechanism kafka-reassign-partitions.sh uses: brokers remember the requested log directory of
   * a replica they do not host yet, and reject the request with {@link ReplicaNotAvailableException} or
   * {@link NotLeaderOrFollowerException}, which are hence expected.
   *
   * @param logDirByReplica The requested log directory of each replica to add.
   * @param config The configurations for Cruise Control.
   */
  private void requestLogDirsOfReplicasToAdd(Map<TopicPartitionReplica, String> logDirByReplica, KafkaCruiseControlConfig config)
      throws InterruptedException {
    if (logDirByReplica.isEmpty()) {
      return;
    }
    LOG.info("User task {}: Requesting the log directories of replicas to add {}.", _uuid, logDirByReplica);
    long timeoutMs = config.getLong(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG);
    Map<TopicPartitionReplica, KafkaFuture<Void>> results = _kafkaCruiseControl.adminClient().alterReplicaLogDirs(logDirByReplica).values();
    long deadlineMs = System.currentTimeMillis() + timeoutMs;
    List<String> failures = new ArrayList<>();
    for (Map.Entry<TopicPartitionReplica, KafkaFuture<Void>> entry : results.entrySet()) {
      try {
        entry.getValue().get(Math.max(0L, deadlineMs - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
      } catch (ExecutionException ee) {
        if (!(ee.getCause() instanceof ReplicaNotAvailableException) && !(ee.getCause() instanceof NotLeaderOrFollowerException)) {
          failures.add(String.format("%s: %s", entry.getKey(), ee.getCause()));
        }
      } catch (TimeoutException te) {
        failures.add(String.format("%s: timed out after %d ms", entry.getKey(), timeoutMs));
      }
    }
    if (!failures.isEmpty()) {
      throw new IllegalStateException(String.format("Failed to request the log directories of replicas to add: %s", failures));
    }
  }
}
