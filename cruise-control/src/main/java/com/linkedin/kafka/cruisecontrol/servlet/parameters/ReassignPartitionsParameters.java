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

import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

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
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#REASSIGN_PARTITIONS}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>The partition reassignment is given in the JSON request body (header {@code Content-Type: application/json}) -- see
 *   {@link ReassignPartitionsBodyParser} for the format. The body is optional only when polling an existing user task with the
 *   User-Task-ID header.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/reassign_partitions?json=[true/false]&amp;dryrun=[true/false]&amp;reason=[reason-for-request]
 *    &amp;stop_ongoing_execution=[true/false]&amp;review_id=[id]&amp;goals=[goal1,goal2...]&amp;skip_hard_goal_check=[true/false]
 *    &amp;allow_capacity_estimation=[true/false]&amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]
 *    &amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]&amp;concurrent_intra_broker_partition_movements=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;replica_movement_strategies=[strategy1,strategy2...]
 *    &amp;replication_throttle=[bytes_per_second]&amp;get_response_schema=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class ReassignPartitionsParameters extends AbstractParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(DRY_RUN_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
    validParameterNames.add(REVIEW_ID_PARAM);
    validParameterNames.add(GOALS_PARAM);
    validParameterNames.add(SKIP_HARD_GOAL_CHECK_PARAM);
    validParameterNames.add(ALLOW_CAPACITY_ESTIMATION_PARAM);
    validParameterNames.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    validParameterNames.add(MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
    validParameterNames.add(CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_PARAM);
    validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.add(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
    validParameterNames.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    validParameterNames.add(REPLICATION_THROTTLE_PARAM);
    validParameterNames.addAll(AbstractParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected Integer _reviewId;
  protected boolean _dryRun;
  protected String _reason;
  protected boolean _stopOngoingExecution;
  protected List<String> _goals;
  protected boolean _skipHardGoalCheck;
  protected boolean _allowCapacityEstimation;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _maxInterBrokerPartitionMovements;
  protected Integer _concurrentIntraBrokerPartitionMovements;
  protected Integer _clusterLeaderMovementConcurrency;
  protected Integer _brokerLeaderMovementConcurrency;
  protected Long _executionProgressCheckIntervalMs;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Long _replicationThrottle;
  protected List<RequestedPartitionReassignment> _requestedReassignments;

  public ReassignPartitionsParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
    _dryRun = ParameterUtils.getDryRun(_requestContext);
    boolean requestReasonRequired = _config.getBoolean(ExecutorConfig.REQUEST_REASON_REQUIRED_CONFIG);
    _reason = ParameterUtils.reason(_requestContext, requestReasonRequired && !_dryRun);
    _stopOngoingExecution = ParameterUtils.stopOngoingExecution(_requestContext);
    if (_stopOngoingExecution && _dryRun) {
      throw new UserRequestException(String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
    }
    _goals = ParameterUtils.getGoals(_requestContext);
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_requestContext);
    _allowCapacityEstimation = ParameterUtils.allowCapacityEstimation(_requestContext);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTER_BROKER_REPLICA);
    _maxInterBrokerPartitionMovements = ParameterUtils.maxPartitionMovements(_requestContext);
    _concurrentIntraBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTRA_BROKER_REPLICA);
    _clusterLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_CLUSTER);
    _brokerLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_BROKER);
    _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_requestContext);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_requestContext, _config);
    _replicationThrottle = ParameterUtils.replicationThrottle(_requestContext, _config);
    // Parse the body eagerly: Two-step verification keeps this object (not the request) in the purgatory, and the body of a
    // servlet request can be read only once.
    _requestedReassignments = ParameterUtils.requestedReassignments(_requestContext);
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public boolean dryRun() {
    return _dryRun;
  }

  public String reason() {
    return _reason;
  }

  public boolean stopOngoingExecution() {
    return _stopOngoingExecution;
  }

  public List<String> goals() {
    return _goals;
  }

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public boolean allowCapacityEstimation() {
    return _allowCapacityEstimation;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer maxInterBrokerPartitionMovements() {
    return _maxInterBrokerPartitionMovements;
  }

  public Integer concurrentIntraBrokerPartitionMovements() {
    return _concurrentIntraBrokerPartitionMovements;
  }

  public Integer clusterLeaderMovementConcurrency() {
    return _clusterLeaderMovementConcurrency;
  }

  public Integer brokerLeaderMovementConcurrency() {
    return _brokerLeaderMovementConcurrency;
  }

  public Long executionProgressCheckIntervalMs() {
    return _executionProgressCheckIntervalMs;
  }

  public ReplicaMovementStrategy replicaMovementStrategy() {
    return _replicaMovementStrategy;
  }

  public Long replicationThrottle() {
    return _replicationThrottle;
  }

  /**
   * @return The requested partition reassignments, or {@code null} if this request polls an existing user task without a body.
   */
  public List<RequestedPartitionReassignment> requestedReassignments() {
    return _requestedReassignments;
  }

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
