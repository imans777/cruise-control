/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.executor.ConcurrencyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_OTHER_TOPICS_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_LEADER_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DRY_RUN_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_COUNT;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_DISK_MB_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_NW_IN_KBPS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.PARTITION_NW_OUT_KBPS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASON_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICATION_FACTOR_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICATION_THROTTLE_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REPLICA_MOVEMENT_STRATEGIES_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REVIEW_ID_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_HARD_GOAL_CHECK_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_RACK_AWARENESS_CHECK_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.STOP_ONGOING_EXECUTION_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.TOPIC_PARAM;


/**
 * Parameters for {@link CruiseControlEndPoint#CREATE_TOPIC}.
 *
 * <ul>
 *   <li>Note that "review_id" is mutually exclusive to the other parameters -- i.e. they cannot be used together.</li>
 *   <li>"topic", "partition_count", and "replication_factor" are required. The expected load of each partition (i.e.
 *   "partition_disk_mb", "partition_nw_in_kbps", and "partition_nw_out_kbps") defaults to 0.</li>
 * </ul>
 *
 * <pre>
 *    POST /kafkacruisecontrol/create_topic?json=[true/false]&amp;verbose=[true/false]&amp;topic=[topic]
 *    &amp;partition_count=[POSITIVE-INTEGER]&amp;replication_factor=[POSITIVE-INTEGER]
 *    &amp;partition_disk_mb=[NON-NEGATIVE-NUMBER]&amp;partition_nw_in_kbps=[NON-NEGATIVE-NUMBER]
 *    &amp;partition_nw_out_kbps=[NON-NEGATIVE-NUMBER]&amp;allow_other_topics_movements=[true/false]
 *    &amp;skip_rack_awareness_check=[true/false]&amp;dryRun=[true/false]&amp;goals=[goal1,goal2...]
 *    &amp;skip_hard_goal_check=[true/false]&amp;excluded_topics=[pattern]&amp;allow_capacity_estimation=[true/false]
 *    &amp;concurrent_partition_movements_per_broker=[POSITIVE-INTEGER]&amp;max_partition_movements_in_cluster=[POSITIVE-INTEGER]
 *    &amp;concurrent_leader_movements=[POSITIVE-INTEGER]&amp;broker_concurrent_leader_movements=[POSITIVE-INTEGER]
 *    &amp;exclude_recently_demoted_brokers=[true/false]&amp;exclude_recently_removed_brokers=[true/false]
 *    &amp;replica_movement_strategies=[strategy1,strategy2...]&amp;review_id=[id]&amp;replication_throttle=[bytes_per_second]
 *    &amp;execution_progress_check_interval_ms=[interval_in_ms]&amp;reason=[reason-for-request]
 *    &amp;stop_ongoing_execution=[true/false]&amp;get_response_schema=[true/false]&amp;fast_mode=[true/false]&amp;doAs=[user]
 * </pre>
 */
public class CreateTopicParameters extends GoalBasedOptimizationParameters {
  protected static final SortedSet<String> CASE_INSENSITIVE_PARAMETER_NAMES;
  static {
    SortedSet<String> validParameterNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    validParameterNames.add(TOPIC_PARAM);
    validParameterNames.add(PARTITION_COUNT);
    validParameterNames.add(REPLICATION_FACTOR_PARAM);
    validParameterNames.add(PARTITION_DISK_MB_PARAM);
    validParameterNames.add(PARTITION_NW_IN_KBPS_PARAM);
    validParameterNames.add(PARTITION_NW_OUT_KBPS_PARAM);
    validParameterNames.add(ALLOW_OTHER_TOPICS_MOVEMENTS_PARAM);
    validParameterNames.add(SKIP_RACK_AWARENESS_CHECK_PARAM);
    validParameterNames.add(DRY_RUN_PARAM);
    validParameterNames.add(REASON_PARAM);
    validParameterNames.add(STOP_ONGOING_EXECUTION_PARAM);
    validParameterNames.add(REVIEW_ID_PARAM);
    validParameterNames.add(SKIP_HARD_GOAL_CHECK_PARAM);
    validParameterNames.add(CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_PARAM);
    validParameterNames.add(MAX_PARTITION_MOVEMENTS_IN_CLUSTER_PARAM);
    validParameterNames.add(CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.add(BROKER_CONCURRENT_LEADER_MOVEMENTS_PARAM);
    validParameterNames.add(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_PARAM);
    validParameterNames.add(REPLICA_MOVEMENT_STRATEGIES_PARAM);
    validParameterNames.add(REPLICATION_THROTTLE_PARAM);
    validParameterNames.addAll(GoalBasedOptimizationParameters.CASE_INSENSITIVE_PARAMETER_NAMES);
    CASE_INSENSITIVE_PARAMETER_NAMES = Collections.unmodifiableSortedSet(validParameterNames);
  }
  protected String _topic;
  protected int _partitionCount;
  protected short _replicationFactor;
  protected double _partitionDiskMb;
  protected double _partitionNwInKbps;
  protected double _partitionNwOutKbps;
  protected boolean _allowOtherTopicsMovements;
  protected boolean _skipRackAwarenessCheck;
  protected Integer _reviewId;
  protected boolean _dryRun;
  protected String _reason;
  protected boolean _stopOngoingExecution;
  protected boolean _skipHardGoalCheck;
  protected Integer _concurrentInterBrokerPartitionMovements;
  protected Integer _maxInterBrokerPartitionMovements;
  protected Integer _clusterLeaderMovementConcurrency;
  protected Integer _brokerLeaderMovementConcurrency;
  protected Long _executionProgressCheckIntervalMs;
  protected ReplicaMovementStrategy _replicaMovementStrategy;
  protected Long _replicationThrottle;

  public CreateTopicParameters() {
    super();
  }

  @Override
  protected void initParameters() throws UnsupportedEncodingException {
    super.initParameters();
    _topic = ParameterUtils.topicToCreate(_requestContext);
    if (_topic == null) {
      throw new UserRequestException(String.format("The topic to create must be specified via %s parameter.", TOPIC_PARAM));
    }
    _partitionCount = ParameterUtils.partitionCount(_requestContext);
    if (_partitionCount == ProvisionRecommendation.DEFAULT_OPTIONAL_INT) {
      throw new UserRequestException(String.format("The partition count of the topic to create must be specified via %s parameter.",
                                                   PARTITION_COUNT));
    }
    Short replicationFactor = ParameterUtils.replicationFactor(_requestContext);
    if (replicationFactor == null || replicationFactor < 1) {
      throw new UserRequestException(String.format("The replication factor of the topic to create must be specified as a positive "
                                                   + "integer via %s parameter (Requested: %s).", REPLICATION_FACTOR_PARAM,
                                                   replicationFactor));
    }
    _replicationFactor = replicationFactor;
    _partitionDiskMb = ParameterUtils.partitionLoad(_requestContext, PARTITION_DISK_MB_PARAM);
    _partitionNwInKbps = ParameterUtils.partitionLoad(_requestContext, PARTITION_NW_IN_KBPS_PARAM);
    _partitionNwOutKbps = ParameterUtils.partitionLoad(_requestContext, PARTITION_NW_OUT_KBPS_PARAM);
    _allowOtherTopicsMovements = ParameterUtils.allowOtherTopicsMovements(_requestContext);
    _skipRackAwarenessCheck = ParameterUtils.skipRackAwarenessCheck(_requestContext);
    boolean twoStepVerificationEnabled = _config.getBoolean(WebServerConfig.TWO_STEP_VERIFICATION_ENABLED_CONFIG);
    _reviewId = ParameterUtils.reviewId(_requestContext, twoStepVerificationEnabled);
    _dryRun = ParameterUtils.getDryRun(_requestContext);
    boolean requestReasonRequired = _config.getBoolean(ExecutorConfig.REQUEST_REASON_REQUIRED_CONFIG);
    _reason = ParameterUtils.reason(_requestContext, requestReasonRequired && !_dryRun);
    _stopOngoingExecution = ParameterUtils.stopOngoingExecution(_requestContext);
    if (_stopOngoingExecution && _dryRun) {
      throw new UserRequestException(String.format("%s and %s cannot both be set to true.", STOP_ONGOING_EXECUTION_PARAM, DRY_RUN_PARAM));
    }
    _skipHardGoalCheck = ParameterUtils.skipHardGoalCheck(_requestContext);
    _concurrentInterBrokerPartitionMovements = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.INTER_BROKER_REPLICA);
    _maxInterBrokerPartitionMovements = ParameterUtils.maxPartitionMovements(_requestContext);
    _clusterLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_CLUSTER);
    _brokerLeaderMovementConcurrency = ParameterUtils.concurrentMovements(_requestContext, ConcurrencyType.LEADERSHIP_BROKER);
    _executionProgressCheckIntervalMs = ParameterUtils.executionProgressCheckIntervalMs(_requestContext);
    _replicaMovementStrategy = ParameterUtils.getReplicaMovementStrategy(_requestContext, _config);
    _replicationThrottle = ParameterUtils.replicationThrottle(_requestContext, _config);
  }

  @Override
  public void setReviewId(int reviewId) {
    _reviewId = reviewId;
  }

  public Integer reviewId() {
    return _reviewId;
  }

  public String topic() {
    return _topic;
  }

  public int partitionCount() {
    return _partitionCount;
  }

  public short replicationFactor() {
    return _replicationFactor;
  }

  public double partitionDiskMb() {
    return _partitionDiskMb;
  }

  public double partitionNwInKbps() {
    return _partitionNwInKbps;
  }

  public double partitionNwOutKbps() {
    return _partitionNwOutKbps;
  }

  public boolean allowOtherTopicsMovements() {
    return _allowOtherTopicsMovements;
  }

  public boolean skipRackAwarenessCheck() {
    return _skipRackAwarenessCheck;
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

  public boolean skipHardGoalCheck() {
    return _skipHardGoalCheck;
  }

  public Integer concurrentInterBrokerPartitionMovements() {
    return _concurrentInterBrokerPartitionMovements;
  }

  public Integer maxInterBrokerPartitionMovements() {
    return _maxInterBrokerPartitionMovements;
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

  @Override
  public SortedSet<String> caseInsensitiveParameterNames() {
    return CASE_INSENSITIVE_PARAMETER_NAMES;
  }
}
