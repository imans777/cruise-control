/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;


/**
 * The result of {@link CruiseControlEndPoint#CREATE_TOPIC}: the optimization result of the cluster with the new topic, along
 * with the replica assignment of the new topic. Proposals in the optimization result are limited to the movements of the
 * existing partitions.
 */
@JsonResponseClass
public class CreateTopicResult extends OptimizationResult {
  @JsonResponseField
  protected static final String TOPIC = "topic";
  @JsonResponseField
  protected static final String REPLICA_ASSIGNMENT = "replicaAssignment";
  protected final String _topic;
  protected final SortedMap<Integer, List<ReplicaPlacementInfo>> _replicaAssignment;

  /**
   * @param optimizerResult The optimization result of the cluster with the new topic.
   * @param topic The new topic.
   * @param replicaAssignment Replicas of each partition of the new topic by partition id, starting with the leader. Logdirs
   *                          of replicas are {@code null} unless their placement over disks has been decided.
   * @param config The configurations for Cruise Control.
   */
  public CreateTopicResult(OptimizerResult optimizerResult,
                           String topic,
                           Map<Integer, List<ReplicaPlacementInfo>> replicaAssignment,
                           KafkaCruiseControlConfig config) {
    super(optimizerResult, config);
    _topic = topic;
    _replicaAssignment = new TreeMap<>(replicaAssignment);
  }

  /**
   * @return The new topic.
   */
  public String topic() {
    return _topic;
  }

  /**
   * @return Replicas of each partition of the new topic by partition id, starting with the leader.
   */
  public SortedMap<Integer, List<ReplicaPlacementInfo>> replicaAssignment() {
    return _replicaAssignment;
  }

  @Override
  protected String getPlaintext(boolean isVerbose, String pretext) {
    StringBuilder sb = new StringBuilder(super.getPlaintext(isVerbose, pretext));
    sb.append(String.format("%n%nReplica assignment of topic %s (leader first):%n", _topic));
    _replicaAssignment.forEach((partition, replicas) -> sb.append(String.format("%s-%d: %s%n", _topic, partition, replicas)));
    return sb.toString();
  }

  @Override
  protected Map<String, Object> getJsonStructure(boolean isVerbose) {
    Map<String, Object> createTopicResult = super.getJsonStructure(isVerbose);
    createTopicResult.put(TOPIC, _topic);
    List<Map<String, Object>> replicaAssignment = new ArrayList<>(_replicaAssignment.size());
    _replicaAssignment.forEach((partition, replicas) -> replicaAssignment.add(new PartitionAssignment(partition, replicas).getJsonStructure()));
    createTopicResult.put(REPLICA_ASSIGNMENT, replicaAssignment);
    return createTopicResult;
  }

  @JsonResponseClass
  protected static class PartitionAssignment {
    @JsonResponseField
    protected static final String PARTITION = "partition";
    @JsonResponseField
    protected static final String REPLICAS = "replicas";
    @JsonResponseField(required = false)
    protected static final String LOGDIRS = "logdirs";
    protected final int _partition;
    protected final List<ReplicaPlacementInfo> _replicas;

    PartitionAssignment(int partition, List<ReplicaPlacementInfo> replicas) {
      _partition = partition;
      _replicas = replicas;
    }

    protected Map<String, Object> getJsonStructure() {
      Map<String, Object> partitionAssignment = new HashMap<>();
      partitionAssignment.put(PARTITION, _partition);
      partitionAssignment.put(REPLICAS, _replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList()));
      if (_replicas.stream().anyMatch(r -> r.logdir() != null)) {
        partitionAssignment.put(LOGDIRS, _replicas.stream().map(ReplicaPlacementInfo::logdir).collect(Collectors.toList()));
      }
      return partitionAssignment;
    }
  }
}
