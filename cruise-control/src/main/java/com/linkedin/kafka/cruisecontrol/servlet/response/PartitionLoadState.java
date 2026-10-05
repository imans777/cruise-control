/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

@JsonResponseClass
public class PartitionLoadState extends AbstractCruiseControlResponse {
  @JsonResponseField
  protected static final String RECORDS = "records";
  protected static final String UNKNOWN_LOG_DIR = "unknown";
  protected final List<Partition> _sortedPartitions;
  protected final boolean _wantMaxLoad;
  protected final boolean _wantAvgLoad;
  protected final int _entries;
  protected final int _partitionUpperBoundary;
  protected final int _partitionLowerBoundary;
  protected final int _topicNameLength;
  protected Pattern _topic;
  protected Map<TopicPartition, Map<Integer, String>> _logDirByReplica;

  public PartitionLoadState(List<Partition> sortedPartitions,
                            boolean wantMaxLoad,
                            boolean wantAvgLoad,
                            int entries,
                            int partitionUpperBoundary,
                            int partitionLowerBoundary,
                            Pattern topic,
                            int topicNameLength,
                            KafkaCruiseControlConfig config) {
    this(sortedPartitions, wantMaxLoad, wantAvgLoad, entries, partitionUpperBoundary, partitionLowerBoundary, topic, topicNameLength,
         config, null);
  }

  /**
   * @param sortedPartitions Partitions sorted by the requested resource utilization.
   * @param wantMaxLoad {@code true} to show the max load, {@code false} otherwise.
   * @param wantAvgLoad {@code true} to show the average load, {@code false} otherwise.
   * @param entries Maximum number of partitions to show.
   * @param partitionUpperBoundary Upper boundary of the partition ids to show.
   * @param partitionLowerBoundary Lower boundary of the partition ids to show.
   * @param topic Pattern of topics to show, or {@code null} to show all topics.
   * @param topicNameLength The length of the topic name column in the plaintext response.
   * @param config The configurations for Cruise Control.
   * @param logDirByReplica The current log directory by broker by partition to show the log directory of each replica, or {@code null}
   *                        to not show log directories.
   */
  public PartitionLoadState(List<Partition> sortedPartitions,
                            boolean wantMaxLoad,
                            boolean wantAvgLoad,
                            int entries,
                            int partitionUpperBoundary,
                            int partitionLowerBoundary,
                            Pattern topic,
                            int topicNameLength,
                            KafkaCruiseControlConfig config,
                            Map<TopicPartition, Map<Integer, String>> logDirByReplica) {
    super(config);
    _sortedPartitions = sortedPartitions;
    _wantMaxLoad = wantMaxLoad;
    _wantAvgLoad = wantAvgLoad;
    _entries = entries;
    _partitionUpperBoundary = partitionUpperBoundary;
    _partitionLowerBoundary = partitionLowerBoundary;
    _topic = topic;
    _topicNameLength = topicNameLength;
    _logDirByReplica = logDirByReplica;
  }

  private String logDir(Partition partition, int brokerId) {
    String logDir = _logDirByReplica.getOrDefault(partition.topicPartition(), Collections.emptyMap()).get(brokerId);
    return logDir == null ? UNKNOWN_LOG_DIR : logDir;
  }

  private List<String> followerLogDirs(Partition partition) {
    return partition.followers().stream().map(replica -> logDir(partition, replica.broker().id())).collect(Collectors.toList());
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%" + _topicNameLength + "s%10s%30s%20s%20s%20s%20s%20s", "PARTITION", "LEADER", "FOLLOWERS",
                            "CPU (%_CORES)", "DISK (MB)", "NW_IN (KB/s)", "NW_OUT (KB/s)", "MSG_IN (#/s)"));
    if (_logDirByReplica != null) {
      sb.append(String.format("  %-30s%s", "LEADER_LOGDIR", "FOLLOWER_LOGDIRS"));
    }
    sb.append(String.format("%n"));
    int numEntries = 0;
    for (Partition p : _sortedPartitions) {
      if (shouldSkipPartition(p)) {
        continue;
      }
      if (++numEntries > _entries) {
        break;
      }
      List<Integer> followers = p.followers().stream().map(replica -> replica.broker().id()).collect(Collectors.toList());
      sb.append(String.format("%" + _topicNameLength + "s%10s%30s%19.6f%19.3f%19.3f%19.3f%19.3f",
                              p.leader().topicPartition(),
                              p.leader().broker().id(),
                              followers,
                              p.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad, _wantAvgLoad),
                              p.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad, _wantAvgLoad)));
      if (_logDirByReplica != null) {
        sb.append(String.format("  %-30s%s", logDir(p, p.leader().broker().id()), followerLogDirs(p)));
      }
      sb.append(String.format("%n"));
    }
    return sb.toString();
  }

  /**
   * Skips the partition if it does not match the requested topic pattern, or is out of the requested partition scope.
   *
   * @param partition Partition to check whether be included in the response.
   * @return {@code true} to skip partition, {@code false} otherwise.
   */
  private boolean shouldSkipPartition(Partition partition) {
    return (_topic != null && !_topic.matcher(partition.topicPartition().topic()).matches())
           || partition.topicPartition().partition() < _partitionLowerBoundary
           || partition.topicPartition().partition() > _partitionUpperBoundary;
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    // Cache relevant response.
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
    // Discard irrelevant response.
    _sortedPartitions.clear();
    _topic = null;
    _logDirByReplica = null;
  }

  protected String getJsonString() {
    Map<String, Object> partitionMap = new HashMap<>();
    List<Object> partitionList = new ArrayList<>();
    partitionMap.put(VERSION, JSON_VERSION);
    int numEntries = 0;
    for (Partition p : _sortedPartitions) {
      if (shouldSkipPartition(p)) {
        continue;
      }
      if (++numEntries > _entries) {
        break;
      }
      partitionList.add(new PartitionLoadRecord(p).getJsonStructure());
    }
    partitionMap.put(RECORDS, partitionList);
    Gson gson = new Gson();
    return gson.toJson(partitionMap);
  }

  @JsonResponseClass
  @JsonResponseExternalFields(Resource.class)
  protected class PartitionLoadRecord {
    @JsonResponseField
    protected static final String TOPIC = "topic";
    @JsonResponseField
    protected static final String PARTITION = "partition";
    @JsonResponseField
    protected static final String LEADER = "leader";
    @JsonResponseField
    protected static final String FOLLOWERS = "followers";
    @JsonResponseField
    protected static final String MSG_IN = "msg_in";
    @JsonResponseField(required = false)
    protected static final String LEADER_LOGDIR = "leaderLogdir";
    @JsonResponseField(required = false)
    protected static final String FOLLOWER_LOGDIRS = "followerLogdirs";
    protected Partition _partition;

    PartitionLoadRecord(Partition partition) {
      _partition = partition;
    }

    protected Map<String, Object> getJsonStructure() {
      List<Integer> followers = _partition.followers().stream().map(replica -> replica.broker().id()).collect(Collectors.toList());
      Map<String, Object> record = new HashMap<>(Map.of(
          TOPIC, _partition.leader().topicPartition().topic(), PARTITION, _partition.leader().topicPartition().partition(),
          LEADER, _partition.leader().broker().id(), FOLLOWERS, followers,
          Resource.CPU.resource(), _partition.leader().load().expectedUtilizationFor(Resource.CPU, _wantMaxLoad, _wantAvgLoad),
          Resource.DISK.resource(), _partition.leader().load().expectedUtilizationFor(Resource.DISK, _wantMaxLoad, _wantAvgLoad),
          Resource.NW_IN.resource(), _partition.leader().load().expectedUtilizationFor(Resource.NW_IN, _wantMaxLoad, _wantAvgLoad),
          Resource.NW_OUT.resource(), _partition.leader().load().expectedUtilizationFor(Resource.NW_OUT, _wantMaxLoad, _wantAvgLoad),
          MSG_IN, _partition.leader().load().expectedUtilizationFor(KafkaMetricDef.MESSAGE_IN_RATE, _wantMaxLoad, _wantAvgLoad)));
      if (_logDirByReplica != null) {
        record.put(LEADER_LOGDIR, logDir(_partition, _partition.leader().broker().id()));
        record.put(FOLLOWER_LOGDIRS, followerLogDirs(_partition));
      }
      return record;
    }
  }
}
