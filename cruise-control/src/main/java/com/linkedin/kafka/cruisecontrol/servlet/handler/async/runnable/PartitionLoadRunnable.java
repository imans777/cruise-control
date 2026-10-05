/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.PartitionLoadParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.PartitionLoadState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG;
import static com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig.MIN_VALID_PARTITION_RATIO_CONFIG;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.withoutTrailingSeparator;


/**
 * The async runnable to get partition load in the cluster.
 */
public class PartitionLoadRunnable extends OperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionLoadRunnable.class);
  protected final PartitionLoadParameters _parameters;

  public PartitionLoadRunnable(KafkaCruiseControl kafkaCruiseControl,
                               OperationFuture future,
                               PartitionLoadParameters parameters) {
    super(kafkaCruiseControl, future);
    _parameters = parameters;
  }

  @Override
  protected PartitionLoadState getResult() throws Exception {
    _kafkaCruiseControl.sanityCheckBrokerPresence(_parameters.brokerIds());
    _kafkaCruiseControl.sanityCheckBrokerPresence(_parameters.brokerIdAndLogdirs().keySet());

    LoadRunnable loadRunnable = new LoadRunnable(_kafkaCruiseControl, _future, _parameters);
    Double minValidPartitionRatio = _parameters.minValidPartitionRatio();
    if (minValidPartitionRatio == null) {
      minValidPartitionRatio = _kafkaCruiseControl.config().getDouble(MIN_VALID_PARTITION_RATIO_CONFIG);
    }
    ClusterModel clusterModel = loadRunnable.clusterModel(minValidPartitionRatio);
    int topicNameLength = clusterModel.topics().stream().mapToInt(String::length).max().orElse(20) + 5;
    List<Partition> partitionList = clusterModel.replicasSortedByUtilization(_parameters.resource(),
                                                                             _parameters.wantMaxLoad(),
                                                                             _parameters.wantAvgLoad());
    if (!_parameters.brokerIds().isEmpty()) {
      partitionList = partitionList.stream()
                                   .filter(partition -> partition.partitionBrokers().stream().anyMatch(
                                       broker -> _parameters.brokerIds().contains(broker.id())))
                                   .collect(Collectors.toList());
    }
    Map<TopicPartition, Map<Integer, String>> logDirByReplica = null;
    if (_parameters.populateDiskInfo()) {
      partitionList = partitionList.stream().filter(p -> isInScope(p, _parameters.topic(), _parameters.partitionLowerBoundary(),
                                                                   _parameters.partitionUpperBoundary())).collect(Collectors.toList());
      logDirByReplica = logDirByReplica(partitionList);
      if (!_parameters.brokerIdAndLogdirs().isEmpty()) {
        partitionList = partitionsOnLogDirs(partitionList, logDirByReplica, _parameters.brokerIdAndLogdirs());
      }
    }
    return new PartitionLoadState(partitionList,
                                  _parameters.wantMaxLoad(),
                                  _parameters.wantAvgLoad(),
                                  _parameters.entries(),
                                  _parameters.partitionUpperBoundary(),
                                  _parameters.partitionLowerBoundary(),
                                  _parameters.topic(),
                                  topicNameLength,
                                  _kafkaCruiseControl.config(),
                                  logDirByReplica);
  }

  /**
   * @param partitions Partitions whose replicas to get the log directory of.
   * @return The current log directory by broker by partition, as reported by Kafka.
   */
  private Map<TopicPartition, Map<Integer, String>> logDirByReplica(List<Partition> partitions) {
    Set<Integer> brokers = partitions.stream().flatMap(p -> p.partitionBrokers().stream()).map(Broker::id).collect(Collectors.toSet());
    Map<Integer, String> errorByBroker = new HashMap<>();
    Map<Integer, Map<String, LogDirDescription>> logDirsByBroker =
        RunnableUtils.describeLogDirs(_kafkaCruiseControl.adminClient(), brokers,
                                      _kafkaCruiseControl.config().getLong(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG), errorByBroker);
    if (!errorByBroker.isEmpty()) {
      LOG.warn("Log directories of replicas on brokers {} are unknown: {}", errorByBroker.keySet(), errorByBroker);
    }
    return RunnableUtils.currentLogDirByReplica(logDirsByBroker,
                                                partitions.stream().map(Partition::topicPartition).collect(Collectors.toSet()));
  }

  /**
   * @param partition Partition to check.
   * @param topic Topic pattern to match, or {@code null} to match all topics.
   * @param partitionLowerBoundary Lower boundary of the partition id to match.
   * @param partitionUpperBoundary Upper boundary of the partition id to match.
   * @return {@code true} if the given partition matches the given topic pattern and partition id boundaries, {@code false} otherwise.
   */
  static boolean isInScope(Partition partition, Pattern topic, int partitionLowerBoundary, int partitionUpperBoundary) {
    TopicPartition tp = partition.topicPartition();
    return (topic == null || topic.matcher(tp.topic()).matches())
           && tp.partition() >= partitionLowerBoundary && tp.partition() <= partitionUpperBoundary;
  }

  /**
   * @param partitions Partitions to filter.
   * @param logDirByReplica The current log directory by broker by partition.
   * @param logDirsByBroker Log directories of interest by broker.
   * @return Partitions with a replica on one of the log directories of interest, in the original order.
   */
  static List<Partition> partitionsOnLogDirs(List<Partition> partitions,
                                             Map<TopicPartition, Map<Integer, String>> logDirByReplica,
                                             Map<Integer, Set<String>> logDirsByBroker) {
    Map<Integer, Set<String>> normalizedLogDirsByBroker = new HashMap<>();
    logDirsByBroker.forEach((broker, logDirs) -> normalizedLogDirsByBroker.put(
        broker, logDirs.stream().map(RunnableUtils::withoutTrailingSeparator).collect(Collectors.toSet())));
    return partitions.stream().filter(p -> logDirByReplica.getOrDefault(p.topicPartition(), Collections.emptyMap()).entrySet().stream()
        .anyMatch(e -> normalizedLogDirsByBroker.getOrDefault(e.getKey(), Collections.emptySet())
                                                .contains(withoutTrailingSeparator(e.getValue()))))
                     .collect(Collectors.toList());
  }
}
