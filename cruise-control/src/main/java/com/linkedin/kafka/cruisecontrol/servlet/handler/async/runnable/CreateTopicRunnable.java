/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CreateTopicParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CreateTopicResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.CLIENT_REQUEST_TIMEOUT_MS;
import static com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RunnableUtils.computeOptimizationOptions;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ALLOW_OTHER_TOPICS_MOVEMENTS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.SKIP_RACK_AWARENESS_CHECK_PARAM;


/**
 * The async runnable to create a topic, whose replicas are placed based on the expected load of its partitions and the
 * current load of the cluster -- see {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#CREATE_TOPIC}.
 *
 * Replicas of the new topic are placed in the following steps.
 * <ol>
 *   <li>Tentatively add the replicas of the new topic with their expected load to the cluster model in a rack-aware (if
 *   possible), round-robin way.</li>
 *   <li>Optimize the location of the new replicas with the provided {@link Goal} list. Unless the movement of other topics
 *   is allowed, the optimization moves only the replicas of the new topic. Otherwise, it may also move the replicas of other
 *   topics, so that the cluster satisfies the goals after creating the topic.</li>
 *   <li>If the cluster uses JBOD (i.e. the disk capacity of every broker is specified per logdir, and some brokers have
 *   multiple logdirs), place each new replica on the alive disk of its broker that has the lowest utilization after the
 *   placement, without exceeding the disk capacity threshold.</li>
 * </ol>
 *
 * If it is not a dry run, the topic is created with this replica assignment, and then the movement of other topics' replicas
 * (if any) is executed.
 */
public class CreateTopicRunnable extends GoalBasedOperationRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTopicRunnable.class);
  protected static final boolean SKIP_AUTO_REFRESHING_CONCURRENCY = true;
  protected final String _topic;
  protected final int _partitionCount;
  protected final short _replicationFactor;
  protected final double _partitionDiskMb;
  protected final double _partitionNwInKbps;
  protected final double _partitionNwOutKbps;
  protected final boolean _allowOtherTopicsMovements;
  protected final boolean _skipRackAwarenessCheck;
  protected final Integer _concurrentInterBrokerPartitionMovements;
  protected final Integer _maxInterBrokerPartitionMovements;
  protected final Integer _clusterLeaderMovementConcurrency;
  protected final Integer _brokerLeaderMovementConcurrency;
  protected final Long _executionProgressCheckIntervalMs;
  protected final ReplicaMovementStrategy _replicaMovementStrategy;
  protected final Long _replicationThrottle;
  // Replicas of each partition of the new topic by partition id, starting with the leader.
  protected SortedMap<Integer, List<ReplicaPlacementInfo>> _replicaAssignment;

  public CreateTopicRunnable(KafkaCruiseControl kafkaCruiseControl,
                             OperationFuture future,
                             String uuid,
                             CreateTopicParameters parameters) {
    super(kafkaCruiseControl, future, parameters, parameters.dryRun(), parameters.stopOngoingExecution(), parameters.skipHardGoalCheck(),
          uuid, parameters::reason);
    _topic = parameters.topic();
    _partitionCount = parameters.partitionCount();
    _replicationFactor = parameters.replicationFactor();
    _partitionDiskMb = parameters.partitionDiskMb();
    _partitionNwInKbps = parameters.partitionNwInKbps();
    _partitionNwOutKbps = parameters.partitionNwOutKbps();
    _allowOtherTopicsMovements = parameters.allowOtherTopicsMovements();
    _skipRackAwarenessCheck = parameters.skipRackAwarenessCheck();
    _concurrentInterBrokerPartitionMovements = parameters.concurrentInterBrokerPartitionMovements();
    _maxInterBrokerPartitionMovements = parameters.maxInterBrokerPartitionMovements();
    _clusterLeaderMovementConcurrency = parameters.clusterLeaderMovementConcurrency();
    _brokerLeaderMovementConcurrency = parameters.brokerLeaderMovementConcurrency();
    _executionProgressCheckIntervalMs = parameters.executionProgressCheckIntervalMs();
    _replicaMovementStrategy = parameters.replicaMovementStrategy();
    _replicationThrottle = parameters.replicationThrottle();
    _replicaAssignment = null;
  }

  @Override
  protected CreateTopicResult getResult() throws Exception {
    OptimizerResult optimizerResult = computeResult();
    return new CreateTopicResult(optimizerResult, _topic, _replicaAssignment, _kafkaCruiseControl.config());
  }

  @Override
  protected void init() {
    super.init();
    if (_kafkaCruiseControl.kafkaCluster().topics().contains(_topic)) {
      throw new IllegalArgumentException(String.format("Topic %s already exists in the cluster.", _topic));
    }
  }

  @Override
  protected OptimizerResult workWithClusterModel() throws KafkaCruiseControlException, TimeoutException, NotEnoughValidWindowsException {
    ClusterModel clusterModel = _kafkaCruiseControl.clusterModel(_combinedCompletenessRequirements, _allowCapacityEstimation, _operationProgress);
    if (!clusterModel.isClusterAlive()) {
      throw new IllegalArgumentException("All brokers are dead in the cluster.");
    }

    // Compute optimization options before adding the new topic, so it is never excluded from replica movement.
    OptimizationOptions optimizationOptions = computeOptimizationOptions(clusterModel,
                                                                         false,
                                                                         _kafkaCruiseControl,
                                                                         Collections.emptySet(),
                                                                         _dryRun,
                                                                         _excludeRecentlyDemotedBrokers,
                                                                         _excludeRecentlyRemovedBrokers,
                                                                         _excludedTopics,
                                                                         Collections.emptySet(),
                                                                         !_allowOtherTopicsMovements,
                                                                         _fastMode);
    if (!_allowOtherTopicsMovements) {
      // Some goals (e.g. leadership goals) do not restrict their movements to immigrant replicas, but all goals respect excluded topics.
      optimizationOptions = new OptimizationOptions(clusterModel.topics(),
                                                    optimizationOptions.excludedBrokersForLeadership(),
                                                    optimizationOptions.excludedBrokersForReplicaMove(),
                                                    optimizationOptions.isTriggeredByGoalViolation(),
                                                    optimizationOptions.requestedDestinationBrokerIds(),
                                                    optimizationOptions.onlyMoveImmigrantReplicas(),
                                                    optimizationOptions.fastMode());
    }

    // Proposals are generated against the replica distribution before adding the new topic; hence, they exclude the new topic.
    Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaDistribution = clusterModel.getReplicaDistribution();
    clusterModel.createReplicasForNewTopic(_topic, _partitionCount, _replicationFactor, _partitionDiskMb, _partitionNwInKbps,
                                           _partitionNwOutKbps, brokersByRackForNewTopic(clusterModel, optimizationOptions));
    OptimizerResult result = _kafkaCruiseControl.optimizations(clusterModel, _goalsByPriority, _operationProgress,
                                                               initReplicaDistribution, optimizationOptions);
    SortedMap<Integer, List<Integer>> brokersByPartition = brokersByPartition(clusterModel, _topic);
    if (_kafkaCruiseControl.loadMonitor().isUsingJBOD(_allowCapacityEstimation)) {
      ClusterModel diskClusterModel = _kafkaCruiseControl.clusterModel(DEFAULT_START_TIME_FOR_CLUSTER_MODEL,
                                                                       _kafkaCruiseControl.timeMs(),
                                                                       _combinedCompletenessRequirements,
                                                                       true,
                                                                       _allowCapacityEstimation,
                                                                       _operationProgress);
      double diskCapacityThreshold = new BalancingConstraint(_kafkaCruiseControl.config()).capacityThreshold(Resource.DISK);
      _replicaAssignment = placeReplicasOnDisks(_topic, brokersByPartition, diskClusterModel, _partitionDiskMb, diskCapacityThreshold);
    } else {
      _replicaAssignment = new TreeMap<>();
      brokersByPartition.forEach((partition, brokers) -> _replicaAssignment.put(
          partition, brokers.stream().map(ReplicaPlacementInfo::new).collect(Collectors.toList())));
    }

    if (!_dryRun) {
      try {
        createTopic(_kafkaCruiseControl.adminClient(), _topic, _replicaAssignment,
                    _kafkaCruiseControl.config().getLong(ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG));
      } catch (ExecutionException | InterruptedException e) {
        throw new KafkaCruiseControlException(String.format("Failed to create topic %s.", _topic), e);
      }
      _kafkaCruiseControl.executeProposals(result.goalProposals(), Collections.emptySet(), false, _concurrentInterBrokerPartitionMovements,
                                           _maxInterBrokerPartitionMovements, 0, _clusterLeaderMovementConcurrency,
                                           _brokerLeaderMovementConcurrency, _executionProgressCheckIntervalMs, _replicaMovementStrategy,
                                           _replicationThrottle, _isTriggeredByUserRequest, _uuid, SKIP_AUTO_REFRESHING_CONCURRENCY);
    }
    return result;
  }

  /**
   * Get the alive brokers to tentatively add the replicas of the new topic to, by rack. Brokers excluded from replica moves
   * are skipped, unless doing so leaves fewer brokers than the replication factor. This method also conducts a sanity check
   * to ensure that there are enough brokers and racks in the cluster for the replication factor of the new topic.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   * @return Alive brokers to tentatively add the replicas of the new topic to, by rack.
   */
  protected Map<String, List<Integer>> brokersByRackForNewTopic(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    Set<Broker> aliveBrokers = clusterModel.aliveBrokers();
    if (_replicationFactor > aliveBrokers.size()) {
      throw new IllegalArgumentException(String.format("Unable to create topic %s with replication factor %d since there are only %d "
                                                       + "alive brokers in the cluster.", _topic, _replicationFactor, aliveBrokers.size()));
    }
    Set<String> aliveRacks = aliveBrokers.stream().map(b -> b.rack().id()).collect(Collectors.toSet());
    if (_replicationFactor > aliveRacks.size()) {
      if (!_skipRackAwarenessCheck) {
        throw new IllegalArgumentException(String.format("Unable to create topic %s with replication factor %d since there are only %d "
                                                         + "racks in the cluster, to skip the rack-awareness check, set %s to true in "
                                                         + "the request.", _topic, _replicationFactor, aliveRacks.size(),
                                                         SKIP_RACK_AWARENESS_CHECK_PARAM));
      }
      LOG.info("Replication factor of topic {} is {}, which is larger than the number of racks in cluster. Hence, the same rack will "
               + "contain more than one replicas from the same partition.", _topic, _replicationFactor);
    }

    Set<Integer> excludedBrokers = optimizationOptions.excludedBrokersForReplicaMove();
    List<Broker> candidateBrokers = aliveBrokers.stream().filter(b -> !excludedBrokers.contains(b.id())).collect(Collectors.toList());
    if (candidateBrokers.size() < _replicationFactor) {
      candidateBrokers = new ArrayList<>(aliveBrokers);
    }
    Map<String, List<Integer>> brokersByRack = new TreeMap<>();
    candidateBrokers.stream().sorted(Comparator.comparingInt(Broker::id))
                    .forEach(b -> brokersByRack.computeIfAbsent(b.rack().id(), r -> new ArrayList<>()).add(b.id()));
    return brokersByRack;
  }

  /**
   * @param clusterModel The state of the cluster.
   * @param topic The topic whose replica assignment is requested.
   * @return Brokers hosting the replicas of each partition of the given topic by partition id, starting with the leader.
   */
  static SortedMap<Integer, List<Integer>> brokersByPartition(ClusterModel clusterModel, String topic) {
    SortedMap<Integer, List<Integer>> brokersByPartition = new TreeMap<>();
    for (Partition partition : clusterModel.getPartitionsByTopic().get(topic)) {
      List<Integer> brokers = new ArrayList<>(partition.replicas().size());
      brokers.add(partition.leader().broker().id());
      for (Replica follower : partition.followers()) {
        brokers.add(follower.broker().id());
      }
      brokersByPartition.put(partition.topicPartition().partition(), brokers);
    }
    return brokersByPartition;
  }

  /**
   * Place each replica of the new topic on a disk of its broker. Replicas are placed one by one, each on the alive disk of
   * its broker that has the lowest utilization after the placement, without exceeding the disk capacity threshold. Ties
   * are broken in favor of the disk with fewer replicas of the new topic. Logdirs of replicas on brokers with a single
   * logdir are left {@code null}.
   *
   * @param topic The new topic.
   * @param brokersByPartition Brokers hosting the replicas of each partition of the new topic by partition id.
   * @param diskClusterModel The state of the cluster, including the replica placement over disks.
   * @param replicaDiskMb Expected disk usage of each replica of the new topic in MB.
   * @param diskCapacityThreshold The disk capacity threshold.
   * @return Replicas of each partition of the new topic by partition id, along with their logdir.
   */
  static SortedMap<Integer, List<ReplicaPlacementInfo>> placeReplicasOnDisks(String topic,
                                                                             SortedMap<Integer, List<Integer>> brokersByPartition,
                                                                             ClusterModel diskClusterModel,
                                                                             double replicaDiskMb,
                                                                             double diskCapacityThreshold) {
    Map<Disk, Double> addedUtilizationByDisk = new HashMap<>();
    Map<Disk, Integer> numNewReplicasByDisk = new HashMap<>();
    SortedMap<Integer, List<ReplicaPlacementInfo>> replicaAssignment = new TreeMap<>();
    for (Map.Entry<Integer, List<Integer>> entry : brokersByPartition.entrySet()) {
      int partition = entry.getKey();
      List<ReplicaPlacementInfo> replicas = new ArrayList<>(entry.getValue().size());
      for (int brokerId : entry.getValue()) {
        Broker broker = diskClusterModel.broker(brokerId);
        if (broker == null || !broker.isAlive()) {
          throw new IllegalStateException(String.format("Broker %d to host a replica of %s-%d is not alive.", brokerId, topic, partition));
        }
        Comparator<Disk> diskComparator =
            Comparator.comparingDouble((Disk d) -> utilizationAfterPlacement(d, addedUtilizationByDisk, replicaDiskMb))
                      .thenComparingInt(d -> numNewReplicasByDisk.getOrDefault(d, 0))
                      .thenComparing(Disk::logDir);
        Disk disk = broker.disks().stream()
                          .filter(d -> d.isAlive() && d.capacity() > 0)
                          .filter(d -> utilizationAfterPlacement(d, addedUtilizationByDisk, replicaDiskMb) <= diskCapacityThreshold)
                          .min(diskComparator)
                          .orElseThrow(() -> new IllegalStateException(String.format(
                              "Unable to place a replica of %s-%d (%.2f MB) on any disk of broker %d without exceeding the disk "
                              + "capacity threshold (%.2f). Consider setting %s to true in the request.", topic, partition,
                              replicaDiskMb, brokerId, diskCapacityThreshold, ALLOW_OTHER_TOPICS_MOVEMENTS_PARAM)));
        addedUtilizationByDisk.merge(disk, replicaDiskMb, Double::sum);
        numNewReplicasByDisk.merge(disk, 1, Integer::sum);
        // Kafka can only use the logdir of a broker with a single logdir.
        replicas.add(new ReplicaPlacementInfo(brokerId, broker.disks().size() > 1 ? disk.logDir() : null));
      }
      replicaAssignment.put(partition, replicas);
    }
    return replicaAssignment;
  }

  private static double utilizationAfterPlacement(Disk disk, Map<Disk, Double> addedUtilizationByDisk, double replicaDiskMb) {
    return (disk.utilization() + addedUtilizationByDisk.getOrDefault(disk, 0.0) + replicaDiskMb) / disk.capacity();
  }

  /**
   * Create the topic with the given replica assignment. If logdirs of replicas are specified, brokers are requested to create
   * the replicas on their logdirs.
   *
   * @param adminClient The adminClient to send requests to the Kafka cluster.
   * @param topic The topic to create.
   * @param replicaAssignment Replicas of each partition of the topic by partition id, starting with the leader.
   * @param logdirResponseTimeoutMs Timeout in ms for brokers to respond to the requests to alter logdirs of replicas.
   */
  static void createTopic(Admin adminClient,
                          String topic,
                          SortedMap<Integer, List<ReplicaPlacementInfo>> replicaAssignment,
                          long logdirResponseTimeoutMs) throws ExecutionException, InterruptedException, TimeoutException {
    Map<Integer, List<Integer>> brokersByPartition = new HashMap<>();
    Map<TopicPartitionReplica, String> logdirByReplica = new HashMap<>();
    replicaAssignment.forEach((partition, replicas) -> {
      brokersByPartition.put(partition, replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList()));
      replicas.stream().filter(r -> r.logdir() != null)
              .forEach(r -> logdirByReplica.put(new TopicPartitionReplica(topic, partition, r.brokerId()), r.logdir()));
    });

    if (!logdirByReplica.isEmpty()) {
      // Brokers keep the requested logdir of a replica that does not exist yet, and create the replica on that logdir. Hence,
      // a missing replica is an expected failure here; but any other failure (e.g. an offline logdir) aborts topic creation.
      for (Map.Entry<TopicPartitionReplica, KafkaFuture<Void>> entry : adminClient.alterReplicaLogDirs(logdirByReplica).values().entrySet()) {
        try {
          entry.getValue().get(logdirResponseTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof ReplicaNotAvailableException)) {
            throw e;
          }
        }
      }
    }

    adminClient.createTopics(Collections.singletonList(new NewTopic(topic, brokersByPartition))).all()
               .get(CLIENT_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    LOG.info("Topic {} has been created with replica assignment {}.", topic, replicaAssignment);

    if (!logdirByReplica.isEmpty()) {
      // Move replicas that might have been created on another logdir before brokers received their requested logdir. The topic is
      // already created at this point; hence, failures are only logged.
      Set<TopicPartitionReplica> replicasWithUnconfirmedLogdir = new HashSet<>();
      for (Map.Entry<TopicPartitionReplica, KafkaFuture<Void>> entry : adminClient.alterReplicaLogDirs(logdirByReplica).values().entrySet()) {
        try {
          entry.getValue().get(logdirResponseTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
          if (!(e.getCause() instanceof ReplicaNotAvailableException)) {
            LOG.warn("Failed to alter the logdir of replica {} to {}.", entry.getKey(), logdirByReplica.get(entry.getKey()), e);
          }
          replicasWithUnconfirmedLogdir.add(entry.getKey());
        }
      }
      if (!replicasWithUnconfirmedLogdir.isEmpty()) {
        LOG.warn("Logdirs of replicas {} of topic {} are not confirmed by brokers.", replicasWithUnconfirmedLogdir, topic);
      }
    }
  }

  @Override
  protected boolean shouldWorkWithClusterModel() {
    return true;
  }

  @Override
  protected OptimizerResult workWithoutClusterModel() {
    return null;
  }
}
