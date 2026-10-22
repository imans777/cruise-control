/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CreateTopicParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.CreateTopicResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterReplicaLogDirsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class CreateTopicRunnableTest {
  private static final String TOPIC = "existing_topic";
  private static final String NEW_TOPIC = "new_topic";
  private static final String LOGDIR0 = "/mnt/d0";
  private static final String LOGDIR1 = "/mnt/d1";
  private static final double REPLICA_DISK_MB = 100.0;
  // Disk capacity threshold is 0.8 by default -- i.e. up to 800 MB of each broker can be used.
  private static final Map<Resource, Double> BROKER_CAPACITY = Map.of(Resource.CPU, 100.0, Resource.DISK, 1000.0,
                                                                      Resource.NW_IN, 10000.0, Resource.NW_OUT, 10000.0);
  private static final List<String> GOALS = List.of("RackAwareGoal", "ReplicaCapacityGoal", "DiskCapacityGoal", "NetworkInboundCapacityGoal",
                                                    "NetworkOutboundCapacityGoal", "ReplicaDistributionGoal", "DiskUsageDistributionGoal",
                                                    "LeaderReplicaDistributionGoal", "LeaderBytesInDistributionGoal");
  private static final KafkaCruiseControlConfig CONFIG =
      new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());

  /**
   * Create a cluster with a broker per rack, in which each partition of {@link #TOPIC} has a single replica.
   *
   * @param diskMbByBrokerByPartition Disk usage in MB of each partition of {@link #TOPIC} by its broker.
   * @param numBrokers Number of brokers in the cluster.
   * @return The cluster model.
   */
  private static ClusterModel clusterModel(List<Map.Entry<Integer, Double>> diskMbByBrokerByPartition, int numBrokers) {
    Map<Integer, Integer> rackByBroker = new HashMap<>();
    for (int brokerId = 0; brokerId < numBrokers; brokerId++) {
      rackByBroker.put(brokerId, brokerId);
    }
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(rackByBroker, BROKER_CAPACITY, null);
    for (int partition = 0; partition < diskMbByBrokerByPartition.size(); partition++) {
      int brokerId = diskMbByBrokerByPartition.get(partition).getKey();
      double diskMb = diskMbByBrokerByPartition.get(partition).getValue();
      String rack = Integer.toString(brokerId);
      TopicPartition tp = new TopicPartition(TOPIC, partition);
      clusterModel.createReplica(rack, brokerId, tp, 0, true);
      clusterModel.setReplicaLoad(rack, brokerId, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, diskMb), Collections.singletonList(1L));
    }
    return clusterModel;
  }

  private static CreateTopicParameters parameters(int partitionCount, short replicationFactor, boolean allowOtherTopicsMovements,
                                                  boolean dryRun) {
    CreateTopicParameters parameters = EasyMock.niceMock(CreateTopicParameters.class);
    EasyMock.expect(parameters.goals()).andReturn(GOALS).anyTimes();
    EasyMock.expect(parameters.modelCompletenessRequirements()).andReturn(new ModelCompletenessRequirements(1, 0.0, false)).anyTimes();
    EasyMock.expect(parameters.skipHardGoalCheck()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.fastMode()).andReturn(false).anyTimes();
    EasyMock.expect(parameters.dryRun()).andReturn(dryRun).anyTimes();
    EasyMock.expect(parameters.topic()).andReturn(NEW_TOPIC).anyTimes();
    EasyMock.expect(parameters.partitionCount()).andReturn(partitionCount).anyTimes();
    EasyMock.expect(parameters.replicationFactor()).andReturn(replicationFactor).anyTimes();
    EasyMock.expect(parameters.partitionDiskMb()).andReturn(REPLICA_DISK_MB).anyTimes();
    EasyMock.expect(parameters.partitionNwInKbps()).andReturn(10.0).anyTimes();
    EasyMock.expect(parameters.partitionNwOutKbps()).andReturn(10.0).anyTimes();
    EasyMock.expect(parameters.allowOtherTopicsMovements()).andReturn(allowOtherTopicsMovements).anyTimes();
    EasyMock.replay(parameters);
    return parameters;
  }

  /**
   * Mock Cruise Control to optimize the given cluster model with a goal optimizer.
   *
   * @param clusterModel The cluster model to optimize.
   * @param optimizationOptions Capture of the optimization options.
   * @param adminClient The admin client to create the topic.
   * @param executedProposals Capture of the executed proposals.
   * @return Mocked Cruise Control.
   */
  @SuppressWarnings("unchecked")
  private static KafkaCruiseControl kafkaCruiseControl(ClusterModel clusterModel,
                                                       Capture<OptimizationOptions> optimizationOptions,
                                                       Admin adminClient,
                                                       Capture<Set<ExecutionProposal>> executedProposals) throws Exception {
    GoalOptimizer goalOptimizer = new GoalOptimizer(CONFIG, null, Time.SYSTEM, new MetricRegistry(), EasyMock.mock(Executor.class),
                                                    EasyMock.mock(Admin.class));
    KafkaCruiseControl kafkaCruiseControl = EasyMock.niceMock(KafkaCruiseControl.class);
    EasyMock.expect(kafkaCruiseControl.config()).andReturn(CONFIG).anyTimes();
    EasyMock.expect(kafkaCruiseControl.kafkaCluster()).andReturn(DeterministicCluster.generateClusterFromClusterModel(clusterModel)).anyTimes();
    EasyMock.expect(kafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject()))
            .andReturn(new ModelCompletenessRequirements(1, 0.0, false)).anyTimes();
    EasyMock.expect(kafkaCruiseControl.clusterModel(EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject()))
            .andReturn(clusterModel).anyTimes();
    EasyMock.expect(kafkaCruiseControl.executorState())
            .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet())).anyTimes();
    EasyMock.expect(kafkaCruiseControl.excludedTopics(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Collections.emptySet()).anyTimes();
    LoadMonitor loadMonitor = EasyMock.niceMock(LoadMonitor.class);
    EasyMock.expect(loadMonitor.isUsingJBOD(EasyMock.anyBoolean())).andReturn(false).anyTimes();
    EasyMock.expect(kafkaCruiseControl.loadMonitor()).andReturn(loadMonitor).anyTimes();
    EasyMock.expect(kafkaCruiseControl.optimizations(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                                                     EasyMock.capture(optimizationOptions)))
            .andAnswer(() -> goalOptimizer.optimizations((ClusterModel) EasyMock.getCurrentArgument(0),
                                                         (List<Goal>) EasyMock.getCurrentArgument(1),
                                                         (OperationProgress) EasyMock.getCurrentArgument(2),
                                                         (Map<TopicPartition, List<ReplicaPlacementInfo>>) EasyMock.getCurrentArgument(3),
                                                         (OptimizationOptions) EasyMock.getCurrentArgument(4)));
    EasyMock.expect(kafkaCruiseControl.adminClient()).andReturn(adminClient).anyTimes();
    kafkaCruiseControl.executeProposals(EasyMock.capture(executedProposals), EasyMock.anyObject(), EasyMock.anyBoolean(), EasyMock.anyObject(),
                                        EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
                                        EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyBoolean(),
                                        EasyMock.anyObject(), EasyMock.anyBoolean());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(kafkaCruiseControl, loadMonitor);
    return kafkaCruiseControl;
  }

  private static CreateTopicRunnable runnable(KafkaCruiseControl kafkaCruiseControl, CreateTopicParameters parameters) {
    return new CreateTopicRunnable(kafkaCruiseControl, new OperationFuture("Create Topic"), "uuid", parameters);
  }

  private static void assertRackAware(SortedMap<Integer, List<ReplicaPlacementInfo>> replicaAssignment, int partitionCount,
                                      short replicationFactor) {
    assertEquals(partitionCount, replicaAssignment.size());
    for (List<ReplicaPlacementInfo> replicas : replicaAssignment.values()) {
      // Each broker is on a separate rack.
      assertEquals(replicationFactor, replicas.stream().map(ReplicaPlacementInfo::brokerId).distinct().count());
    }
  }

  @Test
  public void testNewTopicAvoidsOverloadedBroker() throws Exception {
    // Broker 0 cannot host any new replica without exceeding its disk capacity threshold.
    ClusterModel clusterModel = clusterModel(List.of(Map.entry(0, 750.0), Map.entry(1, 100.0), Map.entry(2, 100.0), Map.entry(3, 100.0)), 4);
    Capture<OptimizationOptions> optimizationOptions = Capture.newInstance();
    Admin adminClient = EasyMock.mock(Admin.class);
    EasyMock.replay(adminClient);
    KafkaCruiseControl kafkaCruiseControl = kafkaCruiseControl(clusterModel, optimizationOptions, adminClient, Capture.newInstance());

    CreateTopicResult result = runnable(kafkaCruiseControl, parameters(3, (short) 2, false, true)).getResult();

    assertRackAware(result.replicaAssignment(), 3, (short) 2);
    for (List<ReplicaPlacementInfo> replicas : result.replicaAssignment().values()) {
      for (ReplicaPlacementInfo replica : replicas) {
        assertFalse(replica.brokerId() == 0);
        // Logdirs are not decided unless the cluster uses JBOD.
        assertNull(replica.logdir());
      }
    }
    // Replicas of the existing topic are neither moved nor considered for movement.
    assertTrue(result.optimizerResult().goalProposals().isEmpty());
    assertEquals(Set.of(TOPIC), optimizationOptions.getValue().excludedTopics());
    assertTrue(optimizationOptions.getValue().onlyMoveImmigrantReplicas());
    // A dry run does not create the topic.
    EasyMock.verify(adminClient);
  }

  @Test
  public void testNewTopicRequiresOtherTopicsMovements() throws Exception {
    // A replica of the new topic must be on each broker, but broker 0 cannot host one unless another replica moves out.
    List<Map.Entry<Integer, Double>> diskMbByBrokerByPartition = List.of(Map.entry(0, 450.0), Map.entry(0, 300.0), Map.entry(1, 300.0),
                                                                         Map.entry(2, 300.0));
    KafkaCruiseControl kafkaCruiseControl = kafkaCruiseControl(clusterModel(diskMbByBrokerByPartition, 3), Capture.newInstance(),
                                                               EasyMock.mock(Admin.class), Capture.newInstance());
    CreateTopicRunnable runnable = runnable(kafkaCruiseControl, parameters(1, (short) 3, false, true));
    assertThrows(KafkaCruiseControlException.class, runnable::getResult);

    // Once the movement of other topics is allowed, the topic is created, and then other topics' replicas are moved.
    Admin adminClient = EasyMock.mock(Admin.class);
    CreateTopicsResult createTopicsResult = EasyMock.mock(CreateTopicsResult.class);
    Capture<Collection<NewTopic>> newTopics = Capture.newInstance();
    EasyMock.expect(adminClient.createTopics(EasyMock.capture(newTopics))).andReturn(createTopicsResult).once();
    EasyMock.expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null)).once();
    EasyMock.replay(adminClient, createTopicsResult);
    Capture<OptimizationOptions> optimizationOptions = Capture.newInstance();
    Capture<Set<ExecutionProposal>> executedProposals = Capture.newInstance();
    kafkaCruiseControl = kafkaCruiseControl(clusterModel(diskMbByBrokerByPartition, 3), optimizationOptions, adminClient, executedProposals);

    CreateTopicResult result = runnable(kafkaCruiseControl, parameters(1, (short) 3, true, false)).getResult();

    assertRackAware(result.replicaAssignment(), 1, (short) 3);
    assertTrue(optimizationOptions.getValue().excludedTopics().isEmpty());
    assertFalse(optimizationOptions.getValue().onlyMoveImmigrantReplicas());
    Set<ExecutionProposal> proposals = result.optimizerResult().goalProposals();
    assertFalse(proposals.isEmpty());
    assertTrue(proposals.stream().allMatch(p -> p.topic().equals(TOPIC)));
    assertEquals(proposals, executedProposals.getValue());
    EasyMock.verify(adminClient, createTopicsResult);
    NewTopic newTopic = newTopics.getValue().iterator().next();
    assertEquals(NEW_TOPIC, newTopic.name());
    Map<Integer, List<Integer>> expectedAssignment = new HashMap<>();
    result.replicaAssignment().forEach((partition, replicas) -> expectedAssignment.put(
        partition, replicas.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList())));
    assertEquals(expectedAssignment, newTopic.replicasAssignments());
  }

  @Test
  public void testPlaceReplicasOnDisks() {
    Map<String, Double> diskCapacityByLogDir = Map.of(LOGDIR0, 500.0, LOGDIR1, 500.0);
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(Map.of(0, 0), BROKER_CAPACITY, diskCapacityByLogDir);
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    clusterModel.createReplica("0", 0, tp, 0, true, false, LOGDIR0, false);
    clusterModel.setReplicaLoad("0", 0, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, 300.0), Collections.singletonList(1L));
    SortedMap<Integer, List<Integer>> brokersByPartition = new TreeMap<>();
    for (int partition = 0; partition < 4; partition++) {
      brokersByPartition.put(partition, List.of(0));
    }

    // Each replica goes to the disk with the lowest utilization after placement (LOGDIR1 until 400 MB is used on both disks), and
    // then to the disk with fewer replicas of the new topic.
    SortedMap<Integer, List<ReplicaPlacementInfo>> replicaAssignment =
        CreateTopicRunnable.placeReplicasOnDisks(NEW_TOPIC, brokersByPartition, clusterModel, REPLICA_DISK_MB, 0.8);
    List<String> logdirs = new ArrayList<>();
    replicaAssignment.values().forEach(replicas -> logdirs.add(replicas.get(0).logdir()));
    assertEquals(List.of(LOGDIR1, LOGDIR1, LOGDIR1, LOGDIR0), logdirs);

    // Expected: IllegalStateException if no disk can host the replica without exceeding the disk capacity threshold -- i.e. LOGDIR1
    // can host one more replica, but then both disks are at 400 MB.
    brokersByPartition.put(4, List.of(0));
    assertEquals(LOGDIR1, CreateTopicRunnable.placeReplicasOnDisks(NEW_TOPIC, brokersByPartition, clusterModel, REPLICA_DISK_MB, 0.8)
                                             .get(4).get(0).logdir());
    brokersByPartition.put(5, List.of(0));
    assertThrows(IllegalStateException.class,
                 () -> CreateTopicRunnable.placeReplicasOnDisks(NEW_TOPIC, brokersByPartition, clusterModel, REPLICA_DISK_MB, 0.8));

    // Logdirs are not decided for brokers with a single logdir.
    ClusterModel singleLogdirClusterModel = DeterministicCluster.getHomogeneousCluster(Map.of(0, 0), BROKER_CAPACITY, Map.of(LOGDIR0, 500.0));
    replicaAssignment = CreateTopicRunnable.placeReplicasOnDisks(NEW_TOPIC, new TreeMap<>(Map.of(0, List.of(0))), singleLogdirClusterModel,
                                                                 REPLICA_DISK_MB, 0.8);
    assertNull(replicaAssignment.get(0).get(0).logdir());
  }

  private static AlterReplicaLogDirsResult alterReplicaLogDirsResult(Set<TopicPartitionReplica> replicas, Exception exception) {
    Map<TopicPartitionReplica, KafkaFuture<Void>> futures = new HashMap<>();
    for (TopicPartitionReplica replica : replicas) {
      KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
      if (exception == null) {
        future.complete(null);
      } else {
        future.completeExceptionally(exception);
      }
      futures.put(replica, future);
    }
    AlterReplicaLogDirsResult result = EasyMock.mock(AlterReplicaLogDirsResult.class);
    EasyMock.expect(result.values()).andReturn(futures).once();
    EasyMock.replay(result);
    return result;
  }

  @Test
  public void testCreateTopicWithLogdirs() throws Exception {
    SortedMap<Integer, List<ReplicaPlacementInfo>> replicaAssignment = new TreeMap<>();
    replicaAssignment.put(0, List.of(new ReplicaPlacementInfo(1, LOGDIR0), new ReplicaPlacementInfo(0, LOGDIR1)));
    replicaAssignment.put(1, List.of(new ReplicaPlacementInfo(0, LOGDIR0), new ReplicaPlacementInfo(1, LOGDIR1)));
    Map<TopicPartitionReplica, String> logdirByReplica = Map.of(new TopicPartitionReplica(NEW_TOPIC, 0, 1), LOGDIR0,
                                                                new TopicPartitionReplica(NEW_TOPIC, 0, 0), LOGDIR1,
                                                                new TopicPartitionReplica(NEW_TOPIC, 1, 0), LOGDIR0,
                                                                new TopicPartitionReplica(NEW_TOPIC, 1, 1), LOGDIR1);

    // Brokers do not have the replicas before topic creation, but keep their requested logdirs.
    Admin adminClient = EasyMock.mock(Admin.class);
    CreateTopicsResult createTopicsResult = EasyMock.mock(CreateTopicsResult.class);
    Capture<Collection<NewTopic>> newTopics = Capture.newInstance();
    EasyMock.expect(adminClient.alterReplicaLogDirs(logdirByReplica))
            .andReturn(alterReplicaLogDirsResult(logdirByReplica.keySet(), new ReplicaNotAvailableException("Replica does not exist.")))
            .andReturn(alterReplicaLogDirsResult(logdirByReplica.keySet(), null));
    EasyMock.expect(adminClient.createTopics(EasyMock.capture(newTopics))).andReturn(createTopicsResult).once();
    EasyMock.expect(createTopicsResult.all()).andReturn(KafkaFuture.completedFuture(null)).once();
    EasyMock.replay(adminClient, createTopicsResult);

    CreateTopicRunnable.createTopic(adminClient, NEW_TOPIC, replicaAssignment, 1000L);

    EasyMock.verify(adminClient, createTopicsResult);
    assertEquals(Map.of(0, List.of(1, 0), 1, List.of(0, 1)), newTopics.getValue().iterator().next().replicasAssignments());

    // Expected: The topic is not created if a requested logdir is not found.
    adminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(adminClient.alterReplicaLogDirs(logdirByReplica))
            .andReturn(alterReplicaLogDirsResult(logdirByReplica.keySet(), new LogDirNotFoundException("Logdir is not found.")));
    EasyMock.replay(adminClient);
    Admin finalAdminClient = adminClient;
    assertThrows(ExecutionException.class, () -> CreateTopicRunnable.createTopic(finalAdminClient, NEW_TOPIC, replicaAssignment, 1000L));
    EasyMock.verify(adminClient);
  }

  @Test
  public void testExistingTopicCannotBeCreated() throws Exception {
    ClusterModel clusterModel = clusterModel(List.of(Map.entry(0, 100.0)), 2);
    KafkaCruiseControl kafkaCruiseControl = kafkaCruiseControl(clusterModel, Capture.newInstance(), EasyMock.mock(Admin.class),
                                                               Capture.newInstance());
    CreateTopicParameters parameters = EasyMock.niceMock(CreateTopicParameters.class);
    EasyMock.expect(parameters.goals()).andReturn(GOALS).anyTimes();
    EasyMock.expect(parameters.skipHardGoalCheck()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.dryRun()).andReturn(true).anyTimes();
    EasyMock.expect(parameters.topic()).andReturn(TOPIC).anyTimes();
    EasyMock.replay(parameters);
    assertThrows(IllegalArgumentException.class, runnable(kafkaCruiseControl, parameters)::getResult);
  }
}
