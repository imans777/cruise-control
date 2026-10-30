/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.waitUntilTrue;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutorTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ReplicationThrottleHelperTest extends CCKafkaIntegrationTestHarness {
  private static final Config EMPTY_CONFIG = new Config(Collections.emptyList());

  /**
   * The admin client
   */
  private Admin _adminClient;

  @Override
  public int clusterSize() {
    return 4;
  }

  /**
   * Setup the test.
   */
  @Before
  public void setUp() {
    super.setUp();
    _adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
  }

  /**
   * Teardown the test.
   */
  @After
  public void tearDown() {
    super.tearDown();
    if (_adminClient != null) {
      _adminClient.close(Duration.ofMillis(1000L));
    }
  }

  private void createTopics() throws ExecutionException, InterruptedException {
    _adminClient.createTopics(Arrays.asList(
        new NewTopic(TOPIC0, 2, (short) 2),
        new NewTopic(TOPIC1, 2, (short) 2)
    )).all().get();
  }

  private void setWildcardThrottleReplicaForTopic(ReplicationThrottleHelper helper, String topicName) throws Exception {
    for (String replicaThrottleProp : Arrays.asList(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
                                                    ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)) {
      Collection<AlterConfigOp> configs = Collections.singletonList(
              new AlterConfigOp(new ConfigEntry(replicaThrottleProp, ReplicationThrottleHelper.WILDCARD_ASTERISK), AlterConfigOp.OpType.SET)
      );
      changeTopicConfigs(helper, topicName, configs);
    }
  }

  // Applies topic configs directly via the admin client and waits until they are readable.
  private void changeTopicConfigs(ReplicationThrottleHelper helper, String topic, Collection<AlterConfigOp> ops) throws Exception {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    _adminClient.incrementalAlterConfigs(Collections.singletonMap(cf, ops)).all().get();
    helper.waitForBatchConfigs(Collections.singletonMap(cf, ops));
  }

  // Applies broker configs directly via the admin client and waits until they are readable.
  private void changeBrokerConfigs(ReplicationThrottleHelper helper, int brokerId, Collection<AlterConfigOp> ops) throws Exception {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    _adminClient.incrementalAlterConfigs(Collections.singletonMap(cf, ops)).all().get();
    helper.waitForBatchConfigs(Collections.singletonMap(cf, ops));
  }

  private ExecutionTask inProgressTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = new ExecutionTask(id, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION, EXECUTION_ALERTING_THRESHOLD_MS);
    task.inProgress(0);
    return task;
  }

  private ExecutionTask completedTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = inProgressTaskForProposal(id, proposal);
    task.completed(1);
    return task;
  }

  @Test
  public void testIsNoOpWhenThrottleIsNull() throws Exception {
    Admin mockAdminClient = EasyMock.strictMock(Admin.class);
    EasyMock.replay(mockAdminClient);

    // Test would fail on any unexpected interactions with the adminClient
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, null);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    assertFalse(throttleHelper.throttlingEnabled());
    assertTrue(throttleHelper.throttledBrokers().isEmpty());
    assertTrue(throttleHelper.throttleRateByBroker().isEmpty());
    assertNull(throttleHelper.leaderThrottleRate(0));
    assertNull(throttleHelper.followerThrottleRate(0));
    assertThrows(IllegalStateException.class, () -> throttleHelper.updateThrottleRates(0, 100L, 100L));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testIsNoOpForProposalsWithoutDataMovement() throws Exception {
    Admin mockAdminClient = EasyMock.strictMock(Admin.class);
    EasyMock.replay(mockAdminClient);

    // Test would fail on any unexpected interactions with the adminClient
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L);
    // A proposal that only removes a replica (e.g. to decrease the replication factor) does not replicate any data.
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition("topic", 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1),
                                                                     new ReplicaPlacementInfo(2)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testThrottleRateByBrokerDoesNotWaitForTheLockOfTheHelper() throws Exception {
    // The executor state reads the rates while setting, clearing or updating the throttles holds the lock of the helper for as
    // long as it takes to alter and verify the configs.
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(EasyMock.mock(Admin.class), 100L);
    assertTrue(readWhileLocked(throttleHelper, throttleHelper::throttleRateByBroker).isEmpty());
  }

  @Test
  public void testClearThrottleOnNonExistentTopic() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);
    final int partitionId = 0;
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, partitionId),
                                                       100,
                                                       new ReplicaPlacementInfo(brokerId0),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Case 1: a situation where Topic0 does not exist. Hence no property is returned upon read.

    // Before the dynamic broker config is removed for the follower throttle rate
    Config brokerConfig = new Config(Arrays.asList(
            mockConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
            mockConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG))
    );
    // After the dynamic broker config is removed for the follower throttle rate
    Config brokerConfig2 = new Config(Arrays.asList(
            mockConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG),
            mockConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "300",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG))
    );
    // Expect batched calls: one batch read (via values()), one batch write, one batch verify (via values()),
    // then topic read fails
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers, brokerConfig);
    expectBatchIncrementalBrokerConfigsViaValues(mockAdminClient, brokers);
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers, brokerConfig2);
    expectBatchDescribeTopicConfigsViaValues(mockAdminClient, TOPIC0, EMPTY_CONFIG, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    ExecutionTask mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);

    // Case 2: a situation where Topic0 gets deleted after its configs were read.
    EasyMock.reset(mockAdminClient);
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers);
    expectBatchIncrementalBrokerConfigsViaValues(mockAdminClient, brokers);
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers, EMPTY_CONFIG);
    String throttledReplicas = brokerId0 + "," + brokerId1;
    Config topicConfigProps = new Config(Arrays.asList(
            new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas),
            new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas)));
    expectBatchDescribeTopicConfigsViaValues(mockAdminClient, TOPIC0, topicConfigProps, true);
    expectBatchIncrementalTopicConfigsViaValues(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    // Expect no exception
    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);
  }

  @Test
  public void testClearThrottleDoesNotRemoveThrottledRateNotSetOnBroker() throws Exception {
    final long throttleRate = 100L;
    final List<Integer> brokers = Arrays.asList(0, 1, 2);
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    Admin mockAdminClient = EasyMock.strictMock(Admin.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // The throttle rates come from the cluster-wide dynamic default and the static broker configs. Hence, there is no
    // broker-level throttle rate to remove and the test would fail on any attempt to alter the broker configs.
    Config brokerConfig = new Config(Arrays.asList(
            mockConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "200",
                    ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG),
            mockConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "300",
                    ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG))
    );
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers, brokerConfig);
    expectBatchDescribeTopicConfigsViaValues(mockAdminClient, TOPIC0, EMPTY_CONFIG, true);
    ExecutionTask mockCompleteTask = prepareMockCompleteTask(proposal);
    EasyMock.replay(mockAdminClient);

    throttleHelper.clearThrottles(Collections.singletonList(mockCompleteTask), Collections.emptyList());
    EasyMock.verify(mockAdminClient, mockCompleteTask);
  }

  @Test
  public void testSetThrottleOnNonExistentTopic() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);
    final int partitionId = 0;
    // A proposal to move a partition with 2 replicas from broker 0 and 1 to broker 0 and 2
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, partitionId),
                                                       100,
                                                       new ReplicaPlacementInfo(brokerId0),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    Admin mockAdminClient = EasyMock.strictMock(Admin.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Case 1: a situation where Topic0 does not exist. Hence no property is returned upon read.
    // Batch read brokers (rates already match 100L, so no broker writes)
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers);
    // Batch read topic configs (topic doesn't exist)
    expectBatchDescribeTopicConfigsViaValues(mockAdminClient, TOPIC0, EMPTY_CONFIG, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    // Batch write topic configs (topic doesn't exist, write fails)
    expectBatchIncrementalTopicConfigsViaValues(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    // Expect no exception
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);

    // Case 2: a situation where Topic0 gets deleted after its configs were read. Change configs should not fail.
    EasyMock.reset(mockAdminClient);
    // Batch read brokers (rates already match)
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers);
    String throttledReplicas = brokerId0 + "," + brokerId1;
    Config topicConfigs = new Config(Arrays.asList(
      new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas),
      new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, throttledReplicas)));
    // Batch read topic configs (topic exists at read time)
    expectBatchDescribeTopicConfigsViaValues(mockAdminClient, TOPIC0, topicConfigs, true);
    // Batch write topic configs (topic gets deleted, write fails)
    expectBatchIncrementalTopicConfigsViaValues(mockAdminClient, TOPIC0, false);
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    // Expect no exception
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testSetThrottleOnMixedTopicExistence() throws Exception {
    final long throttleRate = 100L;
    final int brokerId0 = 0;
    final int brokerId1 = 1;
    final int brokerId2 = 2;
    final List<Integer> brokers = Arrays.asList(brokerId0, brokerId1, brokerId2);

    // Two proposals on different topics: TOPIC0 exists, TOPIC1 does not
    ExecutionProposal proposal0 = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
        100, new ReplicaPlacementInfo(brokerId0),
        Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
        Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));
    ExecutionProposal proposal1 = new ExecutionProposal(new TopicPartition(TOPIC1, 0),
        100, new ReplicaPlacementInfo(brokerId0),
        Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId1)),
        Arrays.asList(new ReplicaPlacementInfo(brokerId0), new ReplicaPlacementInfo(brokerId2)));

    Admin mockAdminClient = EasyMock.mock(Admin.class);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, throttleRate);

    // Batch read brokers — rates already match, no writes needed
    expectBatchDescribeBrokerConfigsViaValues(mockAdminClient, brokers);

    // Batch read topic configs via values() — TOPIC0 succeeds, TOPIC1 fails
    String existingReplicas = "1:0,1:1";
    Config topic0Config = new Config(Arrays.asList(
        new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, existingReplicas),
        new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, existingReplicas)));
    expectBatchDescribeTopicConfigsMixed(mockAdminClient, TOPIC0, topic0Config, TOPIC1);
    // listTopics for TOPIC1 non-existence check
    expectListTopics(mockAdminClient, Collections.emptySet());

    // Batch write topic configs via values() — TOPIC0 succeeds, TOPIC1 fails
    expectBatchIncrementalTopicConfigsMixed(mockAdminClient, TOPIC0, TOPIC1);
    // listTopics for TOPIC1 non-existence check during write
    expectListTopics(mockAdminClient, Collections.emptySet());

    // Verification for TOPIC0 only (TOPIC1 was filtered out as unsuccessful)
    ConfigResource topic0Cf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    Config verifyConfig = new Config(Arrays.asList(
        new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0:0,0:1,0:2,1:0,1:1"),
        new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0:0,0:1,0:2,1:0,1:1")));
    expectBatchDescribeConfigsViaValues(mockAdminClient, Collections.singletonMap(topic0Cf, verifyConfig));

    EasyMock.replay(mockAdminClient);
    // Expect no exception — TOPIC0 throttle is applied, TOPIC1 is gracefully skipped
    throttleHelper.setThrottles(Arrays.asList(proposal0, proposal1));
    EasyMock.verify(mockAdminClient);
  }

  @Test
  public void testAddingThrottlesWithNoPreExistingThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testUpdateThrottleRates() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    // Updating the throttle rates of a broker that is currently throttled applies the new rates right away.
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    assertEquals(Set.of(0, 1, 2), throttleHelper.throttledBrokers());
    throttleHelper.updateThrottleRates(1, 200L, 300L);
    assertEquals(200L, throttleHelper.leaderThrottleRate(1).longValue());
    assertEquals(300L, throttleHelper.followerThrottleRate(1).longValue());
    assertEquals(throttleRate, throttleHelper.leaderThrottleRate(0).longValue());
    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRatesForBroker(1, 200L, 300L);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, null);

    // Updating the throttle rates of a broker whose throttle has been removed must not throttle the broker again.
    throttleHelper.clearThrottles(Collections.singletonList(completedTaskForProposal(0, proposal)), Collections.emptyList());
    assertTrue(throttleHelper.throttledBrokers().isEmpty());
    throttleHelper.updateThrottleRates(1, 400L, 500L);
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }

    // The updated throttle rates are applied once the broker participates in subsequent throttled replica movements.
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRatesForBroker(1, 400L, 500L);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, null);

    throttleHelper.clearThrottles(Collections.singletonList(completedTaskForProposal(1, proposal)), Collections.emptyList());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testDynamicallyUpdatingThrottleRate() throws Exception {
    createTopics();

    final long initialThrottleRate = 100L;
    final long updatedThrottleRate = 300L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, initialThrottleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertEquals(Long.valueOf(initialThrottleRate), throttleHelper.throttleRate());
    assertExpectedThrottledRateForBroker(0, initialThrottleRate);
    assertExpectedThrottledRateForBroker(1, initialThrottleRate);
    assertExpectedThrottledRateForBroker(2, initialThrottleRate);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2");

    // Dynamically update the throttle rate and re-apply the throttles for the in-progress proposal. The in-progress proposals
    // must be retrieved while holding the lock of the helper, so that a concurrent clearThrottles cannot be undone.
    throttleHelper.updateThrottleRate(updatedThrottleRate, () -> {
      assertTrue(Thread.holdsLock(throttleHelper));
      return Collections.singletonList(proposal);
    });

    assertEquals(Long.valueOf(updatedThrottleRate), throttleHelper.throttleRate());
    assertExpectedThrottledRateForBroker(0, updatedThrottleRate);
    assertExpectedThrottledRateForBroker(1, updatedThrottleRate);
    assertExpectedThrottledRateForBroker(2, updatedThrottleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    // The throttled replicas are unchanged by the throttle rate update.
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up once the task completes.
    ExecutionTask task = completedTaskForProposal(0, proposal);
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testUpdateThrottleRatesInBatch() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    // Rates set before any broker is throttled are only remembered -- e.g. the initial rates of an execution without a throttle.
    throttleHelper.updateThrottleRates(Map.of(0, 150L, 3, 160L), Map.of(0, 250L, 3, 260L));
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    // ... so the executor state reports no broker yet.
    assertTrue(throttleHelper.throttleRateByBroker().isEmpty());
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    assertExpectedThrottledRatesForBroker(0, 150L, 250L);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, null);
    assertEquals(Map.of(0, List.of(150L, 250L), 1, List.of(throttleRate, throttleRate), 2, List.of(throttleRate, throttleRate)),
                 leaderAndFollowerRateByBroker(throttleHelper));

    // Several throttled brokers are updated in one batch; broker 3 is not throttled, so its rates are only remembered.
    throttleHelper.updateThrottleRates(Map.of(1, 200L, 2, 210L, 3, 220L), Map.of(1, 300L, 2, 310L, 3, 320L));
    assertExpectedThrottledRatesForBroker(0, 150L, 250L);
    assertExpectedThrottledRatesForBroker(1, 200L, 300L);
    assertExpectedThrottledRatesForBroker(2, 210L, 310L);
    assertExpectedThrottledRateForBroker(3, null);
    assertEquals(220L, throttleHelper.leaderThrottleRate(3).longValue());
    // The executor state reports the throttled brokers only, at their current rates.
    assertEquals(Map.of(0, List.of(150L, 250L), 1, List.of(200L, 300L), 2, List.of(210L, 310L)),
                 leaderAndFollowerRateByBroker(throttleHelper));

    throttleHelper.clearThrottles(Collections.singletonList(completedTaskForProposal(0, proposal)), Collections.emptyList());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
    assertTrue(throttleHelper.throttleRateByBroker().isEmpty());
  }

  private static Map<Integer, List<Long>> leaderAndFollowerRateByBroker(ReplicationThrottleHelper throttleHelper) {
    Map<Integer, List<Long>> leaderAndFollowerRateByBroker = new HashMap<>();
    throttleHelper.throttleRateByBroker()
                  .forEach((brokerId, throttle) -> leaderAndFollowerRateByBroker.put(brokerId, List.of(throttle.leader(), throttle.follower())));
    return leaderAndFollowerRateByBroker;
  }

  @Test
  public void testThrottleRateDoesNotWaitForTheLockOfTheHelper() throws Exception {
    // The executor state reads the rate while setting, clearing or updating the throttles holds the lock of the helper for as
    // long as it takes to alter and verify the configs.
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(EasyMock.mock(Admin.class), 100L);
    assertEquals(Long.valueOf(100L), readWhileLocked(throttleHelper, throttleHelper::throttleRate));
  }

  @Test
  public void testRemovingThrottleOfAnOngoingExecution() throws Exception {
    createTopics();

    final long initialThrottleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, initialThrottleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Collections.singletonList(proposal));
    assertExpectedThrottledRateForBroker(0, initialThrottleRate);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2");

    // Removing the throttle of the ongoing execution clears the rate of the participating brokers and the throttled
    // replicas of their topics, with the movement still in progress.
    assertTrue(throttleHelper.removeThrottleRate(() -> {
      assertTrue(Thread.holdsLock(throttleHelper));
      return Collections.singletonList(proposal);
    }));
    assertNull(throttleHelper.throttleRate());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");

    // Upcoming task batches of the same execution are not throttled either, and a second removal is a no-op.
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    assertExpectedThrottledRateForBroker(0, null);
    assertFalse(throttleHelper.removeThrottleRate(Collections::emptyList));
  }

  @Test
  public void testAddingThrottlesWithPreExistingThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition(TOPIC0, 0),
        100,
        new ReplicaPlacementInfo(0),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    // Broker 0 has an existing leader throttle rate of 200 and follower throttle rate of 100
    // We expect to overwrite the leader throttle to the desired throttleRate of 100
    long preExistingBroker0ThrottleRate = 200L;
    List<AlterConfigOp> broker0Configs = Arrays.asList(
      new AlterConfigOp(
              new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(preExistingBroker0ThrottleRate)),
              AlterConfigOp.OpType.SET),
      new AlterConfigOp(
              new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, String.valueOf(throttleRate)),
              AlterConfigOp.OpType.SET)
    );
    changeBrokerConfigs(throttleHelper, 0, broker0Configs);

    // Partition 1 (which is not involved in any execution proposal) has pre-existing throttled
    // replicas (on both leaders and followers); we expect these configurations to be merged
    // with our new throttled replicas.
    List<AlterConfigOp> topic0Configs = Arrays.asList(
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:0,1:1"),
              AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:0,1:1"),
              AlterConfigOp.OpType.SET)
    );
    changeTopicConfigs(throttleHelper, TOPIC0, topic0Configs);

    // Topic 1 is not involved in any execution proposal. It has pre-existing throttled replicas.
    List<AlterConfigOp> topic1Config = Arrays.asList(
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1"),
              AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1"),
              AlterConfigOp.OpType.SET)
    );
    changeTopicConfigs(throttleHelper, TOPIC1, topic1Config);

    throttleHelper.setThrottles(Collections.singletonList(proposal));

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(3, null);
    // Existing throttled replicas are merged with new throttled replicas for topic 0:
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2,1:0,1:1");
    // Existing throttled replicas are unchanged for topic 1:
    assertExpectedThrottledReplicas(TOPIC1, "1:1");

    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    // We expect all throttles related to replica movement to be removed. Specifically,
    // any throttles related to partitions which were not moved will remain.
    // However, we do expect the broker throttles to be removed.
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:1");
    assertExpectedThrottledReplicas(TOPIC1, "1:1");
  }

  @Test
  public void testClearThrottlesWithClusterDefaultThrottledRate() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    final long clusterDefaultThrottleRate = 200L;
    // Use a few retries, so that the test fails fast if waiting for the throttles to be removed never converges.
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate, 3);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                                       100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    // The throttle rate is set outside of Cruise Control as a cluster-wide dynamic default broker config, e.g. by
    // an operator who adjusts the throttle of an ongoing execution.
    _adminClient.incrementalAlterConfigs(Collections.singletonMap(KafkaCruiseControlUtils.CLUSTER_CONFIG, Arrays.asList(
        new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                                          String.valueOf(clusterDefaultThrottleRate)), AlterConfigOp.OpType.SET),
        new AlterConfigOp(new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                                          String.valueOf(clusterDefaultThrottleRate)), AlterConfigOp.OpType.SET)
    ))).all().get();
    waitForClusterDefaultThrottledRate(clusterDefaultThrottleRate);

    // The broker-level throttles set by Cruise Control take precedence over the cluster-wide default.
    throttleHelper.setThrottles(Collections.singletonList(proposal));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);

    // Clearing the throttles removes the broker-level throttles, and the brokers fall back to the cluster-wide default.
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    waitForClusterDefaultThrottledRate(clusterDefaultThrottleRate);
    assertExpectedThrottledReplicas(TOPIC0, "");

    // Clearing the throttles again leaves the cluster-wide default untouched.
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
    waitForClusterDefaultThrottledRate(clusterDefaultThrottleRate);
  }

  @Test
  public void testDoNotModifyExistingWildcardReplicaThrottles() throws Exception {
    createTopics();

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);

    // Set replica throttle config values for both topics
    setWildcardThrottleReplicaForTopic(throttleHelper, TOPIC0);
    setWildcardThrottleReplicaForTopic(throttleHelper, TOPIC1);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(new TopicPartition(TOPIC0, 1),
                                            100,
                                                        new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2));

    ExecutionTask completedTask = completedTaskForProposal(0, proposal);
    ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    // passing an inProgress task that is not complete should have no effect.
    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);

    // Completing the in-progress task and the "*" should not be cleaned up.
    inProgressTask.completed(3);
    throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    // Topic-level throttled replica config value should remain as "*"
    assertExpectedThrottledReplicas(TOPIC0, ReplicationThrottleHelper.WILDCARD_ASTERISK);
    assertExpectedThrottledReplicas(TOPIC1, ReplicationThrottleHelper.WILDCARD_ASTERISK);
  }

  @Test
  public void testDoNotRemoveThrottlesForInProgressTasks() throws Exception {
    createTopics();

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0),
                                           100,
                                                       new ReplicaPlacementInfo(0),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                       Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(new TopicPartition(TOPIC0, 1),
                                            100,
                                                        new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2));

    ExecutionTask completedTask = completedTaskForProposal(0, proposal);
    ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);

    assertExpectedThrottledRateForBroker(0, throttleRate);
    assertExpectedThrottledRateForBroker(1, throttleRate);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "0:0,0:1,0:2,1:0,1:2,1:3");

    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:2,1:3");

    // passing an inProgress task that is not complete should have no effect.
    throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
    assertExpectedThrottledRateForBroker(0, throttleRate);
    // we expect broker 1 to be null since all replica movement related to it has completed.
    assertExpectedThrottledRateForBroker(1, null);
    assertExpectedThrottledRateForBroker(2, throttleRate);
    // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
    assertExpectedThrottledRateForBroker(3, throttleRate);
    assertExpectedThrottledReplicas(TOPIC0, "1:0,1:2,1:3");

    // Completing the in-progress task and clearing the throttles should clean everything up.
    inProgressTask.completed(3);
    throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
  }

  @Test
  public void testThrottlesThatFailToBeRemovedAreRemovedLater() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    final int brokerToFail = 1;
    AtomicBoolean failRemoval = new AtomicBoolean(false);
    ReplicationThrottleHelper.AppliedThrottles appliedThrottles = new ReplicationThrottleHelper.AppliedThrottles();
    ConfigResource brokerResourceToFail = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerToFail));
    ConfigResource topicResourceToFail = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC1);
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate, ReplicationThrottleHelper.RETRIES,
                                                                             new HashSet<>(), appliedThrottles) {
      // Fails the batched config writes of broker 1 and topic1 while failRemoval is set; the other resources are written.
      @Override
      Map<ConfigResource, KafkaFuture<Void>> alterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> chunkOps) {
        Map<ConfigResource, Collection<AlterConfigOp>> opsToWrite = new HashMap<>(chunkOps);
        Map<ConfigResource, KafkaFuture<Void>> futures = new HashMap<>();
        if (failRemoval.get()) {
          for (ConfigResource cf : Arrays.asList(brokerResourceToFail, topicResourceToFail)) {
            if (opsToWrite.remove(cf) != null) {
              KafkaFutureImpl<Void> failedFuture = new KafkaFutureImpl<>();
              failedFuture.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Injected failure"));
              futures.put(cf, failedFuture);
            }
          }
        }
        if (!opsToWrite.isEmpty()) {
          futures.putAll(super.alterConfigs(opsToWrite));
        }
        return futures;
      }
    };
    // Throttles brokers 0, 1, 2 and topic0 replicas 0:0, 0:1, 0:2.
    ExecutionProposal proposal0 = new ExecutionProposal(new TopicPartition(TOPIC0, 0), 100, new ReplicaPlacementInfo(0),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
    // Throttles brokers 0, 2, 3 and topic1 replicas 0:0, 0:2, 0:3.
    ExecutionProposal proposal1 = new ExecutionProposal(new TopicPartition(TOPIC1, 0), 100, new ReplicaPlacementInfo(3),
                                                        Arrays.asList(new ReplicaPlacementInfo(3), new ReplicaPlacementInfo(0)),
                                                        Arrays.asList(new ReplicaPlacementInfo(3), new ReplicaPlacementInfo(2)));
    throttleHelper.setThrottles(Arrays.asList(proposal0, proposal1));
    assertEquals(Set.of(0, 1, 2, 3), appliedThrottles.brokers());
    assertEquals(Set.of(proposal0, proposal1), appliedThrottles.proposals());

    // Failing to remove the throttled rate of broker 1 and the throttled replicas of topic1 fails the call, but only after
    // removing the others.
    failRemoval.set(true);
    List<ExecutionTask> completedTasks = Arrays.asList(completedTaskForProposal(0, proposal0), completedTaskForProposal(1, proposal1));
    IllegalStateException exception = assertThrows(IllegalStateException.class,
                                                   () -> throttleHelper.clearThrottles(completedTasks, Collections.emptyList()));
    assertTrue(exception.getMessage(), exception.getMessage().contains("brokers [" + brokerToFail + "] and topics [" + TOPIC1 + "]"));
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, i == brokerToFail ? throttleRate : null);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
    assertExpectedThrottledReplicas(TOPIC1, "0:0,0:2,0:3");
    assertEquals(Set.of(brokerToFail), appliedThrottles.brokers());
    assertEquals(Set.of(proposal1), appliedThrottles.proposals());

    // The throttles that could not be removed are removed later.
    failRemoval.set(false);
    throttleHelper.clearLeftoverThrottles(Collections.emptySet());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC1, "");
    assertTrue(appliedThrottles.isEmpty());
  }

  @Test
  public void testClearLeftoverThrottlesRetainsThrottlesOfOngoingMovements() throws Exception {
    createTopics();

    final long throttleRate = 100L;
    ReplicationThrottleHelper.AppliedThrottles appliedThrottles = new ReplicationThrottleHelper.AppliedThrottles();
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(_adminClient, throttleRate, ReplicationThrottleHelper.RETRIES,
                                                                             new HashSet<>(), appliedThrottles);
    // Throttles brokers 0, 1, 2 and topic0 replicas 0:0, 0:1, 0:2.
    ExecutionProposal finishedProposal = new ExecutionProposal(new TopicPartition(TOPIC0, 0), 100, new ReplicaPlacementInfo(0),
                                                               Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
                                                               Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));
    // Throttles brokers 1, 2, 3 and topic1 replicas 0:1, 0:2, 0:3.
    ExecutionProposal ongoingProposal = new ExecutionProposal(new TopicPartition(TOPIC1, 0), 100, new ReplicaPlacementInfo(2),
                                                              Arrays.asList(new ReplicaPlacementInfo(2), new ReplicaPlacementInfo(3)),
                                                              Arrays.asList(new ReplicaPlacementInfo(3), new ReplicaPlacementInfo(1)));
    // E.g. the execution fails after setting the throttles, and Kafka carries on with the partition reassignments.
    throttleHelper.setThrottles(Arrays.asList(finishedProposal, ongoingProposal));

    // Retain the throttles that the ongoing partition reassignment needs.
    throttleHelper.clearLeftoverThrottles(Collections.singleton(ongoingProposal.topicPartition()));
    assertExpectedThrottledRateForBroker(0, null);
    for (int i = 1; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, throttleRate);
    }
    assertExpectedThrottledReplicas(TOPIC0, "");
    assertExpectedThrottledReplicas(TOPIC1, "0:1,0:2,0:3");
    assertEquals(Set.of(1, 2, 3), appliedThrottles.brokers());
    assertEquals(Set.of(ongoingProposal), appliedThrottles.proposals());

    // Remove the rest once the partition reassignment is over.
    throttleHelper.clearLeftoverThrottles(Collections.emptySet());
    for (int i = 0; i < clusterSize(); i++) {
      assertExpectedThrottledRateForBroker(i, null);
    }
    assertExpectedThrottledReplicas(TOPIC1, "");
    assertTrue(appliedThrottles.isEmpty());
  }

  @Test
  public void testRemoveReplicasFromConfigTest() {
    Set<String> replicas = new LinkedHashSet<>();
    replicas.add("foo");
    replicas.add("bar");
    replicas.add("baz");
    String throttleConfig = "foo,bar,qux,qaz,baz";
    String result = ReplicationThrottleHelper.removeReplicasFromConfig(throttleConfig, replicas);
    assertEquals("qux,qaz", result);
  }

  @Test
  public void testWaitForBatchConfigs() throws Exception {
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    int retries = 3;
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockAdminClient, 100L, retries);

    ConfigResource brokerCf0 = new ConfigResource(ConfigResource.Type.BROKER, "0");
    ConfigResource brokerCf1 = new ConfigResource(ConfigResource.Type.BROKER, "1");

    Collection<AlterConfigOp> setOps = Collections.singletonList(
        new AlterConfigOp(
            new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "100"),
            AlterConfigOp.OpType.SET));
    Map<ConfigResource, Collection<AlterConfigOp>> allOps = new HashMap<>();
    allOps.put(brokerCf0, setOps);
    allOps.put(brokerCf1, setOps);

    Config matchingConfig = new Config(Collections.singletonList(
        new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "100")));

    // Case 1: retries exhaust with configs never matching, expect IllegalStateException
    Map<ConfigResource, Config> mismatchMap = new HashMap<>();
    mismatchMap.put(brokerCf0, EMPTY_CONFIG);
    mismatchMap.put(brokerCf1, EMPTY_CONFIG);
    for (int i = 0; i < retries + 1; i++) {
      expectBatchDescribeConfigsViaValues(mockAdminClient, mismatchMap);
    }
    EasyMock.replay(mockAdminClient);
    assertThrows(IllegalStateException.class, () -> throttleHelper.waitForBatchConfigs(allOps));

    // Case 2: configs match on first attempt
    EasyMock.reset(mockAdminClient);
    Map<ConfigResource, Config> matchMap = new HashMap<>();
    matchMap.put(brokerCf0, matchingConfig);
    matchMap.put(brokerCf1, matchingConfig);
    expectBatchDescribeConfigsViaValues(mockAdminClient, matchMap);
    EasyMock.replay(mockAdminClient);
    throttleHelper.waitForBatchConfigs(allOps);

    // Case 3: partial match — one broker matches, the other doesn't, then both match
    EasyMock.reset(mockAdminClient);
    Map<ConfigResource, Config> partialMap = new HashMap<>();
    partialMap.put(brokerCf0, matchingConfig);
    partialMap.put(brokerCf1, EMPTY_CONFIG);
    expectBatchDescribeConfigsViaValues(mockAdminClient, partialMap);
    expectBatchDescribeConfigsViaValues(mockAdminClient, matchMap);
    EasyMock.replay(mockAdminClient);
    throttleHelper.waitForBatchConfigs(allOps);

    // Case 4: a topic resource fails during verification and the topic is confirmed deleted.
    // Verification should succeed (deleted topic is dropped, matching resource passes).
    ConfigResource topicCf = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC0);
    Map<ConfigResource, Collection<AlterConfigOp>> opsWithTopic = new HashMap<>();
    opsWithTopic.put(brokerCf0, setOps);
    opsWithTopic.put(topicCf, setOps);
    EasyMock.reset(mockAdminClient);
    expectBatchDescribeConfigsViaValuesWithFailure(mockAdminClient, brokerCf0, matchingConfig,
        topicCf, new ExecutionException(new UnknownTopicOrPartitionException()));
    // The topic is confirmed deleted, so its verification is skipped.
    expectListTopics(mockAdminClient, Collections.emptySet());
    EasyMock.replay(mockAdminClient);
    throttleHelper.waitForBatchConfigs(opsWithTopic);

    // Case 5: a topic resource fails transiently during verification while the topic still exists.
    // The resource stays pending and is verified on the next attempt.
    EasyMock.reset(mockAdminClient);
    expectBatchDescribeConfigsViaValuesWithFailure(mockAdminClient, brokerCf0, matchingConfig,
        topicCf, new TimeoutException("simulated timeout"));
    // The topic still exists, so verification retries instead of skipping.
    expectListTopics(mockAdminClient, Collections.singleton(TOPIC0));
    expectBatchDescribeConfigsViaValues(mockAdminClient, Collections.singletonMap(topicCf, matchingConfig));
    EasyMock.replay(mockAdminClient);
    throttleHelper.waitForBatchConfigs(opsWithTopic);

    // Case 6: a broker resource keeps failing during verification. It must not pass silently:
    // verification retries until attempts are exhausted and then throws.
    EasyMock.reset(mockAdminClient);
    for (int i = 0; i < retries + 1; i++) {
      expectBatchDescribeConfigsViaValuesWithFailure(mockAdminClient, brokerCf0, matchingConfig,
          brokerCf1, new TimeoutException("simulated timeout"));
    }
    EasyMock.replay(mockAdminClient);
    assertThrows(IllegalStateException.class, () -> throttleHelper.waitForBatchConfigs(allOps));
  }

  @Test
  public void testConfigsEqual() {
    Map<String, String> expectedConfigs = new HashMap<>();
    List<ConfigEntry> entries = new ArrayList<>();

    assertTrue(ReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));
    expectedConfigs.put("name1", "value1");
    assertFalse(ReplicationThrottleHelper.configsEqual(EMPTY_CONFIG, expectedConfigs));
    entries.add(new ConfigEntry("name1", "value1"));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name2", null);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name2", ""));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name3", null);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name3", null));
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name4", "value4");
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    entries.add(new ConfigEntry("name4", "other-value"));
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name4", "other-value");
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));

    // In the case that a dynamic config is deleted and a static config exists, the comparison is skipped
    ConfigEntry mockStaticConfig = mockConfigEntry("name5", "value5", ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG);
    expectedConfigs.put("name5", null);
    entries.add(mockStaticConfig);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    EasyMock.verify(mockStaticConfig);

    // A deleted per-broker config that resolves to a cluster-wide dynamic default
    // (kafka-configs --entity-default) is also considered applied
    ConfigEntry mockDynamicDefaultConfig = mockConfigEntry("name6", "value6", ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
    expectedConfigs.put("name6", null);
    entries.add(mockDynamicDefaultConfig);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    EasyMock.verify(mockDynamicDefaultConfig);

    // A deleted config that resolves to the Kafka default is also considered applied
    ConfigEntry mockDefaultConfig = mockConfigEntry("name7", "value7", ConfigEntry.ConfigSource.DEFAULT_CONFIG);
    expectedConfigs.put("name7", null);
    entries.add(mockDefaultConfig);
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    EasyMock.verify(mockDefaultConfig);

    // A non-null expected value must still match even when the source is a default layer
    expectedConfigs.put("name6", "value6");
    assertTrue(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name6", "other-value");
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    expectedConfigs.put("name6", null);

    // In the case that a dynamic config is deleted, but the dynamic config is still returned, the configs are not equal
    expectedConfigs.put("name8", null);
    entries.add(mockConfigEntry("name8", "value8", ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG));
    assertFalse(ReplicationThrottleHelper.configsEqual(new Config(entries), expectedConfigs));
    // In the case that a deleted config is reported with the default config source but still with the deleted value, the
    // deletion is not applied yet; a different value from the default config source means that it is applied
    Map<String, String> expectedDeletion = Collections.singletonMap("name9", null);
    Config staleDefaultConfig = new Config(Collections.singletonList(mockConfigEntry("name9", "value9", ConfigEntry.ConfigSource.DEFAULT_CONFIG)));
    assertFalse(ReplicationThrottleHelper.configsEqual(staleDefaultConfig, expectedDeletion, Collections.singletonMap("name9", "value9")));
    assertTrue(ReplicationThrottleHelper.configsEqual(staleDefaultConfig, expectedDeletion, Collections.singletonMap("name9", "other-value")));
    // ... which does not apply to a cluster-wide dynamic default with the deleted value
    Config clusterDefaultConfig =
        new Config(Collections.singletonList(mockConfigEntry("name9", "value9", ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG)));
    assertTrue(ReplicationThrottleHelper.configsEqual(clusterDefaultConfig, expectedDeletion, Collections.singletonMap("name9", "value9")));
  }

  private ExecutionTask prepareMockCompleteTask(ExecutionProposal proposal) {
    ExecutionTask mockCompleteTask = EasyMock.mock(ExecutionTask.class);
    EasyMock.expect(mockCompleteTask.state()).andReturn(ExecutionTaskState.COMPLETED).times(2);
    EasyMock.expect(mockCompleteTask.type()).andReturn(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION).once();
    EasyMock.expect(mockCompleteTask.proposal()).andReturn(proposal).once();
    EasyMock.replay(mockCompleteTask);
    return mockCompleteTask;
  }

  private ConfigEntry mockConfigEntry(String name, String value, ConfigEntry.ConfigSource configSource) {
    ConfigEntry configEntry = EasyMock.mock(ConfigEntry.class);
    EasyMock.expect(configEntry.name()).andReturn(name).atLeastOnce();
    EasyMock.expect(configEntry.value()).andReturn(value).atLeastOnce();
    EasyMock.expect(configEntry.source()).andReturn(configSource).atLeastOnce();
    EasyMock.replay(configEntry);
    return configEntry;
  }

  private void expectListTopics(Admin adminClient, Set<String> topics)
  throws ExecutionException, InterruptedException, TimeoutException {
    ListTopicsResult mockListTopicsResult = EasyMock.mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> mockFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockListTopicsResult.names()).andReturn(mockFuture);
    EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(topics);
    EasyMock.expect(adminClient.listTopics()).andReturn(mockListTopicsResult);
    EasyMock.replay(mockListTopicsResult, mockFuture);
  }

  // --- Batch mock helpers (used by testSetThrottleOnNonExistentTopic and testClearThrottleOnNonExistentTopic) ---

  // Expects a single batched incrementalAlterConfigs call for the given brokers, succeeding via per-broker futures (values()),
  // as the removal of throttled rates writes them.
  private void expectBatchIncrementalBrokerConfigsViaValues(Admin adminClient, List<Integer> brokers) {
    AlterConfigsResult mockAlterConfigsResult = EasyMock.mock(AlterConfigsResult.class);
    Map<ConfigResource, KafkaFuture<Void>> futureMap = new HashMap<>();
    for (int id : brokers) {
      futureMap.put(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)), KafkaFuture.completedFuture(null));
    }
    EasyMock.expect(mockAlterConfigsResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.anyObject())).andReturn(mockAlterConfigsResult);
    EasyMock.replay(mockAlterConfigsResult);
  }

  // Expects a single batched describeConfigs call for a topic, returning per-topic futures via values().
  // If topicExists is false, the topic's future will throw ExecutionException(UnknownTopicOrPartitionException).
  private void expectBatchDescribeTopicConfigsViaValues(Admin adminClient, String topic, Config topicConfig, boolean topicExists)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Config> mockFuture = EasyMock.mock(KafkaFuture.class);
    if (topicExists) {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(topicConfig);
    } else {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
              .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));
    }
    Map<ConfigResource, KafkaFuture<Config>> futureMap = Collections.singletonMap(cf, mockFuture);
    EasyMock.expect(mockDescribeConfigsResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mockDescribeConfigsResult, mockFuture);
  }

  // Expects a single batched incrementalAlterConfigs call for a topic, returning per-topic futures via values().
  // If topicExists is false, the topic's future will throw ExecutionException(UnknownTopicOrPartitionException).
  private void expectBatchIncrementalTopicConfigsViaValues(Admin adminClient, String topic, boolean topicExists)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    AlterConfigsResult mockAlterConfigsResult = EasyMock.mock(AlterConfigsResult.class);
    KafkaFuture<Void> mockFuture = EasyMock.mock(KafkaFuture.class);
    if (topicExists) {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);
    } else {
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
              .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));
    }
    Map<ConfigResource, KafkaFuture<Void>> futureMap = Collections.singletonMap(cf, mockFuture);
    EasyMock.expect(mockAlterConfigsResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.anyObject())).andReturn(mockAlterConfigsResult);
    EasyMock.replay(mockAlterConfigsResult, mockFuture);
  }

  // Expects a batched describeConfigs via values() for two topics: one existing, one non-existing.
  // The existing topic returns its config; the non-existing one throws ExecutionException.
  private void expectBatchDescribeTopicConfigsMixed(
      Admin adminClient, String existingTopic, Config existingConfig, String missingTopic)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource existingCf = new ConfigResource(ConfigResource.Type.TOPIC, existingTopic);
    ConfigResource missingCf = new ConfigResource(ConfigResource.Type.TOPIC, missingTopic);
    DescribeConfigsResult mockResult = EasyMock.mock(DescribeConfigsResult.class);

    KafkaFuture<Config> existingFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(existingFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(existingConfig);

    KafkaFuture<Config> missingFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(missingFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
        .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));

    Map<ConfigResource, KafkaFuture<Config>> futureMap = new HashMap<>();
    futureMap.put(existingCf, existingFuture);
    futureMap.put(missingCf, missingFuture);

    EasyMock.expect(mockResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockResult);
    EasyMock.replay(mockResult, existingFuture, missingFuture);
  }

  // Expects a batched incrementalAlterConfigs via values() for two topics: one succeeds, one fails.
  private void expectBatchIncrementalTopicConfigsMixed(
      Admin adminClient, String existingTopic, String missingTopic)
  throws ExecutionException, InterruptedException, TimeoutException {
    ConfigResource existingCf = new ConfigResource(ConfigResource.Type.TOPIC, existingTopic);
    ConfigResource missingCf = new ConfigResource(ConfigResource.Type.TOPIC, missingTopic);
    AlterConfigsResult mockResult = EasyMock.mock(AlterConfigsResult.class);

    KafkaFuture<Void> existingFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(existingFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(null);

    KafkaFuture<Void> missingFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(missingFuture.get(EasyMock.anyLong(), EasyMock.anyObject()))
        .andThrow(new ExecutionException(new UnknownTopicOrPartitionException()));

    Map<ConfigResource, KafkaFuture<Void>> futureMap = new HashMap<>();
    futureMap.put(existingCf, existingFuture);
    futureMap.put(missingCf, missingFuture);

    EasyMock.expect(mockResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.incrementalAlterConfigs(EasyMock.anyObject())).andReturn(mockResult);
    EasyMock.replay(mockResult, existingFuture, missingFuture);
  }

  // Expects a single batched describeConfigs call for brokers returning per-resource futures via values().
  // Used for the clearThrottles broker config read and for waitForBatchConfigs verification steps.
  private void expectBatchDescribeBrokerConfigsViaValues(Admin adminClient, List<Integer> brokers, Config brokerConfig)
  throws ExecutionException, InterruptedException, TimeoutException {
    Map<ConfigResource, Config> configMap = new HashMap<>();
    for (int id : brokers) {
      configMap.put(new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id)), brokerConfig);
    }
    expectBatchDescribeConfigsViaValues(adminClient, configMap);
  }

  // Same as above, with all brokers already at the default throttle rate (rate=100).
  private void expectBatchDescribeBrokerConfigsViaValues(Admin adminClient, List<Integer> brokers)
  throws ExecutionException, InterruptedException, TimeoutException {
    Config brokerConfig = new Config(Arrays.asList(
      new ConfigEntry(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "100"),
      new ConfigEntry(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "100")));
    expectBatchDescribeBrokerConfigsViaValues(adminClient, brokers, brokerConfig);
  }

  // Expects a single batched describeConfigs call returning per-resource futures via values().
  // Used by testWaitForBatchConfigs to test multi-resource verification.
  private void expectBatchDescribeConfigsViaValues(Admin adminClient, Map<ConfigResource, Config> configMap)
  throws ExecutionException, InterruptedException, TimeoutException {
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    Map<ConfigResource, KafkaFuture<Config>> futureMap = new HashMap<>();
    List<Object> mocksToReplay = new ArrayList<>();
    mocksToReplay.add(mockDescribeConfigsResult);

    for (Map.Entry<ConfigResource, Config> entry : configMap.entrySet()) {
      KafkaFuture<Config> mockFuture = EasyMock.mock(KafkaFuture.class);
      EasyMock.expect(mockFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(entry.getValue());
      futureMap.put(entry.getKey(), mockFuture);
      mocksToReplay.add(mockFuture);
    }

    EasyMock.expect(mockDescribeConfigsResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mocksToReplay.toArray());
  }

  // Expects a single batched describeConfigs call where one resource succeeds and another throws.
  // Used to test per-resource error handling in waitForBatchConfigs.
  private void expectBatchDescribeConfigsViaValuesWithFailure(
      Admin adminClient, ConfigResource successResource, Config successConfig,
      ConfigResource failResource, Exception failException)
  throws ExecutionException, InterruptedException, TimeoutException {
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    Map<ConfigResource, KafkaFuture<Config>> futureMap = new HashMap<>();

    KafkaFuture<Config> successFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(successFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andReturn(successConfig);
    futureMap.put(successResource, successFuture);

    KafkaFuture<Config> failFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(failFuture.get(EasyMock.anyLong(), EasyMock.anyObject())).andThrow(failException);
    futureMap.put(failResource, failFuture);

    EasyMock.expect(mockDescribeConfigsResult.values()).andReturn(futureMap);
    EasyMock.expect(adminClient.describeConfigs(EasyMock.anyObject())).andReturn(mockDescribeConfigsResult);
    EasyMock.replay(mockDescribeConfigsResult, successFuture, failFuture);
  }

  private void assertExpectedThrottledRatesForBroker(int brokerId, long expectedLeaderRate, long expectedFollowerRate)
      throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Config> brokerConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    assertNotNull(brokerConfig.get(cf));
    assertEquals(String.valueOf(expectedLeaderRate),
                 brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG).value());
    assertEquals(String.valueOf(expectedFollowerRate),
                 brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG).value());
  }
  // --- Assertion helpers ---

  private void assertExpectedThrottledRateForBroker(int brokerId, Long expectedRate) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Map<ConfigResource, Config> brokerConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    String expectedString = expectedRate == null ? null : String.valueOf(expectedRate);
    assertNotNull(brokerConfig.get(cf));
    if (expectedRate == null) {
      assertNull(brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG));
      assertNull(brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG));
    } else {
      assertEquals(expectedString, brokerConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG).value());
      assertEquals(expectedString, brokerConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG).value());
    }
  }

  private void waitForClusterDefaultThrottledRate(long expectedRate) {
    for (int i = 0; i < clusterSize(); i++) {
      int brokerId = i;
      waitUntilTrue(() -> hasClusterDefaultThrottledRate(brokerId, expectedRate),
                    String.format("Broker %d does not have the cluster-wide default throttle rate %d", brokerId, expectedRate),
                    10_000L, 100L);
    }
  }

  private boolean hasClusterDefaultThrottledRate(int brokerId, long expectedRate) {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
    Config brokerConfig;
    try {
      brokerConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get().get(cf);
    } catch (InterruptedException | ExecutionException e) {
      return false;
    }
    for (String throttledRateConfig : Arrays.asList(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                                                    ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)) {
      ConfigEntry configEntry = brokerConfig.get(throttledRateConfig);
      if (configEntry == null
          || configEntry.source() != ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
          || !String.valueOf(expectedRate).equals(configEntry.value())) {
        return false;
      }
    }
    return true;
  }

  private void assertExpectedThrottledReplicas(String topic, String expectedReplicas) throws ExecutionException, InterruptedException {
    ConfigResource cf = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Map<ConfigResource, Config> topicConfig = _adminClient.describeConfigs(Collections.singletonList(cf)).all().get();
    assertNotNull(topicConfig.get(cf));
    assertEquals(expectedReplicas, topicConfig.get(cf).get(ReplicationThrottleHelper.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value());
    assertEquals(expectedReplicas, topicConfig.get(cf).get(ReplicationThrottleHelper.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value());
  }
}
