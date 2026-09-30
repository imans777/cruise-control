/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Disk;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.LOGDIR1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit test for {@link IntraBrokerDiskUsageDistributionGoal}.
 */
public class IntraBrokerDiskUsageDistributionGoalTest {
  private static final String TOPIC = "topic";

  /**
   * Actions of other goals that keep both disks within the balance limits are acceptable, even if they do not make the
   * disks more balanced. However, such actions are not self-satisfied (i.e. the goal itself does not apply them).
   */
  @Test
  public void testActionAcceptanceWithinLimits() {
    // Both disks have a disk usage of 11100, i.e. with the default disk balance threshold, the balance limits are
    // [11100 * (1 - 0.09), 11100 * (1 + 0.09)] = [10101, 12099].
    ClusterModel clusterModel = DeterministicCluster.getHomogeneousCluster(Collections.singletonMap(0, 0),
                                                                           TestConstants.BROKER_CAPACITY,
                                                                           TestConstants.DISK_CAPACITY);
    createReplica(clusterModel, 0, LOGDIR0, 10000.0);
    createReplica(clusterModel, 1, LOGDIR0, 1000.0);
    createReplica(clusterModel, 2, LOGDIR0, 100.0);
    createReplica(clusterModel, 3, LOGDIR1, 10000.0);
    createReplica(clusterModel, 4, LOGDIR1, 1000.0);
    createReplica(clusterModel, 5, LOGDIR1, 100.0);

    BalancingConstraint balancingConstraint =
        new BalancingConstraint(new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties()));
    IntraBrokerDiskUsageDistributionGoal goal = new IntraBrokerDiskUsageDistributionGoal(balancingConstraint);
    goal.initGoalState(clusterModel, new OptimizationOptions(Collections.emptySet(), Collections.emptySet(), Collections.emptySet()));
    Broker broker = clusterModel.broker(0);
    Disk disk0 = broker.disk(LOGDIR0);
    Disk disk1 = broker.disk(LOGDIR1);

    // Moving the small replica keeps both disks within the limits: 11200 and 11000.
    BalancingAction smallMove = new BalancingAction(new TopicPartition(TOPIC, 5), disk1, disk0, ActionType.INTRA_BROKER_REPLICA_MOVEMENT);
    assertEquals(ACCEPT, goal.actionAcceptance(smallMove, clusterModel));
    assertFalse(goal.selfSatisfied(clusterModel, smallMove));
    // Moving the medium replica pushes the destination disk above the balance upper limit: 12100.
    BalancingAction mediumMove = new BalancingAction(new TopicPartition(TOPIC, 4), disk1, disk0, ActionType.INTRA_BROKER_REPLICA_MOVEMENT);
    assertEquals(REPLICA_REJECT, goal.actionAcceptance(mediumMove, clusterModel));
  }

  private static void createReplica(ClusterModel clusterModel, int partition, String logdir, double diskUsage) {
    String rack = clusterModel.broker(0).rack().id();
    TopicPartition tp = new TopicPartition(TOPIC, partition);
    clusterModel.createReplica(rack, 0, tp, 0, true, false, logdir, false);
    clusterModel.setReplicaLoad(rack, 0, tp, getAggregatedMetricValues(1.0, 10.0, 10.0, diskUsage), Collections.singletonList(1L));
  }
}
