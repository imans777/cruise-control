/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.RebalanceRunnable;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.getSelfHealingIntraBrokerGoalNames;
import static com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType.INTRA_BROKER_GOAL_VIOLATION;


/**
 * A class that holds all the intra-broker goal violations -- i.e. imbalance across the disks of brokers in a JBOD deployment,
 * which are fixed by rebalancing the disks of brokers (i.e. moving replicas between the disks of the same broker).
 */
public class IntraBrokerGoalViolations extends AbstractGoalViolations {
  // The brokers whose disks the detection goals rebalanced. The self-healing touches only these brokers: the runnable, created
  // when the anomaly is configured (i.e. before detection), gets a view of this set as its destination brokers.
  private final Set<Integer> _violatingBrokers = Collections.synchronizedSet(new TreeSet<>());

  /**
   * An anomaly to indicate intra-broker goal violation(s).
   */
  public IntraBrokerGoalViolations() {
  }

  /**
   * Add brokers whose disks a detection goal rebalanced, i.e. the brokers to heal.
   *
   * @param brokerIds Ids of the brokers.
   */
  public void addViolatingBrokers(Collection<Integer> brokerIds) {
    _violatingBrokers.addAll(brokerIds);
  }

  /**
   * @return The brokers whose disks the detection goals rebalanced, i.e. the brokers that self-healing rebalances.
   */
  public Set<Integer> violatingBrokers() {
    synchronized (_violatingBrokers) {
      return new TreeSet<>(_violatingBrokers);
    }
  }

  @Override
  protected String details() {
    return String.format(", Brokers to heal: %s", violatingBrokers());
  }

  @Override
  public AnomalyType anomalyType() {
    return INTRA_BROKER_GOAL_VIOLATION;
  }

  @Override
  protected RebalanceRunnable rebalanceRunnable(KafkaCruiseControl kafkaCruiseControl,
                                                KafkaCruiseControlConfig config,
                                                boolean allowCapacityEstimation) {
    return new RebalanceRunnable(kafkaCruiseControl,
                                 getSelfHealingIntraBrokerGoalNames(config),
                                 allowCapacityEstimation,
                                 _excludeRecentlyDemotedBrokers,
                                 _excludeRecentlyRemovedBrokers,
                                 _anomalyId.toString(),
                                 reasonSupplier(),
                                 stopOngoingExecution(),
                                 true,
                                 Collections.unmodifiableSet(_violatingBrokers));
  }
}
