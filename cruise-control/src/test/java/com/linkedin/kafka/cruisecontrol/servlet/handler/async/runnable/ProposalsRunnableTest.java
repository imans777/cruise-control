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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.DeterministicCluster.unbalanced;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DEFAULT_START_TIME_FOR_CLUSTER_MODEL;
import static org.junit.Assert.assertSame;


/**
 * Unit test for populating the replica placement over disks in the cluster model used by {@link ProposalsRunnable}.
 */
public class ProposalsRunnableTest {
  private static final long MOCK_TIME_MS = 100L;
  private static final ModelCompletenessRequirements REQUIREMENTS = new ModelCompletenessRequirements(1, 0.0, false);
  private static final boolean IS_REBALANCE_DISK_MODE = true;
  private static final boolean POPULATE_REPLICA_PLACEMENT_INFO = true;
  private static final List<String> INTER_BROKER_GOALS = List.of(DiskUsageDistributionGoal.class.getSimpleName());
  private static final List<String> INTRA_BROKER_GOALS = List.of(IntraBrokerDiskCapacityGoal.class.getSimpleName(),
                                                                 IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());
  private static final List<String> INTER_BROKER_AND_INTRA_BROKER_GOALS =
      List.of(DiskUsageDistributionGoal.class.getSimpleName(), IntraBrokerDiskCapacityGoal.class.getSimpleName(),
              IntraBrokerDiskUsageDistributionGoal.class.getSimpleName());
  private KafkaCruiseControlConfig _config;
  private KafkaCruiseControl _mockKafkaCruiseControl;
  private OptimizerResult _optimizerResult;

  /**
   * Setup the unit test.
   */
  @Before
  public void setup() {
    _config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    _mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    _optimizerResult = EasyMock.mock(OptimizerResult.class);
  }

  @Test
  public void testInterBrokerGoalsDoNotPopulateReplicaPlacementInfo()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    verifyPopulateReplicaPlacementInfo(INTER_BROKER_GOALS, !IS_REBALANCE_DISK_MODE, !POPULATE_REPLICA_PLACEMENT_INFO);
  }

  @Test
  public void testIntraBrokerGoalsInRebalanceDiskModePopulateReplicaPlacementInfo()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    verifyPopulateReplicaPlacementInfo(INTRA_BROKER_GOALS, IS_REBALANCE_DISK_MODE, POPULATE_REPLICA_PLACEMENT_INFO);
  }

  @Test
  public void testInterBrokerAndIntraBrokerGoalsPopulateReplicaPlacementInfo()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    verifyPopulateReplicaPlacementInfo(INTER_BROKER_AND_INTRA_BROKER_GOALS, !IS_REBALANCE_DISK_MODE, POPULATE_REPLICA_PLACEMENT_INFO);
  }

  @Test
  public void testIntraBrokerGoalsInDefaultGoalsPopulateReplicaPlacementInfo()
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AnalyzerConfig.DEFAULT_GOALS_CONFIG, String.join(",", TestConstants.DEFAULT_GOALS_VALUES,
                                                                       IntraBrokerDiskCapacityGoal.class.getName(),
                                                                       IntraBrokerDiskUsageDistributionGoal.class.getName()));
    _config = new KafkaCruiseControlConfig(props);
    // Empty goals indicate the use of default goals.
    verifyPopulateReplicaPlacementInfo(Collections.emptyList(), !IS_REBALANCE_DISK_MODE, POPULATE_REPLICA_PLACEMENT_INFO);
  }

  private void verifyPopulateReplicaPlacementInfo(List<String> goals, boolean isRebalanceDiskMode, boolean populateReplicaPlacementInfo)
      throws KafkaCruiseControlException, InterruptedException, TimeoutException, NotEnoughValidWindowsException {
    ClusterModel clusterModel = unbalanced();

    // Expect mocks.
    EasyMock.expect(_mockKafkaCruiseControl.config()).andReturn(_config).anyTimes();
    _mockKafkaCruiseControl.sanityCheckDryRun(true, false);
    EasyMock.expect(_mockKafkaCruiseControl.modelCompletenessRequirements(EasyMock.anyObject())).andReturn(REQUIREMENTS);
    EasyMock.expect(_mockKafkaCruiseControl.getLoadMonitorTaskRunnerState())
            .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.SAMPLING);
    // The proposal cache is computed without the replica placement over disks, hence it must be ignored if the optimization needs it.
    EasyMock.expect(_mockKafkaCruiseControl.ignoreProposalCache(EasyMock.eq(goals),
                                                                EasyMock.anyObject(),
                                                                EasyMock.isNull(),
                                                                EasyMock.eq(false),
                                                                EasyMock.eq(false),
                                                                EasyMock.eq(false),
                                                                EasyMock.eq(Collections.emptySet()),
                                                                EasyMock.eq(populateReplicaPlacementInfo))).andReturn(true);
    EasyMock.expect(_mockKafkaCruiseControl.acquireForModelGeneration(EasyMock.anyObject())).andReturn(null);
    EasyMock.expect(_mockKafkaCruiseControl.timeMs()).andReturn(MOCK_TIME_MS);
    EasyMock.expect(_mockKafkaCruiseControl.clusterModel(EasyMock.eq(DEFAULT_START_TIME_FOR_CLUSTER_MODEL),
                                                         EasyMock.eq(MOCK_TIME_MS),
                                                         EasyMock.anyObject(),
                                                         EasyMock.eq(populateReplicaPlacementInfo),
                                                         EasyMock.eq(true),
                                                         EasyMock.anyObject())).andReturn(clusterModel);
    EasyMock.expect(_mockKafkaCruiseControl.executorState())
            .andReturn(ExecutorState.noTaskInProgress(Collections.emptySet(), Collections.emptySet()));
    EasyMock.expect(_mockKafkaCruiseControl.excludedTopics(clusterModel, null)).andReturn(Collections.emptySet());
    EasyMock.expect(_mockKafkaCruiseControl.optimizations(EasyMock.eq(clusterModel),
                                                          EasyMock.anyObject(),
                                                          EasyMock.anyObject(),
                                                          EasyMock.isNull(),
                                                          EasyMock.anyObject())).andReturn(_optimizerResult);

    // Replay mocks.
    EasyMock.replay(_mockKafkaCruiseControl, _optimizerResult);
    ProposalsRunnable proposalsRunnable = new ProposalsRunnable(_mockKafkaCruiseControl, new OperationFuture("Test"), goals,
                                                                REQUIREMENTS, true, null, false, false, false,
                                                                Collections.emptySet(), isRebalanceDiskMode, true, false, false);
    assertSame(_optimizerResult, proposalsRunnable.computeResult());

    // Verify mocks.
    EasyMock.verify(_mockKafkaCruiseControl, _optimizerResult);
  }
}
