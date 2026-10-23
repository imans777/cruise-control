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

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.anomalyComparator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link DiskFailureDetector}.
 */
public class DiskFailureDetectorTest {
  private static final String TOPIC = "topic";
  private static final String HEALTHY_LOGDIR = "/data/healthy";
  private static final String FAILED_LOGDIR = "/data/failed";
  private static final List<Node> NODES = Arrays.asList(new Node(0, "host0", 9092),
                                                        new Node(1, "host1", 9092),
                                                        new Node(2, "host2", 9092));
  private final Time _time = new MockTime(0L, 100L, 0L);
  private final Queue<Anomaly> _anomalies = new PriorityBlockingQueue<>(10, anomalyComparator());
  // Log dirs reported by describeLogDirs.
  private Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> _logDirsByBroker;
  // Partitions in Kafka metadata.
  private List<PartitionInfo> _partitions;
  private int _clusterGeneration;

  /**
   * Start with healthy disks on all brokers and no partitions.
   */
  @Before
  public void setUp() {
    _logDirsByBroker = new HashMap<>();
    for (Node node : NODES) {
      _logDirsByBroker.put(node.id(), logDirs(false));
    }
    _partitions = new ArrayList<>();
  }

  @Test
  public void testReportFailedDiskWithOfflineReplicas() {
    DiskFailureDetector detector = createDiskFailureDetector();
    _logDirsByBroker.put(0, logDirs(true));
    // Broker 0 has an offline replica of a partition with a leader.
    _partitions.add(partition(0, NODES.get(1), Arrays.asList(0, 1), Collections.singletonList(0)));

    runDetector(detector);
    assertEquals(1, _anomalies.size());
    DiskFailures diskFailures = (DiskFailures) _anomalies.poll();
    assertTrue(diskFailures.fixable());
    assertEquals(Collections.singletonMap(0, Collections.singletonMap(FAILED_LOGDIR, _time.milliseconds())),
                 diskFailures.failedDisks());
  }

  @Test
  public void testSkipFailedDiskWithoutOfflineReplicas() {
    DiskFailureDetector detector = createDiskFailureDetector();
    _logDirsByBroker.put(0, logDirs(true));
    // The replicas on the failed disk of broker 0 have already been moved away, so broker 0 has no offline replicas.
    _partitions.add(partition(0, NODES.get(1), Arrays.asList(1, 2), Collections.emptyList()));

    // The failed disk stays failed in describeLogDirs until it is removed from the log.dirs of broker 0, but it is not reported.
    for (int i = 0; i < 3; i++) {
      runDetector(detector);
      assertTrue(_anomalies.isEmpty());
    }
  }

  @Test
  public void testReportFailedDiskWithOfflineReplicasOfLeaderlessPartitionsAsUnfixable() throws Exception {
    DiskFailureDetector detector = createDiskFailureDetector();
    _logDirsByBroker.put(0, logDirs(true));
    // The only replica of the partition was on the failed disk of broker 0, so the partition has no leader.
    _partitions.add(partition(0, null, Collections.singletonList(0), Collections.singletonList(0)));

    runDetector(detector);
    assertEquals(1, _anomalies.size());
    DiskFailures diskFailures = (DiskFailures) _anomalies.poll();
    assertFalse(diskFailures.fixable());
    assertFalse(diskFailures.fix());
  }

  @Test
  public void testReportOnlyFailedDisksOfBrokersWithOfflineReplicas() {
    DiskFailureDetector detector = createDiskFailureDetector();
    _logDirsByBroker.put(0, logDirs(true));
    _logDirsByBroker.put(1, logDirs(true));
    // Broker 0 has an offline replica. Broker 1 has none, so its failed disk is empty.
    _partitions.add(partition(0, NODES.get(2), Arrays.asList(0, 2), Collections.singletonList(0)));
    _partitions.add(partition(1, NODES.get(2), Arrays.asList(1, 2), Collections.emptyList()));

    runDetector(detector);
    assertEquals(1, _anomalies.size());
    DiskFailures diskFailures = (DiskFailures) _anomalies.poll();
    assertTrue(diskFailures.fixable());
    assertEquals(Collections.singleton(0), diskFailures.failedDisks().keySet());
  }

  /**
   * Run the detector on a new cluster model generation, so that it does not skip the detection.
   *
   * @param detector Disk failure detector to run.
   */
  private void runDetector(DiskFailureDetector detector) {
    _clusterGeneration++;
    detector.run();
  }

  private Cluster cluster() {
    return new Cluster("id", NODES, _partitions, Collections.emptySet(), Collections.emptySet());
  }

  private static PartitionInfo partition(int partition, Node leader, List<Integer> replicas, List<Integer> offlineReplicas) {
    Node[] replicaNodes = replicas.stream().map(NODES::get).toArray(Node[]::new);
    Node[] offlineReplicaNodes = offlineReplicas.stream().map(NODES::get).toArray(Node[]::new);
    Node[] inSyncReplicaNodes = replicas.stream().filter(id -> !offlineReplicas.contains(id)).map(NODES::get).toArray(Node[]::new);
    return new PartitionInfo(TOPIC, partition, leader, replicaNodes, inSyncReplicaNodes, offlineReplicaNodes);
  }

  private static KafkaFuture<Map<String, LogDirDescription>> logDirs(boolean hasFailedLogdir) {
    Map<String, LogDirDescription> logDirs = new HashMap<>();
    logDirs.put(HEALTHY_LOGDIR, new LogDirDescription(null, Collections.emptyMap()));
    if (hasFailedLogdir) {
      logDirs.put(FAILED_LOGDIR, new LogDirDescription(new KafkaStorageException("Disk failure"), Collections.emptyMap()));
    }
    return KafkaFuture.completedFuture(logDirs);
  }

  private DiskFailureDetector createDiskFailureDetector() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    DescribeLogDirsResult mockDescribeLogDirsResult = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(mockDescribeLogDirsResult.descriptions()).andAnswer(() -> _logDirsByBroker).anyTimes();
    Admin mockAdminClient = EasyMock.mock(Admin.class);
    EasyMock.expect(mockAdminClient.describeLogDirs(EasyMock.anyObject())).andReturn(mockDescribeLogDirsResult).anyTimes();

    LoadMonitor mockLoadMonitor = EasyMock.mock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.clusterModelGeneration()).andAnswer(() -> new ModelGeneration(_clusterGeneration, 0L)).anyTimes();
    EasyMock.expect(mockLoadMonitor.deadBrokersWithReplicas(EasyMock.anyLong())).andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockLoadMonitor.refreshClusterAndGeneration())
            .andAnswer(() -> new MetadataClient.ClusterAndGeneration(cluster(), _clusterGeneration)).anyTimes();

    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.mock(KafkaCruiseControl.class);
    EasyMock.expect(mockKafkaCruiseControl.adminClient()).andReturn(mockAdminClient).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(config).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.loadMonitor()).andReturn(mockLoadMonitor).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.kafkaCluster()).andAnswer(this::cluster).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.timeMs()).andAnswer(_time::milliseconds).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.getLoadMonitorTaskRunnerState())
            .andReturn(LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING).anyTimes();
    EasyMock.expect(mockKafkaCruiseControl.executionState()).andReturn(ExecutorState.State.NO_TASK_IN_PROGRESS).anyTimes();
    EasyMock.replay(mockDescribeLogDirsResult, mockAdminClient, mockLoadMonitor, mockKafkaCruiseControl);

    return new DiskFailureDetector(_anomalies, mockKafkaCruiseControl);
  }
}
