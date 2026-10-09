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

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.waitUntilTrue;
import static com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE;
import static com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver.DEFAULT_CPU_CAPACITY_WITH_CORES;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CPU_CORES;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_NW_IN_CAPACITY;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_NW_OUT_CAPACITY;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.BROKER_CAPACITY_FETCH_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link AutoDiscoveryBrokerCapacityConfigResolver}.
 */
public class AutoDiscoveryBrokerCapacityConfigResolverTest {
  private static final String CAPACITY_FILE = "testCapacityConfigAutoDiscovery.json";
  private static final String LOG_DIR_1 = "/data/kafka-1";
  private static final String LOG_DIR_2 = "/data/kafka-2";
  private static final long TIB = 1L << 40;
  private static final double TIB_IN_MB = 1L << 20;
  private static final double NUM_CORES = 8.0;
  private static final double NW_BYTES_PER_SEC = 1.25e9;
  private static final double NW_KB_PER_SEC = 1220703.125;
  private static final long TIME_MS = 100L;
  private static final double DELTA = 1e-6;

  private static AutoDiscoveryBrokerCapacityConfigResolver resolver(String capacityFile) {
    return resolver(capacityFile, Collections.emptyMap());
  }

  private static AutoDiscoveryBrokerCapacityConfigResolver resolver(String capacityFile, Map<String, Object> extraConfigs) {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = new AutoDiscoveryBrokerCapacityConfigResolver(false);
    resolver.configure(configs(capacityFile, extraConfigs));
    return resolver;
  }

  private static Map<String, Object> configs(String capacityFile, Map<String, Object> extraConfigs) {
    Map<String, Object> configs = new HashMap<>(extraConfigs);
    if (capacityFile != null) {
      ClassLoader classLoader = AutoDiscoveryBrokerCapacityConfigResolverTest.class.getClassLoader();
      configs.put(CAPACITY_CONFIG_FILE, Objects.requireNonNull(classLoader.getResource(capacityFile)).getFile());
    }
    return configs;
  }

  private static void report(AutoDiscoveryBrokerCapacityConfigResolver resolver, int brokerId, long timeMs, double numCores,
                             Double nwBytesPerSec) {
    resolver.onBrokerCapacityMetric(new BrokerMetric(BROKER_CPU_CORES, timeMs, brokerId, numCores));
    if (nwBytesPerSec != null) {
      resolver.onBrokerCapacityMetric(new BrokerMetric(BROKER_NW_IN_CAPACITY, timeMs, brokerId, nwBytesPerSec));
      resolver.onBrokerCapacityMetric(new BrokerMetric(BROKER_NW_OUT_CAPACITY, timeMs, brokerId, nwBytesPerSec));
    }
  }

  private static LogDirDescription online(long totalBytes) {
    return new LogDirDescription(null, Collections.emptyMap(), totalBytes, totalBytes / 2);
  }

  private static LogDirDescription offline() {
    return new LogDirDescription(new KafkaStorageException("The log dir is offline."), Collections.emptyMap());
  }

  private static BrokerCapacityInfo capacity(AutoDiscoveryBrokerCapacityConfigResolver resolver, int brokerId,
                                             boolean allowCapacityEstimation) throws BrokerCapacityResolutionException {
    return resolver.capacityForBroker("rack", "host", brokerId, BROKER_CAPACITY_FETCH_TIMEOUT_MS, allowCapacityEstimation);
  }

  @Test
  public void testDiscoveredCapacity() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(null);
    report(resolver, 0, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(2 * TIB), LOG_DIR_2, online(TIB)));

    BrokerCapacityInfo capacity = capacity(resolver, 0, false);
    assertFalse(capacity.isEstimated());
    assertEquals(NUM_CORES, capacity.numCpuCores(), DELTA);
    assertEquals(DEFAULT_CPU_CAPACITY_WITH_CORES, capacity.capacity().get(Resource.CPU), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_IN), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_OUT), DELTA);
    assertEquals(Map.of(LOG_DIR_1, 2 * TIB_IN_MB, LOG_DIR_2, TIB_IN_MB), capacity.diskCapacityByLogDir());
    assertEquals(3 * TIB_IN_MB, capacity.capacity().get(Resource.DISK), DELTA);
  }

  @Test
  public void testBrokerEntryOverridesDiscoveredCapacity() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    report(resolver, 1, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(1, Map.of(LOG_DIR_1, online(2 * TIB), LOG_DIR_2, online(TIB)));

    BrokerCapacityInfo capacity = capacity(resolver, 1, false);
    assertFalse(capacity.isEstimated());
    // The entry of broker 1 lists only its network capacity and the size of one log dir, so the rest stays discovered.
    assertEquals(NUM_CORES, capacity.numCpuCores(), DELTA);
    assertEquals(600000.0, capacity.capacity().get(Resource.NW_IN), DELTA);
    assertEquals(500000.0, capacity.capacity().get(Resource.NW_OUT), DELTA);
    assertEquals(Map.of(LOG_DIR_1, 500000.0, LOG_DIR_2, TIB_IN_MB), capacity.diskCapacityByLogDir());
    assertEquals(500000.0 + TIB_IN_MB, capacity.capacity().get(Resource.DISK), DELTA);
  }

  @Test
  public void testBrokerEntryLargerThanVolumeIsUsed() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    report(resolver, 1, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(1, Map.of(LOG_DIR_1, online(100L << 20)));

    assertEquals(Map.of(LOG_DIR_1, 500000.0), capacity(resolver, 1, false).diskCapacityByLogDir());
  }

  @Test
  public void testDiscoveredCapacityOverridesDefaultEntry() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    report(resolver, 3, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(3, Map.of(LOG_DIR_1, online(TIB)));

    BrokerCapacityInfo capacity = capacity(resolver, 3, false);
    assertFalse(capacity.isEstimated());
    assertEquals(NUM_CORES, capacity.numCpuCores(), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_IN), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_OUT), DELTA);
    assertEquals(Map.of(LOG_DIR_1, TIB_IN_MB), capacity.diskCapacityByLogDir());
  }

  @Test
  public void testDefaultEntryFillsMissingCapacity() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    // Broker 3 reports its CPU cores but not its network capacity.
    report(resolver, 3, TIME_MS, NUM_CORES, null);
    resolver.updateLogDirs(3, Map.of(LOG_DIR_1, online(TIB)));

    BrokerCapacityInfo capacity = capacity(resolver, 3, true);
    assertTrue(capacity.isEstimated());
    assertTrue(capacity.estimationInfo().contains("NW_IN"));
    assertTrue(capacity.estimationInfo().contains("NW_OUT"));
    assertFalse(capacity.estimationInfo().contains("CPU"));
    assertEquals(NUM_CORES, capacity.numCpuCores(), DELTA);
    assertEquals(50000.0, capacity.capacity().get(Resource.NW_IN), DELTA);
    assertEquals(40000.0, capacity.capacity().get(Resource.NW_OUT), DELTA);
    assertEquals(Map.of(LOG_DIR_1, TIB_IN_MB), capacity.diskCapacityByLogDir());

    assertThrows(BrokerCapacityResolutionException.class, () -> capacity(resolver, 3, false));
  }

  @Test
  public void testOfflineLogDirIsNotCounted() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    report(resolver, 1, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    // The entry of broker 1 lists a size for LOG_DIR_1, but Kafka reports that log dir as offline.
    resolver.updateLogDirs(1, Map.of(LOG_DIR_1, offline(), LOG_DIR_2, online(TIB)));

    BrokerCapacityInfo capacity = capacity(resolver, 1, false);
    assertEquals(Map.of(LOG_DIR_2, TIB_IN_MB), capacity.diskCapacityByLogDir());
    assertEquals(TIB_IN_MB, capacity.capacity().get(Resource.DISK), DELTA);
  }

  @Test
  public void testLogDirsFollowKafka() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(null);
    report(resolver, 0, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(TIB), LOG_DIR_2, online(TIB)));
    assertEquals(2 * TIB_IN_MB, capacity(resolver, 0, false).capacity().get(Resource.DISK), DELTA);

    // A log dir that is removed from the broker configuration is no longer reported by Kafka.
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(TIB)));
    assertEquals(Map.of(LOG_DIR_1, TIB_IN_MB), capacity(resolver, 0, false).diskCapacityByLogDir());

    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, offline()));
    BrokerCapacityInfo capacity = capacity(resolver, 0, false);
    assertEquals(Collections.emptyMap(), capacity.diskCapacityByLogDir());
    assertEquals(0.0, capacity.capacity().get(Resource.DISK), DELTA);
  }

  @Test
  public void testLogDirWithoutSize() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    report(resolver, 3, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    // Kafka does not report the volume size of these log dirs. The default entry gives the size of LOG_DIR_1 only.
    LogDirDescription withoutSize = new LogDirDescription(null, Collections.emptyMap());
    resolver.updateLogDirs(3, Map.of(LOG_DIR_1, withoutSize, "/data/kafka-3", withoutSize));

    BrokerCapacityInfo capacity = capacity(resolver, 3, true);
    assertTrue(capacity.isEstimated());
    assertEquals(Map.of(LOG_DIR_1, 100000.0), capacity.diskCapacityByLogDir());
  }

  @Test
  public void testTotalDiskCapacityInBrokerEntry() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    resolver.updateLogDirs(2, Map.of(LOG_DIR_1, online(TIB)));

    BrokerCapacityInfo capacity = capacity(resolver, 2, false);
    assertFalse(capacity.isEstimated());
    assertEquals(16.0, capacity.numCpuCores(), DELTA);
    assertEquals(300000.0, capacity.capacity().get(Resource.NW_IN), DELTA);
    assertEquals(Map.of(LOG_DIR_1, 800000.0), capacity.diskCapacityByLogDir());
    assertEquals(800000.0, capacity.capacity().get(Resource.DISK), DELTA);

    // A total cannot be split between several log dirs, so the broker keeps the total without capacity per log dir.
    resolver.updateLogDirs(2, Map.of(LOG_DIR_1, online(TIB), LOG_DIR_2, online(TIB)));
    capacity = capacity(resolver, 2, false);
    assertNull(capacity.diskCapacityByLogDir());
    assertEquals(800000.0, capacity.capacity().get(Resource.DISK), DELTA);
  }

  @Test
  public void testFileDiskCapacityBeforeKafkaReportsLogDirs() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);

    BrokerCapacityInfo capacity = capacity(resolver, 2, true);
    assertTrue(capacity.isEstimated());
    assertTrue(capacity.estimationInfo().contains("has not reported the log dirs"));
    assertEquals(800000.0, capacity.capacity().get(Resource.DISK), DELTA);
    assertThrows(BrokerCapacityResolutionException.class, () -> capacity(resolver, 2, false));

    // Broker 3 has no entry, so all of its capacity comes from the default entry.
    capacity = capacity(resolver, 3, true);
    assertTrue(capacity.isEstimated());
    assertEquals(4.0, capacity.numCpuCores(), DELTA);
    assertEquals(50000.0, capacity.capacity().get(Resource.NW_IN), DELTA);
    assertEquals(40000.0, capacity.capacity().get(Resource.NW_OUT), DELTA);
    assertEquals(Map.of(LOG_DIR_1, 100000.0, LOG_DIR_2, 100000.0), capacity.diskCapacityByLogDir());
  }

  @Test
  public void testUnresolvedCapacity() {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(null);
    BrokerCapacityResolutionException e =
        assertThrows(BrokerCapacityResolutionException.class, () -> capacity(resolver, 5, true));
    assertTrue(e.getMessage(), e.getMessage().contains("broker 5 for [CPU, NW_IN, NW_OUT, DISK]"));

    // CPU cores are known, but the network and the disk capacity are not.
    report(resolver, 5, TIME_MS, NUM_CORES, null);
    e = assertThrows(BrokerCapacityResolutionException.class, () -> capacity(resolver, 5, true));
    assertTrue(e.getMessage(), e.getMessage().contains("broker 5 for [NW_IN, NW_OUT, DISK]"));
  }

  @Test
  public void testInvalidCapacityConfigFile() {
    // CPU capacity given as a percentage cannot be combined with discovered CPU cores.
    assertThrows(IllegalArgumentException.class, () -> resolver("testCapacityConfig.json"));
    Map<String, Object> configs = Map.of(CAPACITY_CONFIG_FILE, "/does/not/exist/capacity.json");
    assertThrows(IllegalArgumentException.class, () -> new AutoDiscoveryBrokerCapacityConfigResolver(false).configure(configs));
  }

  @Test
  public void testReportedValues() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(null);
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(TIB)));
    report(resolver, 0, 200L, NUM_CORES, NW_BYTES_PER_SEC);

    // An older report does not replace a newer one.
    report(resolver, 0, 100L, 2 * NUM_CORES, 2 * NW_BYTES_PER_SEC);
    BrokerCapacityInfo capacity = capacity(resolver, 0, false);
    assertEquals(NUM_CORES, capacity.numCpuCores(), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_IN), DELTA);

    // A newer report without the network capacity keeps the last discovered network capacity.
    report(resolver, 0, 300L, 12.0, null);
    capacity = capacity(resolver, 0, false);
    assertEquals(12.0, capacity.numCpuCores(), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_IN), DELTA);

    // Values that are not positive numbers are ignored.
    report(resolver, 0, 400L, 0.0, Double.NaN);
    capacity = capacity(resolver, 0, false);
    assertEquals(12.0, capacity.numCpuCores(), DELTA);
    assertEquals(NW_KB_PER_SEC, capacity.capacity().get(Resource.NW_IN), DELTA);
  }

  @Test
  public void testNumCpuCores() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    // Broker 3 reports only its CPU cores, so its full capacity is estimated, but its CPU cores are not.
    report(resolver, 3, TIME_MS, NUM_CORES, null);
    assertEquals(NUM_CORES, resolver.numCpuCores("rack", "host", 3, false), DELTA);
    assertThrows(BrokerCapacityResolutionException.class, () -> capacity(resolver, 3, false));

    // The entry of broker 2 overrides the discovered CPU cores.
    report(resolver, 2, TIME_MS, NUM_CORES, null);
    assertEquals(16.0, resolver.numCpuCores("rack", "host", 2, false), DELTA);

    // Broker 4 has neither discovered CPU cores nor an entry, so its CPU cores come from the default entry.
    assertEquals(4.0, resolver.numCpuCores("rack", "host", 4, true), DELTA);
    assertThrows(BrokerCapacityResolutionException.class, () -> resolver.numCpuCores("rack", "host", 4, false));

    AutoDiscoveryBrokerCapacityConfigResolver resolverWithoutFile = resolver(null);
    assertThrows(BrokerCapacityResolutionException.class, () -> resolverWithoutFile.numCpuCores("rack", "host", 4, true));
  }

  @Test
  public void testResolvedCapacityIsCachedUntilAnUpdate() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(null);
    report(resolver, 0, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(TIB)));
    BrokerCapacityInfo capacity = capacity(resolver, 0, false);
    assertSame(capacity, capacity(resolver, 0, false));

    // The same values again do not change the resolved capacity.
    report(resolver, 0, TIME_MS + 1, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(TIB)));
    assertSame(capacity, capacity(resolver, 0, false));

    report(resolver, 0, TIME_MS + 2, 2 * NUM_CORES, NW_BYTES_PER_SEC);
    BrokerCapacityInfo updatedCapacity = capacity(resolver, 0, false);
    assertNotSame(capacity, updatedCapacity);
    assertEquals(2 * NUM_CORES, updatedCapacity.numCpuCores(), DELTA);
  }

  @Test
  public void testNegativeBrokerId() {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(CAPACITY_FILE);
    assertThrows(IllegalArgumentException.class, () -> capacity(resolver, -1, true));
    assertThrows(IllegalArgumentException.class, () -> resolver.numCpuCores("rack", "host", -1, true));
  }

  @Test
  public void testClose() throws BrokerCapacityResolutionException {
    AutoDiscoveryBrokerCapacityConfigResolver resolver = resolver(null);
    report(resolver, 0, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    resolver.updateLogDirs(0, Map.of(LOG_DIR_1, online(TIB)));
    capacity(resolver, 0, false);

    resolver.close();
    assertThrows(BrokerCapacityResolutionException.class, () -> capacity(resolver, 0, true));
  }

  @Test
  public void testRefreshLogDirs() throws Exception {
    KafkaFutureImpl<Map<String, LogDirDescription>> failedLogDirs = new KafkaFutureImpl<>();
    failedLogDirs.completeExceptionally(new TimeoutException("Broker 0 did not answer."));
    AdminClient adminClient = mockAdminClient(
        Map.of(0, KafkaFuture.completedFuture(Map.of(LOG_DIR_1, online(TIB))),
               1, KafkaFuture.completedFuture(Map.of(LOG_DIR_1, online(2 * TIB)))),
        Map.of(0, failedLogDirs,
               1, KafkaFuture.completedFuture(Map.of(LOG_DIR_1, offline(), LOG_DIR_2, online(TIB)))));
    AutoDiscoveryBrokerCapacityConfigResolver resolver =
        resolver(null, Map.of(LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG, adminClient));
    report(resolver, 0, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
    report(resolver, 1, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);

    resolver.refreshLogDirs();
    assertEquals(Map.of(LOG_DIR_1, TIB_IN_MB), capacity(resolver, 0, false).diskCapacityByLogDir());
    assertEquals(Map.of(LOG_DIR_1, 2 * TIB_IN_MB), capacity(resolver, 1, false).diskCapacityByLogDir());

    // Broker 0 does not answer, so it keeps its previous log dirs.
    resolver.refreshLogDirs();
    assertEquals(Map.of(LOG_DIR_1, TIB_IN_MB), capacity(resolver, 0, false).diskCapacityByLogDir());
    assertEquals(Map.of(LOG_DIR_2, TIB_IN_MB), capacity(resolver, 1, false).diskCapacityByLogDir());
  }

  @Test
  public void testBackgroundRefreshOfLogDirs() {
    AdminClient adminClient = mockAdminClient(Map.of(0, KafkaFuture.completedFuture(Map.of(LOG_DIR_1, online(TIB)))),
                                              Map.of(0, KafkaFuture.completedFuture(Map.of(LOG_DIR_1, online(TIB)))));
    AutoDiscoveryBrokerCapacityConfigResolver resolver = new AutoDiscoveryBrokerCapacityConfigResolver();
    resolver.configure(configs(null, Map.of(LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG, adminClient,
                                            MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, 100L)));
    try {
      report(resolver, 0, TIME_MS, NUM_CORES, NW_BYTES_PER_SEC);
      waitUntilTrue(() -> {
        try {
          return Map.of(LOG_DIR_1, TIB_IN_MB).equals(capacity(resolver, 0, false).diskCapacityByLogDir());
        } catch (BrokerCapacityResolutionException e) {
          return false;
        }
      }, "The log dirs of broker 0 were not refreshed in the background.", 10000L, 50L);
    } finally {
      resolver.close();
    }
  }

  /**
   * @param firstLogDirs Log dirs that the first call to describe the log dirs returns by broker id.
   * @param laterLogDirs Log dirs that later calls to describe the log dirs return by broker id.
   * @return An admin client that knows the brokers in the given log dirs.
   */
  private static AdminClient mockAdminClient(Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> firstLogDirs,
                                             Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> laterLogDirs) {
    List<Node> brokers = firstLogDirs.keySet().stream().sorted().map(id -> new Node(id, "host" + id, 9092)).toList();
    DescribeClusterResult describeClusterResult = EasyMock.mock(DescribeClusterResult.class);
    EasyMock.expect(describeClusterResult.nodes()).andReturn(KafkaFuture.completedFuture(brokers)).anyTimes();
    DescribeLogDirsResult firstResult = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(firstResult.descriptions()).andReturn(firstLogDirs).anyTimes();
    DescribeLogDirsResult laterResult = EasyMock.mock(DescribeLogDirsResult.class);
    EasyMock.expect(laterResult.descriptions()).andReturn(laterLogDirs).anyTimes();
    AdminClient adminClient = EasyMock.mock(AdminClient.class);
    EasyMock.expect(adminClient.describeCluster()).andReturn(describeClusterResult).anyTimes();
    List<Integer> brokerIds = brokers.stream().map(Node::id).toList();
    EasyMock.expect(adminClient.describeLogDirs(brokerIds)).andReturn(firstResult).once();
    EasyMock.expect(adminClient.describeLogDirs(brokerIds)).andReturn(laterResult).anyTimes();
    EasyMock.replay(describeClusterResult, firstResult, laterResult, adminClient);
    return adminClient;
  }
}
