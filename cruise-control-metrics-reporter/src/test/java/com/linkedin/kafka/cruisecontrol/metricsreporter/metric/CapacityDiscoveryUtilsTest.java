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

package com.linkedin.kafka.cruisecontrol.metricsreporter.metric;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaTestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CapacityDiscoveryUtils.BYTES_PER_SEC_PER_MBIT_PER_SEC;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CapacityDiscoveryUtils.SPEED_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for {@link CapacityDiscoveryUtils}.
 */
public class CapacityDiscoveryUtilsTest {
  private static final long TIME_MS = 123L;
  private static final int BROKER_ID = 7;
  private static final double DELTA = 1e-9;
  private Path _sysfsNetRoot;

  /**
   * Create a fake sysfs network directory with interfaces of different link speeds.
   */
  @Before
  public void setUp() throws IOException {
    _sysfsNetRoot = CCKafkaTestUtils.newTempDir().toPath();
    writeSpeed("eth0", "1000\n");
    writeSpeed("eth1", "25000\n");
    writeSpeed("veth0", "-1\n");
    writeSpeed("bad0", "not-a-number\n");
    Files.createDirectories(_sysfsNetRoot.resolve("nospeed0"));
  }

  private void writeSpeed(String interfaceName, String content) throws IOException {
    Path interfaceDir = Files.createDirectories(_sysfsNetRoot.resolve(interfaceName));
    Files.write(interfaceDir.resolve(SPEED_FILE), content.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testLinkSpeed() throws IOException {
    assertEquals(OptionalLong.of(1000L * BYTES_PER_SEC_PER_MBIT_PER_SEC),
                 CapacityDiscoveryUtils.linkSpeedBytesPerSec(_sysfsNetRoot, "eth0"));
    // 10 Gbit/s is 1.25 GB/s.
    writeSpeed("eth2", "10000");
    assertEquals(OptionalLong.of(1_250_000_000L), CapacityDiscoveryUtils.linkSpeedBytesPerSec(_sysfsNetRoot, "eth2"));
  }

  @Test
  public void testUnknownLinkSpeed() {
    assertEquals(OptionalLong.empty(), CapacityDiscoveryUtils.linkSpeedBytesPerSec(_sysfsNetRoot, "veth0"));
    assertEquals(OptionalLong.empty(), CapacityDiscoveryUtils.linkSpeedBytesPerSec(_sysfsNetRoot, "bad0"));
    assertEquals(OptionalLong.empty(), CapacityDiscoveryUtils.linkSpeedBytesPerSec(_sysfsNetRoot, "nospeed0"));
    assertEquals(OptionalLong.empty(), CapacityDiscoveryUtils.linkSpeedBytesPerSec(_sysfsNetRoot, "missing0"));
  }

  @Test
  public void testFastestLinkSpeed() {
    assertEquals(OptionalLong.of(25000L * BYTES_PER_SEC_PER_MBIT_PER_SEC),
                 CapacityDiscoveryUtils.fastestLinkSpeedBytesPerSec(_sysfsNetRoot, List.of("veth0", "eth0", "bad0", "eth1")));
    assertEquals(OptionalLong.empty(),
                 CapacityDiscoveryUtils.fastestLinkSpeedBytesPerSec(_sysfsNetRoot, List.of("veth0", "bad0", "missing0")));
    assertEquals(OptionalLong.empty(), CapacityDiscoveryUtils.fastestLinkSpeedBytesPerSec(_sysfsNetRoot, Collections.emptyList()));
  }

  @Test
  public void testNetworkCapacityOverride() {
    assertEquals(OptionalLong.of(42L), CapacityDiscoveryUtils.networkCapacityBytesPerSec("", 42L));
    assertEquals(OptionalLong.of(42L), CapacityDiscoveryUtils.networkCapacityBytesPerSec("cc-test-missing-interface", 42L));
  }

  @Test
  public void testConfiguredInterfaceWithoutLinkSpeed() {
    assertEquals(OptionalLong.empty(), CapacityDiscoveryUtils.networkCapacityBytesPerSec("cc-test-missing-interface", -1L));
  }

  @Test
  public void testNumCpuCores() {
    assertEquals(Runtime.getRuntime().availableProcessors(), CapacityDiscoveryUtils.numCpuCores(false), DELTA);
    // Kubernetes mode falls back to the number of available processors when the container has no readable CPU limit.
    assertTrue(CapacityDiscoveryUtils.numCpuCores(true) > 0);
  }

  @Test
  public void testBuildCapacityMetrics() {
    List<CruiseControlMetric> metrics =
        CapacityDiscoveryUtils.buildCapacityMetrics(TIME_MS, BROKER_ID, 2.5, OptionalLong.of(1_250_000_000L));
    assertEquals(3, metrics.size());
    assertCapacityMetric(metrics.get(0), RawMetricType.BROKER_CPU_CORES, 2.5);
    assertCapacityMetric(metrics.get(1), RawMetricType.BROKER_NW_IN_CAPACITY, 1_250_000_000L);
    assertCapacityMetric(metrics.get(2), RawMetricType.BROKER_NW_OUT_CAPACITY, 1_250_000_000L);

    metrics = CapacityDiscoveryUtils.buildCapacityMetrics(TIME_MS, BROKER_ID, 8, OptionalLong.empty());
    assertEquals(1, metrics.size());
    assertCapacityMetric(metrics.get(0), RawMetricType.BROKER_CPU_CORES, 8);
  }

  private static void assertCapacityMetric(CruiseControlMetric metric, RawMetricType expectedType, double expectedValue) {
    assertTrue(metric instanceof BrokerMetric);
    assertEquals(expectedType, metric.rawMetricType());
    assertEquals(TIME_MS, metric.time());
    assertEquals(BROKER_ID, metric.brokerId());
    assertEquals(expectedValue, metric.value(), DELTA);
  }
}
