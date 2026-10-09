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

package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_ENABLED_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_BYTES_PER_SEC_CONFIG;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_INTERFACE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for the capacity discovery configurations of {@link CruiseControlMetricsReporterConfig}.
 */
public class CruiseControlMetricsReporterConfigTest {

  @Test
  public void testCapacityDiscoveryIsDisabledByDefault() {
    CruiseControlMetricsReporterConfig config = new CruiseControlMetricsReporterConfig(Collections.emptyMap(), false);
    assertFalse(config.getBoolean(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_ENABLED_CONFIG));
    assertEquals("", config.getString(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_INTERFACE_CONFIG));
    assertEquals(-1L, (long) config.getLong(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_BYTES_PER_SEC_CONFIG));
  }

  @Test
  public void testCapacityDiscoveryConfigs() {
    Map<String, String> configs = Map.of(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_ENABLED_CONFIG, "true",
                                         CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_INTERFACE_CONFIG, "eth0",
                                         CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_BYTES_PER_SEC_CONFIG, "125000000");
    CruiseControlMetricsReporterConfig config = new CruiseControlMetricsReporterConfig(configs, false);
    assertTrue(config.getBoolean(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_ENABLED_CONFIG));
    assertEquals("eth0", config.getString(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_INTERFACE_CONFIG));
    assertEquals(125000000L, (long) config.getLong(CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_BYTES_PER_SEC_CONFIG));
  }
}
