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

import java.util.List;
import java.util.Set;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_CPU_CORES;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_NW_IN_CAPACITY;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.BROKER_NW_OUT_CAPACITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;


/**
 * Unit test for {@link RawMetricType}.
 */
public class RawMetricTypeTest {
  private static final List<RawMetricType> CAPACITY_METRIC_TYPES = List.of(BROKER_CPU_CORES, BROKER_NW_IN_CAPACITY, BROKER_NW_OUT_CAPACITY);

  @Test
  public void testIdsMatchOrdinals() {
    // Serialization relies on the id of each metric type being its position in the enum.
    for (RawMetricType type : RawMetricType.values()) {
      assertEquals(type.ordinal(), type.id());
      assertEquals(type, RawMetricType.forId(type.id()));
    }
  }

  @Test
  public void testCapacityMetricTypes() {
    assertEquals(CAPACITY_METRIC_TYPES, RawMetricType.capacityMetricTypes());
    for (RawMetricType type : RawMetricType.values()) {
      assertEquals(CAPACITY_METRIC_TYPES.contains(type), type.isCapacityMetric());
    }
  }

  @Test
  public void testCapacityMetricTypesAreNotLoadMetricTypes() {
    for (RawMetricType type : CAPACITY_METRIC_TYPES) {
      assertFalse(RawMetricType.topicMetricTypes().contains(type));
      assertFalse(RawMetricType.partitionMetricTypes().contains(type));
      for (Set<RawMetricType> brokerMetricTypes : RawMetricType.brokerMetricTypesDiffByVersion().values()) {
        assertFalse(brokerMetricTypes.contains(type));
      }
    }
  }

  @Test
  public void testForIdRejectsUnknownIds() {
    assertThrows(IllegalArgumentException.class, () -> RawMetricType.forId((byte) RawMetricType.values().length));
    assertThrows(IllegalArgumentException.class, () -> RawMetricType.forId((byte) -1));
  }
}
