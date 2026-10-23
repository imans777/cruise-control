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

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.easymock.EasyMock;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.KAFKA_CRUISE_CONTROL_OBJECT_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.DiskFailureDetector.DISK_FAILURES_FIXABLE_CONFIG;
import static com.linkedin.kafka.cruisecontrol.detector.DiskFailureDetector.FAILED_DISKS_OBJECT_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link DiskFailures}.
 */
public class DiskFailuresTest {

  @Test
  public void testFixableByDefault() {
    KafkaCruiseControl mockKafkaCruiseControl = mockKafkaCruiseControl();
    assertTrue(diskFailures(mockKafkaCruiseControl, null).fixable());
    assertTrue(diskFailures(mockKafkaCruiseControl, true).fixable());
    EasyMock.verify(mockKafkaCruiseControl);
  }

  @Test
  public void testUnfixableDiskFailuresAreNotFixed() throws Exception {
    // The strict mock fails the test if fixing the disk failures generates a cluster model or starts an execution.
    KafkaCruiseControl mockKafkaCruiseControl = mockKafkaCruiseControl();
    DiskFailures diskFailures = diskFailures(mockKafkaCruiseControl, false);
    assertFalse(diskFailures.fixable());
    assertTrue(diskFailures.toString().startsWith("Unfixable"));
    assertFalse(diskFailures.fix());
    EasyMock.verify(mockKafkaCruiseControl);
  }

  private static KafkaCruiseControl mockKafkaCruiseControl() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    KafkaCruiseControl mockKafkaCruiseControl = EasyMock.strictMock(KafkaCruiseControl.class);
    EasyMock.expect(mockKafkaCruiseControl.config()).andReturn(config).anyTimes();
    EasyMock.replay(mockKafkaCruiseControl);
    return mockKafkaCruiseControl;
  }

  private static DiskFailures diskFailures(KafkaCruiseControl kafkaCruiseControl, Boolean fixable) {
    Map<String, Object> configs = new HashMap<>();
    configs.put(KAFKA_CRUISE_CONTROL_OBJECT_CONFIG, kafkaCruiseControl);
    configs.put(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, 100L);
    configs.put(FAILED_DISKS_OBJECT_CONFIG, Collections.singletonMap(0, Collections.singletonMap("/data/failed", 100L)));
    if (fixable != null) {
      configs.put(DISK_FAILURES_FIXABLE_CONFIG, fixable);
    }
    return kafkaCruiseControl.config().getConfiguredInstance(AnomalyDetectorConfig.DISK_FAILURES_CLASS_CONFIG, DiskFailures.class, configs);
  }
}
