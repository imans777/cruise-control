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

package com.linkedin.kafka.cruisecontrol.servlet.parameters;

import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.BROKER_ID_AND_LOGDIRS_PARAM;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.POPULATE_DISK_INFO_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PartitionLoadParametersTest {
  private static final String ENDPOINT = CruiseControlEndPoint.PARTITION_LOAD.toString();

  private static PartitionLoadParameters parse(Map<String, String> queryParameters) {
    StubRequestContext request = new StubRequestContext("GET", ENDPOINT, queryParameters, null, null);
    PartitionLoadParameters parameters = new PartitionLoadParameters();
    assertFalse(request.errorMessages().toString(), request.parse(parameters));
    return parameters;
  }

  @Test
  public void testDiskInfoIsNotPopulatedByDefault() {
    PartitionLoadParameters parameters = parse(Collections.emptyMap());
    assertFalse(parameters.populateDiskInfo());
    assertTrue(parameters.brokerIdAndLogdirs().isEmpty());
  }

  @Test
  public void testPopulateDiskInfo() {
    PartitionLoadParameters parameters = parse(Map.of(POPULATE_DISK_INFO_PARAM, "true"));
    assertTrue(parameters.populateDiskInfo());
    assertTrue(parameters.brokerIdAndLogdirs().isEmpty());
  }

  @Test
  public void testLogDirFilterImpliesPopulateDiskInfo() {
    PartitionLoadParameters parameters = parse(Map.of(BROKER_ID_AND_LOGDIRS_PARAM, "1-/data/d1,1-/data/d2,2-/data/d1"));
    assertTrue(parameters.populateDiskInfo());
    assertEquals(Map.of(1, Set.of("/data/d1", "/data/d2"), 2, Set.of("/data/d1")), parameters.brokerIdAndLogdirs());

    parameters = parse(Map.of(BROKER_ID_AND_LOGDIRS_PARAM, "1-/data/d1", POPULATE_DISK_INFO_PARAM, "false"));
    assertTrue(parameters.populateDiskInfo());
  }
}
