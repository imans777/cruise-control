/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

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

import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;


public class MetricSerde implements Serializer<CruiseControlMetric>, Deserializer<CruiseControlMetric> {

  // The overhead of the type bytes
  private static final int METRIC_TYPE_OFFSET = 0;
  private static final int HEADER_LENGTH = 1;

  /**
   * Serialize the Cruise Control metric to a byte array.
   *
   * @param metric Metric to be serialized.
   * @return Serialized Cruise Control metric as a byte array.
   */
  public static byte[] toBytes(CruiseControlMetric metric) {
    ByteBuffer byteBuffer = metric.toBuffer(HEADER_LENGTH);
    byteBuffer.put(METRIC_TYPE_OFFSET, metric.metricClassId().id());
    return byteBuffer.array();
  }

  /**
   * Deserialize from byte array to Cruise Control metric
   * @param bytes Bytes array corresponding to Cruise Control metric.
   * @return Deserialized byte array as Cruise Control metric, or {@code null} if the metric class or the raw metric type is
   * unknown to this version, e.g. because a newer metrics reporter added it.
   */
  public static CruiseControlMetric fromBytes(byte[] bytes) throws UnknownVersionException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try {
      switch (CruiseControlMetric.MetricClassId.forId(buffer.get())) {
        case BROKER_METRIC:
          return BrokerMetric.fromBuffer(buffer);
        case TOPIC_METRIC:
          return TopicMetric.fromBuffer(buffer);
        case PARTITION_METRIC:
          return PartitionMetric.fromBuffer(buffer);
        default:
          return null;
      }
    } catch (IllegalArgumentException e) {
      // This happens when a new metric class or raw metric type is added but we are still running the old code.
      // Simply ignore the metric by returning a null.
      return null;
    }
  }

  @Override
  public CruiseControlMetric deserialize(String topic, byte[] bytes) {
    try {
      return fromBytes(bytes);
    } catch (Exception e) {
      throw new RuntimeException("Error occurred when deserialize Cruise Control metrics.", e);
    }
  }

  @Override
  public void configure(Map<String, ?> map, boolean b) {

  }

  @Override
  public byte[] serialize(String s, CruiseControlMetric cruiseControlMetric) {
    return toBytes(cruiseControlMetric);
  }

  @Override
  public void close() {

  }
}
