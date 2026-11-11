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

import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;


/**
 * A {@link BrokerCapacityConfigResolver} that learns broker capacity from the capacity metrics that brokers report, e.g.
 * through {@link com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter} with capacity discovery
 * enabled. If the configured resolver implements this interface, the metric sampler passes every capacity metric it
 * receives to the resolver.
 */
public interface BrokerCapacityMetricsListener {

  /**
   * Called for each broker capacity metric that the metric sampler receives. Implementations must be thread-safe, because
   * several metric fetcher threads may call this method concurrently.
   *
   * @param metric A broker metric whose raw metric type is a capacity metric type, see {@link RawMetricType#isCapacityMetric()}.
   */
  void onBrokerCapacityMetric(BrokerMetric metric);

  /**
   * Get the number of CPU cores of a broker, independently of the capacity of its other resources. The metric sampler uses
   * it to estimate the CPU utilization of partitions, so a missing or estimated capacity of another resource does not stop
   * metric sampling.
   *
   * @param rack The rack of the broker.
   * @param host The host of the broker.
   * @param brokerId The id of the broker.
   * @param allowCapacityEstimation Whether an estimated number of CPU cores is acceptable.
   * @return The number of CPU cores of the broker.
   * @throws BrokerCapacityResolutionException if the number of CPU cores cannot be resolved.
   */
  double numCpuCores(String rack, String host, int brokerId, boolean allowCapacityEstimation) throws BrokerCapacityResolutionException;
}
