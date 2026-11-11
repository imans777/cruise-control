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

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Discovers the capacity of the broker that runs the metrics reporter: the number of CPU cores available to the broker
 * process and the network capacity. Disk capacity is not discovered here, because Cruise Control reads it from Kafka.
 */
public final class CapacityDiscoveryUtils {
  // Linux reports the link speed of a network interface in megabits per second in this directory.
  static final Path SYSFS_NET_ROOT = Paths.get("/sys/class/net");
  static final String SPEED_FILE = "speed";
  static final long BYTES_PER_SEC_PER_MBIT_PER_SEC = 125_000L;
  private static final Logger LOG = LoggerFactory.getLogger(CapacityDiscoveryUtils.class);

  private CapacityDiscoveryUtils() {
  }

  /**
   * Get the number of CPU cores available to the broker process. In Kubernetes mode, the CPU limit of the container is used
   * if the container has one.
   *
   * @param kubernetesMode {@code true} to take the CPU limit of the container into account.
   * @return The number of CPU cores available to the broker process. It may be fractional in Kubernetes mode.
   */
  public static double numCpuCores(boolean kubernetesMode) {
    if (kubernetesMode) {
      try {
        double cpuLimit = ContainerMetricUtils.getCpuLimit();
        if (cpuLimit > 0) {
          return cpuLimit;
        }
      } catch (IOException | RuntimeException e) {
        LOG.debug("Unable to read the CPU limit of the container. Using the number of available processors instead.", e);
      }
    }
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * Get the network capacity of the broker in bytes per second.
   *
   * @param interfaceName The name of the network interface whose link speed is used. If empty, the fastest network interface
   *                      that is up and is not a loopback interface is used.
   * @param overrideBytesPerSec If positive, this value is returned instead of a discovered link speed.
   * @return The network capacity of the broker in bytes per second, or empty if it cannot be discovered.
   */
  public static OptionalLong networkCapacityBytesPerSec(String interfaceName, long overrideBytesPerSec) {
    if (overrideBytesPerSec > 0) {
      return OptionalLong.of(overrideBytesPerSec);
    }
    if (interfaceName != null && !interfaceName.isEmpty()) {
      return linkSpeedBytesPerSec(SYSFS_NET_ROOT, interfaceName);
    }
    return fastestLinkSpeedBytesPerSec(SYSFS_NET_ROOT, upNonLoopbackInterfaces());
  }

  /**
   * Package private for unit tests.
   *
   * @param sysfsNetRoot The directory that holds one subdirectory per network interface.
   * @param interfaceName The name of the network interface.
   * @return The link speed of the network interface in bytes per second, or empty if it is unknown.
   */
  static OptionalLong linkSpeedBytesPerSec(Path sysfsNetRoot, String interfaceName) {
    Path speedFile = sysfsNetRoot.resolve(interfaceName).resolve(SPEED_FILE);
    try {
      long speedMbitPerSec = Long.parseLong(new String(Files.readAllBytes(speedFile), StandardCharsets.UTF_8).trim());
      // Virtual interfaces report -1 when their speed is unknown.
      return speedMbitPerSec > 0 ? OptionalLong.of(speedMbitPerSec * BYTES_PER_SEC_PER_MBIT_PER_SEC) : OptionalLong.empty();
    } catch (IOException | NumberFormatException e) {
      // The file does not exist outside Linux or for some virtual interfaces, and reading it fails while the link is down.
      LOG.debug("Unable to read the link speed of network interface {} from {}.", interfaceName, speedFile, e);
      return OptionalLong.empty();
    }
  }

  /**
   * Package private for unit tests.
   *
   * @param sysfsNetRoot The directory that holds one subdirectory per network interface.
   * @param interfaceNames The names of the candidate network interfaces.
   * @return The highest known link speed among the given network interfaces in bytes per second, or empty if none is known.
   */
  static OptionalLong fastestLinkSpeedBytesPerSec(Path sysfsNetRoot, Collection<String> interfaceNames) {
    OptionalLong fastest = OptionalLong.empty();
    for (String interfaceName : interfaceNames) {
      OptionalLong linkSpeed = linkSpeedBytesPerSec(sysfsNetRoot, interfaceName);
      if (linkSpeed.isPresent() && (fastest.isEmpty() || linkSpeed.getAsLong() > fastest.getAsLong())) {
        fastest = linkSpeed;
      }
    }
    return fastest;
  }

  private static List<String> upNonLoopbackInterfaces() {
    List<String> interfaceNames = new ArrayList<>();
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      if (networkInterfaces != null) {
        for (NetworkInterface networkInterface : Collections.list(networkInterfaces)) {
          if (networkInterface.isUp() && !networkInterface.isLoopback()) {
            interfaceNames.add(networkInterface.getName());
          }
        }
      }
    } catch (SocketException e) {
      LOG.debug("Unable to list the network interfaces.", e);
    }
    return interfaceNames;
  }

  /**
   * Build the capacity metrics of a broker for one reporting round.
   *
   * @param nowMs The time of the reporting round in milliseconds.
   * @param brokerId The id of the broker.
   * @param numCpuCores The number of CPU cores available to the broker process.
   * @param networkCapacityBytesPerSec The network capacity of the broker in bytes per second, or empty if it is unknown.
   * @return The CPU cores metric, followed by the inbound and outbound network capacity metrics if the network capacity
   * is known.
   */
  public static List<CruiseControlMetric> buildCapacityMetrics(long nowMs,
                                                               int brokerId,
                                                               double numCpuCores,
                                                               OptionalLong networkCapacityBytesPerSec) {
    List<CruiseControlMetric> metrics = new ArrayList<>(3);
    metrics.add(new BrokerMetric(RawMetricType.BROKER_CPU_CORES, nowMs, brokerId, numCpuCores));
    if (networkCapacityBytesPerSec.isPresent()) {
      double bytesPerSec = networkCapacityBytesPerSec.getAsLong();
      metrics.add(new BrokerMetric(RawMetricType.BROKER_NW_IN_CAPACITY, nowMs, brokerId, bytesPerSec));
      metrics.add(new BrokerMetric(RawMetricType.BROKER_NW_OUT_CAPACITY, nowMs, brokerId, bytesPerSec));
    }
    return metrics;
  }
}
