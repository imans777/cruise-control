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

import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.exception.BrokerCapacityResolutionException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.BrokerMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE;
import static com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver.DEFAULT_CAPACITY_BROKER_ID;
import static com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver.DEFAULT_CPU_CAPACITY_WITH_CORES;


/**
 * A broker capacity resolver that discovers the capacity of the brokers instead of reading it only from a file.
 * <ul>
 *   <li>Disk capacity comes from the log dirs that Kafka reports through the admin client: only online log dirs count,
 *   each with the size of its volume. Offline log dirs, e.g. on a read-only or corrupt disk, and log dirs that are no
 *   longer configured are never counted. The log dirs are refreshed every {@code metric.sampling.interval.ms}.</li>
 *   <li>CPU cores and network capacity come from the capacity metrics that
 *   {@link com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter} reports when
 *   {@code cruise.control.metrics.reporter.capacity.discovery.enabled} is set on the brokers.</li>
 * </ul>
 * The optional {@link BrokerCapacityConfigFileResolver#CAPACITY_CONFIG_FILE} uses the format of
 * {@link BrokerCapacityConfigFileResolver}, except that entries may list only some resources and CPU capacity must be
 * given as a number of cores. For each resource of a broker, the first source that has a value wins:
 * <ol>
 *   <li>the entry of the broker in the file, to override a discovered value, e.g. to use less disk than the volume has;</li>
 *   <li>the discovered value;</li>
 *   <li>the default entry with broker id {@link BrokerCapacityConfigFileResolver#DEFAULT_CAPACITY_BROKER_ID} in the file.
 *   Values taken from it are flagged as estimated.</li>
 * </ol>
 * For disk capacity, Kafka always decides which log dirs count, and the file only sizes the log dirs it lists. Before
 * Kafka reports the log dirs of a broker, the disk capacity in the file is used as written and flagged as estimated.
 */
public class AutoDiscoveryBrokerCapacityConfigResolver implements BrokerCapacityConfigResolver, BrokerCapacityMetricsListener {
  static final double BYTES_IN_KB = 1024.0;
  static final double BYTES_IN_MB = 1024.0 * 1024.0;
  private static final Logger LOG = LoggerFactory.getLogger(AutoDiscoveryBrokerCapacityConfigResolver.class);
  private static final long DEFAULT_LOG_DIRS_REFRESH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);
  private static final String DEFAULT_ENTRY = "the default entry (brokerId " + DEFAULT_CAPACITY_BROKER_ID + ")";
  private final boolean _startLogDirsRefresher;
  // CPU cores and network capacity in bytes per second reported by the metrics reporter of each broker.
  private final ConcurrentMap<Integer, EnumMap<RawMetricType, ReportedValue>> _reportedValuesByBroker;
  // Online log dirs of each broker with the size of their volume in bytes, or null if Kafka did not report a size.
  private final ConcurrentMap<Integer, Map<String, Long>> _onlineLogDirsByBroker;
  // Resolved capacity of each broker. An entry is removed whenever a discovered value of its broker changes.
  private final ConcurrentMap<Integer, Resolution> _resolutionByBroker;
  private final Set<String> _issuedWarnings;
  private Map<Integer, FileCapacity> _fileCapacityByBroker;
  private String _capacityConfigFile;
  private AdminClient _adminClient;
  private long _logDirsResponseTimeoutMs;
  private ScheduledExecutorService _logDirsRefresher;

  /**
   * Create a resolver that refreshes the log dirs of the brokers in the background once it is configured with an admin client.
   */
  public AutoDiscoveryBrokerCapacityConfigResolver() {
    this(true);
  }

  /**
   * Package private for unit tests, which refresh the log dirs explicitly.
   *
   * @param startLogDirsRefresher {@code true} to refresh the log dirs of the brokers in the background.
   */
  AutoDiscoveryBrokerCapacityConfigResolver(boolean startLogDirsRefresher) {
    _startLogDirsRefresher = startLogDirsRefresher;
    _reportedValuesByBroker = new ConcurrentHashMap<>();
    _onlineLogDirsByBroker = new ConcurrentHashMap<>();
    _resolutionByBroker = new ConcurrentHashMap<>();
    _issuedWarnings = ConcurrentHashMap.newKeySet();
    _fileCapacityByBroker = Collections.emptyMap();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object capacityConfigFile = configs.get(CAPACITY_CONFIG_FILE);
    _capacityConfigFile = capacityConfigFile == null ? "" : capacityConfigFile.toString().trim();
    _fileCapacityByBroker = _capacityConfigFile.isEmpty() ? Collections.emptyMap() : loadFileCapacities(_capacityConfigFile);
    _adminClient = (AdminClient) configs.get(LoadMonitor.KAFKA_ADMIN_CLIENT_OBJECT_CONFIG);
    _logDirsResponseTimeoutMs = longConfig(configs, ExecutorConfig.LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG,
                                           ExecutorConfig.DEFAULT_LOGDIR_RESPONSE_TIMEOUT_MS);
    long logDirsRefreshIntervalMs = longConfig(configs, MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                                               DEFAULT_LOG_DIRS_REFRESH_INTERVAL_MS);
    _resolutionByBroker.clear();
    if (_adminClient == null) {
      LOG.warn("No admin client is available, so disk capacity is not discovered and comes only from {}.", CAPACITY_CONFIG_FILE);
    } else if (_startLogDirsRefresher && _logDirsRefresher == null) {
      _logDirsRefresher = Executors.newSingleThreadScheduledExecutor(
          new KafkaCruiseControlThreadFactory("BrokerCapacityLogDirsRefresher", true, LOG));
      _logDirsRefresher.scheduleWithFixedDelay(this::refreshLogDirsQuietly, 0L, logDirsRefreshIntervalMs, TimeUnit.MILLISECONDS);
    }
    LOG.info("Discovering broker capacity with {}.",
             _capacityConfigFile.isEmpty() ? "no capacity config file" : "capacity config file " + _capacityConfigFile);
  }

  @Override
  public void onBrokerCapacityMetric(BrokerMetric metric) {
    RawMetricType type = metric.rawMetricType();
    if (!type.isCapacityMetric()) {
      return;
    }
    int brokerId = metric.brokerId();
    double value = metric.value();
    if (!Double.isFinite(value) || value <= 0) {
      warnOnce(String.format("Ignoring %s of broker %d, because %s is not a positive number.", type, brokerId, value));
      return;
    }
    boolean[] changed = new boolean[1];
    _reportedValuesByBroker.compute(brokerId, (id, previousValues) -> {
      ReportedValue previous = previousValues == null ? null : previousValues.get(type);
      if (previous != null && previous._timeMs > metric.time()) {
        // Keep the value of a newer report, e.g. when an older report is read after it.
        return previousValues;
      }
      changed[0] = previous == null || Double.compare(previous._value, value) != 0;
      EnumMap<RawMetricType, ReportedValue> values =
          previousValues == null ? new EnumMap<>(RawMetricType.class) : new EnumMap<>(previousValues);
      values.put(type, new ReportedValue(metric.time(), value));
      return values;
    });
    if (changed[0]) {
      _resolutionByBroker.remove(brokerId);
      LOG.info("Discovered {} of broker {}: {}.", type, brokerId, value);
    }
  }

  @Override
  public BrokerCapacityInfo capacityForBroker(String rack, String host, int brokerId, long timeoutMs, boolean allowCapacityEstimation)
      throws BrokerCapacityResolutionException {
    sanityCheckBrokerId(brokerId);
    Resolution resolution = _resolutionByBroker.computeIfAbsent(brokerId, this::resolve);
    if (resolution._capacity == null) {
      throw new BrokerCapacityResolutionException(resolution._error);
    }
    if (resolution._capacity.isEstimated() && !allowCapacityEstimation) {
      throw new BrokerCapacityResolutionException(String.format("%s Capacity estimation is not allowed.",
                                                                resolution._capacity.estimationInfo()));
    }
    return resolution._capacity;
  }

  @Override
  public double numCpuCores(String rack, String host, int brokerId, boolean allowCapacityEstimation)
      throws BrokerCapacityResolutionException {
    sanityCheckBrokerId(brokerId);
    FileCapacity brokerEntry = _fileCapacityByBroker.get(brokerId);
    if (brokerEntry != null && brokerEntry._numCpuCores != null) {
      return brokerEntry._numCpuCores;
    }
    Double discovered = reportedValue(brokerId, RawMetricType.BROKER_CPU_CORES);
    if (discovered != null) {
      return discovered;
    }
    FileCapacity defaultEntry = _fileCapacityByBroker.get(DEFAULT_CAPACITY_BROKER_ID);
    if (defaultEntry != null && defaultEntry._numCpuCores != null) {
      if (allowCapacityEstimation) {
        return defaultEntry._numCpuCores;
      }
      throw new BrokerCapacityResolutionException(String.format("The CPU cores of broker %d come only from %s of %s, and capacity "
                                                                + "estimation is not allowed.", brokerId, DEFAULT_ENTRY,
                                                                _capacityConfigFile));
    }
    throw new BrokerCapacityResolutionException(missingCapacityMessage(brokerId, List.of(Resource.CPU.name())));
  }

  /**
   * Refresh the online log dirs of the alive brokers and the size of their volumes. A broker that does not answer keeps
   * its previously discovered log dirs. Package private for unit tests.
   */
  void refreshLogDirs() throws InterruptedException, ExecutionException, TimeoutException {
    Collection<Node> brokers = _adminClient.describeCluster().nodes().get(_logDirsResponseTimeoutMs, TimeUnit.MILLISECONDS);
    List<Integer> brokerIds = brokers.stream().map(Node::id).collect(Collectors.toList());
    if (brokerIds.isEmpty()) {
      return;
    }
    Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> logDirsByBroker = _adminClient.describeLogDirs(brokerIds).descriptions();
    long deadlineMs = System.currentTimeMillis() + _logDirsResponseTimeoutMs;
    for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : logDirsByBroker.entrySet()) {
      try {
        long remainingMs = Math.max(0L, deadlineMs - System.currentTimeMillis());
        updateLogDirs(entry.getKey(), entry.getValue().get(remainingMs, TimeUnit.MILLISECONDS));
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Unable to describe the log dirs of broker {}. Keeping its previously discovered disk capacity.", entry.getKey(), e);
      }
    }
  }

  private void refreshLogDirsQuietly() {
    try {
      refreshLogDirs();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | TimeoutException | RuntimeException e) {
      LOG.warn("Unable to refresh the log dirs of the brokers. Keeping the previously discovered disk capacity.", e);
    }
  }

  /**
   * Replace the log dirs of the given broker with the given description from Kafka. Package private for unit tests.
   *
   * @param brokerId The id of the broker.
   * @param logDirs Description of every log dir of the broker by its absolute path.
   */
  void updateLogDirs(int brokerId, Map<String, LogDirDescription> logDirs) {
    Map<String, Long> onlineLogDirs = new TreeMap<>();
    Set<String> offlineLogDirs = new TreeSet<>();
    logDirs.forEach((logDir, description) -> {
      if (description.error() == null) {
        onlineLogDirs.put(logDir, description.totalBytes().isPresent() ? description.totalBytes().getAsLong() : null);
      } else {
        offlineLogDirs.add(logDir);
      }
    });
    Map<String, Long> snapshot = Collections.unmodifiableMap(onlineLogDirs);
    Map<String, Long> previous = _onlineLogDirsByBroker.put(brokerId, snapshot);
    if (!snapshot.equals(previous)) {
      _resolutionByBroker.remove(brokerId);
      LOG.info("Discovered online log dirs of broker {} with the size of their volume in bytes: {}.", brokerId, snapshot);
      if (!offlineLogDirs.isEmpty()) {
        LOG.warn("Log dirs {} of broker {} are offline and are not counted in its disk capacity.", offlineLogDirs, brokerId);
      }
    }
  }

  @Override
  public void close() {
    if (_logDirsRefresher != null) {
      _logDirsRefresher.shutdownNow();
      _logDirsRefresher = null;
    }
    _reportedValuesByBroker.clear();
    _onlineLogDirsByBroker.clear();
    _resolutionByBroker.clear();
  }

  private Resolution resolve(int brokerId) {
    FileCapacity brokerEntry = _fileCapacityByBroker.get(brokerId);
    FileCapacity defaultEntry = _fileCapacityByBroker.get(DEFAULT_CAPACITY_BROKER_ID);
    List<String> missing = new ArrayList<>();
    List<String> estimated = new ArrayList<>();
    Double numCpuCores = pick(Resource.CPU, brokerEntry == null ? null : brokerEntry._numCpuCores,
                              reportedValue(brokerId, RawMetricType.BROKER_CPU_CORES),
                              defaultEntry == null ? null : defaultEntry._numCpuCores, missing, estimated);
    Double nwInKbPerSec = pick(Resource.NW_IN, brokerEntry == null ? null : brokerEntry._nwInKbPerSec,
                               bytesToKb(reportedValue(brokerId, RawMetricType.BROKER_NW_IN_CAPACITY)),
                               defaultEntry == null ? null : defaultEntry._nwInKbPerSec, missing, estimated);
    Double nwOutKbPerSec = pick(Resource.NW_OUT, brokerEntry == null ? null : brokerEntry._nwOutKbPerSec,
                                bytesToKb(reportedValue(brokerId, RawMetricType.BROKER_NW_OUT_CAPACITY)),
                                defaultEntry == null ? null : defaultEntry._nwOutKbPerSec, missing, estimated);
    DiskCapacity disk = resolveDisk(brokerId, brokerEntry, defaultEntry, missing, estimated);
    if (!missing.isEmpty()) {
      return new Resolution(null, missingCapacityMessage(brokerId, missing));
    }
    Map<Resource, Double> capacity = new EnumMap<>(Resource.class);
    capacity.put(Resource.CPU, DEFAULT_CPU_CAPACITY_WITH_CORES);
    capacity.put(Resource.NW_IN, nwInKbPerSec);
    capacity.put(Resource.NW_OUT, nwOutKbPerSec);
    capacity.put(Resource.DISK, disk._capacityMb);
    String estimationInfo = estimated.isEmpty() ? null : String.format("The capacity of broker %d is estimated: %s.", brokerId,
                                                                       String.join("; ", estimated));
    BrokerCapacityInfo brokerCapacity =
        new BrokerCapacityInfo(Collections.unmodifiableMap(capacity), estimationInfo, disk._capacityMbByLogDir, numCpuCores);
    LOG.debug("Resolved capacity of broker {}: {}, log dirs: {}, CPU cores: {}, estimation: {}.", brokerId, brokerCapacity.capacity(),
              brokerCapacity.diskCapacityByLogDir(), brokerCapacity.numCpuCores(), brokerCapacity.estimationInfo());
    return new Resolution(brokerCapacity, null);
  }

  private static Double pick(Resource resource, Double fromBrokerEntry, Double discovered, Double fromDefaultEntry,
                             List<String> missing, List<String> estimated) {
    if (fromBrokerEntry != null) {
      return fromBrokerEntry;
    }
    if (discovered != null) {
      return discovered;
    }
    if (fromDefaultEntry != null) {
      estimated.add(resource.name() + " from " + DEFAULT_ENTRY);
      return fromDefaultEntry;
    }
    missing.add(resource.name());
    return null;
  }

  private DiskCapacity resolveDisk(int brokerId, FileCapacity brokerEntry, FileCapacity defaultEntry, List<String> missing,
                                   List<String> estimated) {
    Map<String, Long> onlineLogDirs = _onlineLogDirsByBroker.get(brokerId);
    if (onlineLogDirs == null) {
      // Kafka has not reported the log dirs of the broker yet, so the log dirs in the file cannot be checked.
      String sourceName;
      FileCapacity source;
      if (hasDiskCapacity(brokerEntry)) {
        sourceName = "the entry of the broker";
        source = brokerEntry;
      } else if (hasDiskCapacity(defaultEntry)) {
        sourceName = DEFAULT_ENTRY;
        source = defaultEntry;
      } else {
        missing.add(Resource.DISK.name());
        return new DiskCapacity(null, null);
      }
      estimated.add(String.format("%s from %s as written, because Kafka has not reported the log dirs of the broker yet",
                                  Resource.DISK.name(), sourceName));
      return new DiskCapacity(source._diskCapacityMb, source._diskCapacityMbByLogDir);
    }
    if (brokerEntry != null && brokerEntry._diskCapacityMb != null && brokerEntry._diskCapacityMbByLogDir == null
        && !onlineLogDirs.isEmpty()) {
      // The entry of the broker gives a plain total instead of a size per log dir.
      if (onlineLogDirs.size() == 1) {
        return new DiskCapacity(brokerEntry._diskCapacityMb, Map.of(onlineLogDirs.keySet().iterator().next(), brokerEntry._diskCapacityMb));
      }
      warnOnce(String.format("The entry of broker %d in %s gives a total disk capacity, but the broker has log dirs %s. Give a "
                             + "capacity per log dir to use JBOD operations with this broker.", brokerId, _capacityConfigFile,
                             onlineLogDirs.keySet()));
      return new DiskCapacity(brokerEntry._diskCapacityMb, null);
    }
    Map<String, Double> capacityMbByLogDir = new TreeMap<>();
    for (Map.Entry<String, Long> logDir : onlineLogDirs.entrySet()) {
      Double capacityMb = logDirCapacityMb(brokerId, logDir.getKey(), logDir.getValue(), onlineLogDirs.size(), brokerEntry,
                                           defaultEntry, estimated);
      if (capacityMb != null) {
        capacityMbByLogDir.put(logDir.getKey(), capacityMb);
      }
    }
    double capacityMb = capacityMbByLogDir.values().stream().mapToDouble(Double::doubleValue).sum();
    return new DiskCapacity(capacityMb, Collections.unmodifiableMap(capacityMbByLogDir));
  }

  private Double logDirCapacityMb(int brokerId, String logDir, Long volumeBytes, int numOnlineLogDirs, FileCapacity brokerEntry,
                                  FileCapacity defaultEntry, List<String> estimated) {
    Double fromBrokerEntry = capacityMbFromFile(brokerEntry, logDir, false);
    if (fromBrokerEntry != null) {
      if (volumeBytes != null && fromBrokerEntry > volumeBytes / BYTES_IN_MB) {
        warnOnce(String.format("The capacity of log dir %s of broker %d in %s (%s MB) is larger than its volume (%s MB).", logDir,
                               brokerId, _capacityConfigFile, fromBrokerEntry, volumeBytes / BYTES_IN_MB));
      }
      return fromBrokerEntry;
    }
    if (volumeBytes != null) {
      return volumeBytes / BYTES_IN_MB;
    }
    Double fromDefaultEntry = capacityMbFromFile(defaultEntry, logDir, numOnlineLogDirs == 1);
    if (fromDefaultEntry != null) {
      estimated.add(String.format("%s of log dir %s from %s", Resource.DISK.name(), logDir, DEFAULT_ENTRY));
      return fromDefaultEntry;
    }
    warnOnce(String.format("Kafka did not report the size of log dir %s of broker %d, and %s does not give it either. The log dir "
                           + "is not counted in the disk capacity of the broker.", logDir, brokerId,
                           _capacityConfigFile.isEmpty() ? CAPACITY_CONFIG_FILE : _capacityConfigFile));
    return null;
  }

  /**
   * @param fileCapacity An entry of the capacity config file, or {@code null}.
   * @param logDir An online log dir.
   * @param useTotal {@code true} to use a total disk capacity of the entry for the log dir, i.e. when it is the only log dir.
   * @return The capacity of the log dir in MB given by the entry, or {@code null} if the entry does not give it.
   */
  private static Double capacityMbFromFile(FileCapacity fileCapacity, String logDir, boolean useTotal) {
    if (fileCapacity == null) {
      return null;
    }
    if (fileCapacity._diskCapacityMbByLogDir != null) {
      return fileCapacity._diskCapacityMbByLogDir.get(logDir);
    }
    return useTotal ? fileCapacity._diskCapacityMb : null;
  }

  private static boolean hasDiskCapacity(FileCapacity fileCapacity) {
    return fileCapacity != null && fileCapacity._diskCapacityMb != null;
  }

  private Double reportedValue(int brokerId, RawMetricType type) {
    Map<RawMetricType, ReportedValue> reportedValues = _reportedValuesByBroker.get(brokerId);
    ReportedValue reportedValue = reportedValues == null ? null : reportedValues.get(type);
    return reportedValue == null ? null : reportedValue._value;
  }

  private static Double bytesToKb(Double bytes) {
    return bytes == null ? null : bytes / BYTES_IN_KB;
  }

  private String missingCapacityMessage(int brokerId, List<String> missing) {
    return String.format("Unable to resolve the capacity of broker %d for %s. CPU cores and network capacity are discovered when "
                         + "%s=true is set on the broker, and network capacity can also be set there with %s. Disk capacity is "
                         + "discovered from the log dirs that Kafka reports. Otherwise, add the missing capacity to the entry of "
                         + "the broker or to the default entry (brokerId %d) in %s.", brokerId, missing,
                         CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_ENABLED_CONFIG,
                         CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_CAPACITY_DISCOVERY_NETWORK_BYTES_PER_SEC_CONFIG,
                         DEFAULT_CAPACITY_BROKER_ID, _capacityConfigFile.isEmpty() ? CAPACITY_CONFIG_FILE : _capacityConfigFile);
  }

  private void warnOnce(String message) {
    if (_issuedWarnings.add(message)) {
      LOG.warn(message);
    }
  }

  private static void sanityCheckBrokerId(int brokerId) {
    if (brokerId < 0) {
      throw new IllegalArgumentException("The broker id(" + brokerId + ") should be non-negative.");
    }
  }

  private static long longConfig(Map<String, ?> configs, String name, long defaultValue) {
    Object value = configs.get(name);
    if (value == null) {
      return defaultValue;
    }
    return value instanceof Number ? ((Number) value).longValue() : Long.parseLong(value.toString().trim());
  }

  private static Map<Integer, FileCapacity> loadFileCapacities(String capacityConfigFile) {
    Map<Integer, Map<Resource, Object>> rawCapacities;
    try {
      rawCapacities = BrokerCapacityConfigFileResolver.readRawCapacities(capacityConfigFile);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
    Map<Integer, FileCapacity> fileCapacityByBroker = new HashMap<>();
    rawCapacities.forEach((brokerId, capacity) -> fileCapacityByBroker.put(brokerId, FileCapacity.parse(brokerId, capacity,
                                                                                                          capacityConfigFile)));
    return Collections.unmodifiableMap(fileCapacityByBroker);
  }

  /**
   * A value reported by the metrics reporter of a broker.
   */
  private static final class ReportedValue {
    private final long _timeMs;
    private final double _value;

    private ReportedValue(long timeMs, double value) {
      _timeMs = timeMs;
      _value = value;
    }
  }

  /**
   * The resolved capacity of a broker, or the reason why it cannot be resolved.
   */
  private static final class Resolution {
    private final BrokerCapacityInfo _capacity;
    private final String _error;

    private Resolution(BrokerCapacityInfo capacity, String error) {
      _capacity = capacity;
      _error = error;
    }
  }

  /**
   * The disk capacity of a broker in MB, with the capacity of each log dir if known.
   */
  private static final class DiskCapacity {
    private final Double _capacityMb;
    private final Map<String, Double> _capacityMbByLogDir;

    private DiskCapacity(Double capacityMb, Map<String, Double> capacityMbByLogDir) {
      _capacityMb = capacityMb;
      _capacityMbByLogDir = capacityMbByLogDir;
    }
  }

  /**
   * The capacity given by one entry of the capacity config file. A field is {@code null} if the entry does not list it.
   */
  private static final class FileCapacity {
    private final Double _numCpuCores;
    private final Double _nwInKbPerSec;
    private final Double _nwOutKbPerSec;
    private final Double _diskCapacityMb;
    private final Map<String, Double> _diskCapacityMbByLogDir;

    private FileCapacity(Double numCpuCores, Double nwInKbPerSec, Double nwOutKbPerSec, Double diskCapacityMb,
                         Map<String, Double> diskCapacityMbByLogDir) {
      _numCpuCores = numCpuCores;
      _nwInKbPerSec = nwInKbPerSec;
      _nwOutKbPerSec = nwOutKbPerSec;
      _diskCapacityMb = diskCapacityMb;
      _diskCapacityMbByLogDir = diskCapacityMbByLogDir;
    }

    private static FileCapacity parse(int brokerId, Map<Resource, Object> capacity, String capacityConfigFile) {
      if (capacity == null || capacity.isEmpty()) {
        return new FileCapacity(null, null, null, null, null);
      }
      Double numCpuCores = BrokerCapacityConfigFileResolver.getUserSpecifiedNumCores(capacity);
      if (capacity.containsKey(Resource.CPU) && numCpuCores == null) {
        throw new IllegalArgumentException(String.format("The CPU capacity of the entry with brokerId %d in %s must be given as "
                                                         + "{\"num.cores\": \"<number of cores>\"}, because %s expresses CPU "
                                                         + "capacity in cores.", brokerId, capacityConfigFile,
                                                         AutoDiscoveryBrokerCapacityConfigResolver.class.getSimpleName()));
      }
      Map<Resource, Double> totalCapacity = BrokerCapacityConfigFileResolver.getTotalCapacity(capacity, numCpuCores != null);
      Map<String, Double> diskCapacityByLogDir = BrokerCapacityConfigFileResolver.getDiskCapacityByLogDir(capacity);
      return new FileCapacity(numCpuCores, totalCapacity.get(Resource.NW_IN), totalCapacity.get(Resource.NW_OUT),
                              totalCapacity.get(Resource.DISK),
                              diskCapacityByLogDir == null ? null : Collections.unmodifiableMap(new TreeMap<>(diskCapacityByLogDir)));
    }
  }
}
