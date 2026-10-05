/*
 * Copyright 2021 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.network.SocketServerConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test util class.
 */
public final class KafkaCruiseControlIntegrationTestUtils {

  public static final String KAFKA_CRUISE_CONTROL_BASE_PATH = WebServerConfig.DEFAULT_WEBSERVER_API_URLPREFIX.replace("*", "").substring(1);
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlIntegrationTestUtils.class);
  private static final Random RANDOM = new Random(0xDEADBEEF);
  private KafkaCruiseControlIntegrationTestUtils() {

  }
  /**
   * Create JSON mapping configuration
   * @return the mapping configuration
   */
  public static Configuration createJsonMappingConfig() {
    return Configuration.builder().jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider()).build();
  }
  /**
   * Find a random open port
   * @return with the port number
   */
  public static Integer findRandomOpenPortOnAllLocalInterfaces() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void waitForConditionMeet(BooleanSupplier condition, int timeOutInSeconds, Error retriesExceededException) {
    waitForConditionMeet(condition, Duration.ofSeconds(timeOutInSeconds), Duration.ofSeconds(5), retriesExceededException);
  }
  /**
   * Execute boolean condition until it returns true
   * @param condition the condition to evaluate
   * @param timeOut the timeout before throwing the retriesExceededException
   * @param retryBackoff the milliseconds between retries
   * @param retriesExceededException the exception if we run out of retries
   */
  public static void waitForConditionMeet(BooleanSupplier condition, 
                                          Duration timeOut, 
                                          Duration retryBackoff, 
                                          Error retriesExceededException) {
    long endTime = System.currentTimeMillis() + timeOut.toMillis();
    while (System.currentTimeMillis() < endTime) {
      boolean conditionResult = false;
      try {
        conditionResult = condition.getAsBoolean();
      } catch (Exception e) {
        LOG.warn("Exception occurred", e);
      }
      if (conditionResult) {
        return;
      } else {
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (retriesExceededException != null) {
      LOG.warn("Retry timeout exceeded");
      throw retriesExceededException;
    }
  }
  
  public static Properties getDefaultProducerProperties(String bootstrapServers) {
    return getDefaultProducerProperties(null, bootstrapServers);
  }
  /**
   * Create default producer properties with string key and value serializer
   * @param overrides the overrides to use
   * @param bootstrapServers the bootstrap servers url
   * @return the created properties
   */
  public static Properties getDefaultProducerProperties(Properties overrides, String bootstrapServers) {
    Properties result = new Properties();

    // populate defaults
    result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    // apply overrides
    if (overrides != null) {
      result.putAll(overrides);
    }
    return result;
  }
  /**
   * Call cruise control REST API 
   * @param serverUrl the server base URL
   * @param path the path with get parameters
   * @return the response body as string
   */
  public static String callCruiseControl(String serverUrl, String path) {
    try {
      HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(serverUrl)
          .resolve(path.toLowerCase(Locale.ROOT)).toURL().openConnection();
      return IOUtils.toString(stateEndpointConnection.getInputStream(), Charset.defaultCharset());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Call a POST endpoint of the cruise control REST API with a JSON request body.
   *
   * @param serverUrl the server base URL
   * @param path the path with query parameters -- unlike {@link #callCruiseControl(String, String)}, it is used as is
   * @param body the JSON request body, or {@code null} to send no body
   * @param headers the request headers
   * @return the status code, headers and body of the response
   */
  public static HttpResult callCruiseControlPost(String serverUrl, String path, String body, Map<String, String> headers) {
    try {
      HttpURLConnection connection = (HttpURLConnection) new URI(serverUrl).resolve(path).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json");
      headers.forEach(connection::setRequestProperty);
      if (body != null) {
        connection.setDoOutput(true);
        try (OutputStream out = connection.getOutputStream()) {
          out.write(body.getBytes(StandardCharsets.UTF_8));
        }
      }
      int statusCode = connection.getResponseCode();
      InputStream in = statusCode < HttpURLConnection.HTTP_BAD_REQUEST ? connection.getInputStream() : connection.getErrorStream();
      String responseBody = in == null ? "" : IOUtils.toString(in, StandardCharsets.UTF_8);
      return new HttpResult(statusCode, connection.getHeaderFields(), responseBody);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The status code, headers and body of an HTTP response.
   */
  public static final class HttpResult {
    private final int _statusCode;
    private final Map<String, List<String>> _headers;
    private final String _body;

    HttpResult(int statusCode, Map<String, List<String>> headers, String body) {
      _statusCode = statusCode;
      _headers = headers;
      _body = body;
    }

    public int statusCode() {
      return _statusCode;
    }

    /**
     * @param name Name of the header -- case-insensitive.
     * @return The first value of the given header, or {@code null} if the response has no such header.
     */
    public String header(String name) {
      return _headers.entrySet().stream().filter(e -> name.equalsIgnoreCase(e.getKey())).findFirst()
                     .map(e -> e.getValue().get(0)).orElse(null);
    }

    public String body() {
      return _body;
    }

    @Override
    public String toString() {
      return String.format("HTTP %d: %s", _statusCode, _body);
    }
  }

  /**
   * Create broker properties with metric reporter configuration
   * @return the created properties
   */
  public static Map<Object, Object> createBrokerProps() {
    Map<Object, Object> props = new HashMap<>();
    props.put("metric.reporters", CruiseControlMetricsReporter.class.getName());
    StringJoiner csvJoiner = new StringJoiner(",");
    csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:"
        + KafkaCruiseControlIntegrationTestUtils.findRandomOpenPortOnAllLocalInterfaces());
    props.put(SocketServerConfigs.LISTENERS_CONFIG, csvJoiner.toString());
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "10000");
    return props;
  }
  /**
   * Create basic Cruise Control overrides for integration tests.
   * @return the configuration
   */
  public static Map<String, Object> ccConfigOverrides() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG, KafkaMetricAnomalyFinder.class.getName());
    configs.put(AnomalyDetectorConfig.TOPIC_ANOMALY_FINDER_CLASSES_CONFIG, 
        TopicReplicationFactorAnomalyFinder.class.getName());
    configs.put(AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG, SelfHealingNotifier.class.getName());
    configs.put(AnomalyDetectorConfig.RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG, "true");
    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG, "40000");
    
    configs.put(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG, CruiseControlMetricsReporterSampler.class.getName());
    configs.put(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, "15000");
    configs.put(MonitorConfig.METADATA_MAX_AGE_MS_CONFIG, "10000");
    
    configs.put(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG, "30000");
    configs.put(MonitorConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG, "20");
    configs.put(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, "30000");
    configs.put(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, "20");

    configs.put(KafkaSampleStore.PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(KafkaSampleStore.BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true");
    configs.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "10000");
    return configs;
  }
  
  public static void produceRandomDataToTopic(String topicName, int messageSize, Properties producerConfig) {
    produceRandomDataToTopic(topicName, 1, messageSize, producerConfig);
  }
  /**
   * Produce random data to a kafka topic
   * @param topicName the topic name
   * @param messageCount the amount of the messages
   * @param messageSize the size of a message
   * @param producerConfig the configuration
   */
  public static void produceRandomDataToTopic(String topicName, int messageCount, int messageSize, Properties producerConfig) {
    if (messageSize > 0 && messageCount > 0) {
      try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
        byte[] randomRecords = new byte[messageSize];
        for (int i = 0; i < messageCount; i++) {
          RANDOM.nextBytes(randomRecords);
          producer.send(new ProducerRecord<>(topicName, Arrays.toString(randomRecords))).get();
          producer.flush();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  public static void createTopic(String brokerAddress, NewTopic topic) {
    createTopic(brokerAddress, Collections.singletonList(topic));
  }
  /**
   * Create topics for testing
   * @param brokerAddress the broker address
   * @param topics the topics to create
   */
  public static void createTopic(String brokerAddress, List<NewTopic> topics) {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections
        .singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress));
    try {
      try {
        adminClient.createTopics(topics).all().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }
}
