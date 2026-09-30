/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor.persistence;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskState;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.TopicPartition;


/**
 * Serializes / deserializes a {@link PersistedExecutionState} to / from JSON.
 *
 * The JSON document is built explicitly (rather than via reflection) to keep the persisted format stable and independent
 * of the internal representation of the executor classes.
 */
public final class PersistedExecutionStateSerde {
  static final String VERSION = "version";
  static final String UUID = "uuid";
  static final String REASON = "reason";
  static final String OPERATION = "operation";
  static final String TRIGGERED_BY_USER_REQUEST = "triggeredByUserRequest";
  static final String START_TIME_MS = "startTimeMs";
  static final String LAST_UPDATE_TIME_MS = "lastUpdateTimeMs";
  static final String RESUME_COUNT = "resumeCount";
  static final String EXECUTOR_STATE = "executorState";
  static final String PARAMETERS = "parameters";
  static final String REMOVED_BROKERS = "removedBrokers";
  static final String DEMOTED_BROKERS = "demotedBrokers";
  static final String BROKERS_TO_SKIP_CONCURRENCY_CHECK = "brokersToSkipConcurrencyCheck";
  static final String INTER_BROKER_PARTITION_MOVEMENT_CONCURRENCY = "interBrokerPartitionMovementConcurrency";
  static final String MAX_INTER_BROKER_PARTITION_MOVEMENTS = "maxInterBrokerPartitionMovements";
  static final String INTRA_BROKER_PARTITION_MOVEMENT_CONCURRENCY = "intraBrokerPartitionMovementConcurrency";
  static final String CLUSTER_LEADERSHIP_MOVEMENT_CONCURRENCY = "clusterLeadershipMovementConcurrency";
  static final String BROKER_LEADERSHIP_MOVEMENT_CONCURRENCY = "brokerLeadershipMovementConcurrency";
  static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS = "executionProgressCheckIntervalMs";
  static final String REPLICA_MOVEMENT_STRATEGY = "replicaMovementStrategy";
  static final String REPLICATION_THROTTLE = "replicationThrottle";
  static final String KAFKA_ASSIGNER_MODE = "kafkaAssignerMode";
  static final String SKIP_INTER_BROKER_REPLICA_CONCURRENCY_ADJUSTMENT = "skipInterBrokerReplicaConcurrencyAdjustment";
  static final String PROPOSALS = "proposals";
  static final String TOPIC = "topic";
  static final String PARTITION = "partition";
  static final String PARTITION_SIZE = "partitionSize";
  static final String OLD_LEADER = "oldLeader";
  static final String OLD_REPLICAS = "oldReplicas";
  static final String NEW_REPLICAS = "newReplicas";
  static final String BROKER_ID = "brokerId";
  static final String LOGDIR = "logdir";
  static final String TASKS = "tasks";
  static final String TYPE = "type";
  static final String STATE = "state";
  static final String END_TIME_MS = "endTimeMs";
  private static final Gson GSON = new Gson();

  private PersistedExecutionStateSerde() {
  }

  /**
   * @param state Execution state to serialize.
   * @return JSON representation of the given execution state.
   */
  public static String toJson(PersistedExecutionState state) {
    JsonObject json = new JsonObject();
    json.addProperty(VERSION, state.version());
    json.addProperty(UUID, state.uuid());
    json.addProperty(REASON, state.reason());
    json.addProperty(OPERATION, state.operation().name());
    json.addProperty(TRIGGERED_BY_USER_REQUEST, state.triggeredByUserRequest());
    json.addProperty(START_TIME_MS, state.startTimeMs());
    json.addProperty(LAST_UPDATE_TIME_MS, state.lastUpdateTimeMs());
    json.addProperty(RESUME_COUNT, state.resumeCount());
    json.addProperty(EXECUTOR_STATE, state.executorState() == null ? null : state.executorState().name());

    JsonObject parameters = new JsonObject();
    parameters.add(REMOVED_BROKERS, toJsonArray(state.removedBrokers()));
    parameters.add(DEMOTED_BROKERS, toJsonArray(state.demotedBrokers()));
    parameters.add(BROKERS_TO_SKIP_CONCURRENCY_CHECK, toJsonArray(state.brokersToSkipConcurrencyCheck()));
    parameters.addProperty(INTER_BROKER_PARTITION_MOVEMENT_CONCURRENCY, state.interBrokerPartitionMovementConcurrency());
    parameters.addProperty(MAX_INTER_BROKER_PARTITION_MOVEMENTS, state.maxInterBrokerPartitionMovements());
    parameters.addProperty(INTRA_BROKER_PARTITION_MOVEMENT_CONCURRENCY, state.intraBrokerPartitionMovementConcurrency());
    parameters.addProperty(CLUSTER_LEADERSHIP_MOVEMENT_CONCURRENCY, state.clusterLeadershipMovementConcurrency());
    parameters.addProperty(BROKER_LEADERSHIP_MOVEMENT_CONCURRENCY, state.brokerLeadershipMovementConcurrency());
    parameters.addProperty(EXECUTION_PROGRESS_CHECK_INTERVAL_MS, state.executionProgressCheckIntervalMs());
    parameters.addProperty(REPLICA_MOVEMENT_STRATEGY, state.replicaMovementStrategy());
    parameters.addProperty(REPLICATION_THROTTLE, state.replicationThrottle());
    parameters.addProperty(KAFKA_ASSIGNER_MODE, state.kafkaAssignerMode());
    parameters.addProperty(SKIP_INTER_BROKER_REPLICA_CONCURRENCY_ADJUSTMENT, state.skipInterBrokerReplicaConcurrencyAdjustment());
    json.add(PARAMETERS, parameters);

    JsonArray proposals = new JsonArray();
    for (ExecutionProposal proposal : state.proposals().values()) {
      JsonObject p = new JsonObject();
      p.addProperty(TOPIC, proposal.topic());
      p.addProperty(PARTITION, proposal.partitionId());
      p.addProperty(PARTITION_SIZE, proposal.partitionSize());
      p.add(OLD_LEADER, replicaToJson(proposal.oldLeader()));
      p.add(OLD_REPLICAS, replicasToJson(proposal.oldReplicas()));
      p.add(NEW_REPLICAS, replicasToJson(proposal.newReplicas()));
      proposals.add(p);
    }
    json.add(PROPOSALS, proposals);

    JsonArray tasks = new JsonArray();
    for (PersistedTask task : state.tasks()) {
      JsonObject t = new JsonObject();
      t.addProperty(TYPE, task.type().name());
      t.addProperty(TOPIC, task.topicPartition().topic());
      t.addProperty(PARTITION, task.topicPartition().partition());
      if (task.brokerId() != PersistedTask.NO_BROKER_ID) {
        t.addProperty(BROKER_ID, task.brokerId());
      }
      t.addProperty(STATE, task.state().name());
      t.addProperty(START_TIME_MS, task.startTimeMs());
      t.addProperty(END_TIME_MS, task.endTimeMs());
      tasks.add(t);
    }
    json.add(TASKS, tasks);
    return GSON.toJson(json);
  }

  /**
   * @param json JSON representation of an execution state.
   * @return The deserialized execution state.
   * @throws IllegalArgumentException If the given JSON is not a valid execution state, or has an unsupported version.
   */
  public static PersistedExecutionState fromJson(String json) {
    try {
      JsonObject root = JsonParser.parseString(json).getAsJsonObject();
      int version = required(root, VERSION).getAsInt();
      if (version > PersistedExecutionState.CURRENT_VERSION) {
        throw new IllegalArgumentException(String.format("Unsupported execution state version %d (latest supported: %d).",
                                                         version, PersistedExecutionState.CURRENT_VERSION));
      }
      PersistedExecutionState.Builder builder =
          new PersistedExecutionState.Builder(required(root, UUID).getAsString(),
                                              PersistedExecutionState.Operation.valueOf(required(root, OPERATION).getAsString()))
              .version(version)
              .reason(optionalString(root, REASON))
              .triggeredByUserRequest(required(root, TRIGGERED_BY_USER_REQUEST).getAsBoolean())
              .startTimeMs(required(root, START_TIME_MS).getAsLong())
              .lastUpdateTimeMs(required(root, LAST_UPDATE_TIME_MS).getAsLong())
              .resumeCount(required(root, RESUME_COUNT).getAsInt());
      String executorState = optionalString(root, EXECUTOR_STATE);
      builder.executorState(executorState == null ? null : ExecutorState.State.valueOf(executorState));

      JsonObject parameters = required(root, PARAMETERS).getAsJsonObject();
      builder.removedBrokers(optionalIntegers(parameters, REMOVED_BROKERS))
             .demotedBrokers(optionalIntegers(parameters, DEMOTED_BROKERS))
             .brokersToSkipConcurrencyCheck(optionalIntegers(parameters, BROKERS_TO_SKIP_CONCURRENCY_CHECK))
             .interBrokerPartitionMovementConcurrency(optionalInteger(parameters, INTER_BROKER_PARTITION_MOVEMENT_CONCURRENCY))
             .maxInterBrokerPartitionMovements(optionalInteger(parameters, MAX_INTER_BROKER_PARTITION_MOVEMENTS))
             .intraBrokerPartitionMovementConcurrency(optionalInteger(parameters, INTRA_BROKER_PARTITION_MOVEMENT_CONCURRENCY))
             .clusterLeadershipMovementConcurrency(optionalInteger(parameters, CLUSTER_LEADERSHIP_MOVEMENT_CONCURRENCY))
             .brokerLeadershipMovementConcurrency(optionalInteger(parameters, BROKER_LEADERSHIP_MOVEMENT_CONCURRENCY))
             .executionProgressCheckIntervalMs(optionalLong(parameters, EXECUTION_PROGRESS_CHECK_INTERVAL_MS))
             .replicaMovementStrategy(optionalString(parameters, REPLICA_MOVEMENT_STRATEGY))
             .replicationThrottle(optionalLong(parameters, REPLICATION_THROTTLE))
             .kafkaAssignerMode(required(parameters, KAFKA_ASSIGNER_MODE).getAsBoolean())
             .skipInterBrokerReplicaConcurrencyAdjustment(required(parameters, SKIP_INTER_BROKER_REPLICA_CONCURRENCY_ADJUSTMENT).getAsBoolean());

      List<ExecutionProposal> proposals = new ArrayList<>();
      for (JsonElement element : required(root, PROPOSALS).getAsJsonArray()) {
        JsonObject p = element.getAsJsonObject();
        proposals.add(new ExecutionProposal(new TopicPartition(required(p, TOPIC).getAsString(), required(p, PARTITION).getAsInt()),
                                            required(p, PARTITION_SIZE).getAsLong(),
                                            replicaFromJson(required(p, OLD_LEADER).getAsJsonObject()),
                                            replicasFromJson(required(p, OLD_REPLICAS).getAsJsonArray()),
                                            replicasFromJson(required(p, NEW_REPLICAS).getAsJsonArray())));
      }
      builder.proposals(proposals);

      List<PersistedTask> tasks = new ArrayList<>();
      for (JsonElement element : required(root, TASKS).getAsJsonArray()) {
        JsonObject t = element.getAsJsonObject();
        Integer brokerId = optionalInteger(t, BROKER_ID);
        tasks.add(new PersistedTask(ExecutionTask.TaskType.valueOf(required(t, TYPE).getAsString()),
                                    new TopicPartition(required(t, TOPIC).getAsString(), required(t, PARTITION).getAsInt()),
                                    brokerId == null ? PersistedTask.NO_BROKER_ID : brokerId,
                                    ExecutionTaskState.valueOf(required(t, STATE).getAsString()),
                                    required(t, START_TIME_MS).getAsLong(),
                                    required(t, END_TIME_MS).getAsLong()));
      }
      builder.tasks(tasks);
      return builder.build();
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Failed to parse the persisted execution state.", e);
    }
  }

  private static JsonElement toJsonArray(Collection<Integer> values) {
    if (values == null) {
      return JsonNull.INSTANCE;
    }
    JsonArray array = new JsonArray();
    values.forEach(array::add);
    return array;
  }

  private static JsonObject replicaToJson(ReplicaPlacementInfo replica) {
    JsonObject json = new JsonObject();
    json.addProperty(BROKER_ID, replica.brokerId());
    if (replica.logdir() != null) {
      json.addProperty(LOGDIR, replica.logdir());
    }
    return json;
  }

  private static JsonArray replicasToJson(List<ReplicaPlacementInfo> replicas) {
    JsonArray array = new JsonArray();
    replicas.forEach(r -> array.add(replicaToJson(r)));
    return array;
  }

  private static ReplicaPlacementInfo replicaFromJson(JsonObject json) {
    int brokerId = required(json, BROKER_ID).getAsInt();
    String logdir = optionalString(json, LOGDIR);
    return logdir == null ? new ReplicaPlacementInfo(brokerId) : new ReplicaPlacementInfo(brokerId, logdir);
  }

  private static List<ReplicaPlacementInfo> replicasFromJson(JsonArray array) {
    List<ReplicaPlacementInfo> replicas = new ArrayList<>(array.size());
    array.forEach(e -> replicas.add(replicaFromJson(e.getAsJsonObject())));
    return replicas;
  }

  private static JsonElement required(JsonObject json, String name) {
    JsonElement element = json.get(name);
    if (element == null || element.isJsonNull()) {
      throw new IllegalArgumentException(String.format("Missing required field '%s' in the persisted execution state.", name));
    }
    return element;
  }

  private static JsonElement optional(JsonObject json, String name) {
    JsonElement element = json.get(name);
    return element == null || element.isJsonNull() ? null : element;
  }

  private static String optionalString(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    return element == null ? null : element.getAsString();
  }

  private static Integer optionalInteger(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    return element == null ? null : element.getAsInt();
  }

  private static Long optionalLong(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    return element == null ? null : element.getAsLong();
  }

  private static List<Integer> optionalIntegers(JsonObject json, String name) {
    JsonElement element = optional(json, name);
    if (element == null) {
      return null;
    }
    List<Integer> values = new ArrayList<>();
    element.getAsJsonArray().forEach(e -> values.add(e.getAsInt()));
    return values;
  }
}
