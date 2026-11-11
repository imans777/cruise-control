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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;


/**
 * A single partition entry of a manual partition reassignment request (see {@link ReassignPartitionsParameters}).
 * Each entry uses exactly one of the following forms:
 * <ul>
 *   <li>{@link Form#REPLICAS}: The format of kafka-reassign-partitions.sh -- an ordered replica list (the first replica is the
 *   leader after execution), optionally with one log directory per replica. A log directory of {@link #ANY_LOG_DIR} keeps the
 *   current log directory of an existing replica, or lets the broker pick one for an added replica.</li>
 *   <li>{@link Form#LEADER}: Keep the replica set, and make the given broker -- an existing replica -- the leader.</li>
 *   <li>{@link Form#LOG_DIRS}: Keep the replica list, and move the replicas on the given brokers to the given log directories.</li>
 * </ul>
 */
public final class RequestedPartitionReassignment {
  public static final String ANY_LOG_DIR = "any";
  private final TopicPartition _topicPartition;
  private final Form _form;
  private final List<Integer> _replicas;
  private final List<String> _logDirs;
  private final Integer _leader;
  private final Map<Integer, String> _logDirByBroker;

  private RequestedPartitionReassignment(TopicPartition topicPartition,
                                         Form form,
                                         List<Integer> replicas,
                                         List<String> logDirs,
                                         Integer leader,
                                         Map<Integer, String> logDirByBroker) {
    _topicPartition = topicPartition;
    _form = form;
    _replicas = replicas;
    _logDirs = logDirs;
    _leader = leader;
    _logDirByBroker = logDirByBroker;
  }

  /**
   * @param topicPartition Topic partition to reassign.
   * @param replicas The ordered new replica list.
   * @param logDirs The log directory for each replica in the new replica list, or {@code null} to keep / let brokers pick them.
   * @return A requested reassignment in {@link Form#REPLICAS} form.
   */
  public static RequestedPartitionReassignment withReplicas(TopicPartition topicPartition, List<Integer> replicas, List<String> logDirs) {
    return new RequestedPartitionReassignment(topicPartition, Form.REPLICAS, List.copyOf(replicas),
                                              logDirs == null ? null : List.copyOf(logDirs), null, null);
  }

  /**
   * @param topicPartition Topic partition to change the leader of.
   * @param leader The broker to become the leader.
   * @return A requested reassignment in {@link Form#LEADER} form.
   */
  public static RequestedPartitionReassignment withLeader(TopicPartition topicPartition, int leader) {
    return new RequestedPartitionReassignment(topicPartition, Form.LEADER, null, null, leader, null);
  }

  /**
   * @param topicPartition Topic partition whose replicas are moved between log directories.
   * @param logDirByBroker The destination log directory by the broker hosting the replica to move.
   * @return A requested reassignment in {@link Form#LOG_DIRS} form.
   */
  public static RequestedPartitionReassignment withLogDirs(TopicPartition topicPartition, Map<Integer, String> logDirByBroker) {
    return new RequestedPartitionReassignment(topicPartition, Form.LOG_DIRS, null, null, null,
                                              Collections.unmodifiableMap(new LinkedHashMap<>(logDirByBroker)));
  }

  public TopicPartition topicPartition() {
    return _topicPartition;
  }

  public Form form() {
    return _form;
  }

  /**
   * @return The ordered new replica list in {@link Form#REPLICAS} form, {@code null} otherwise.
   */
  public List<Integer> replicas() {
    return _replicas;
  }

  /**
   * @return The log directory per replica in {@link Form#REPLICAS} form if specified, {@code null} otherwise.
   */
  public List<String> logDirs() {
    return _logDirs;
  }

  /**
   * @return The requested leader in {@link Form#LEADER} form, {@code null} otherwise.
   */
  public Integer leader() {
    return _leader;
  }

  /**
   * @return The destination log directory by broker in {@link Form#LOG_DIRS} form, {@code null} otherwise.
   */
  public Map<Integer, String> logDirByBroker() {
    return _logDirByBroker;
  }

  @Override
  public String toString() {
    switch (_form) {
      case LEADER:
        return String.format("{%s, leader: %d}", _topicPartition, _leader);
      case LOG_DIRS:
        return String.format("{%s, log_dirs: %s}", _topicPartition, _logDirByBroker);
      case REPLICAS:
        return String.format("{%s, replicas: %s%s}", _topicPartition, _replicas, _logDirs == null ? "" : ", log_dirs: " + _logDirs);
      default:
        throw new IllegalStateException("Unrecognized form " + _form);
    }
  }

  /**
   * The form of a requested partition reassignment.
   */
  public enum Form {
    REPLICAS, LEADER, LOG_DIRS
  }
}
