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

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.ReassignPartitionsRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.ReassignPartitionsParameters;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.REASSIGN_PARTITIONS_PARAMETER_OBJECT_CONFIG;


/**
 * The async request to execute a manual partition reassignment -- see
 * {@link com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint#REASSIGN_PARTITIONS}.
 */
public class ReassignPartitionsRequest extends AbstractAsyncRequest {
  protected ReassignPartitionsParameters _parameters;

  public ReassignPartitionsRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Reassign partitions");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new ReassignPartitionsRunnable(_asyncKafkaCruiseControl, future, uuid, _parameters));
    return future;
  }

  /**
   * The reassignment is given in the request body, which the HTTP session lookup of user tasks does not take into account.
   * Hence, requests to this endpoint are tracked only via their User-Task-ID header.
   *
   * @return {@code false}.
   */
  @Override
  protected boolean mapUserTaskToSession() {
    return false;
  }

  @Override
  public ReassignPartitionsParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return ReassignPartitionsRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (ReassignPartitionsParameters) validateNotNull(configs.get(REASSIGN_PARTITIONS_PARAMETER_OBJECT_CONFIG),
                                                                 "Parameter configuration is missing from the request.");
  }
}
