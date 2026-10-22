/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.CreateTopicRunnable;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.CreateTopicParameters;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.CREATE_TOPIC_PARAMETER_OBJECT_CONFIG;
import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;


public class CreateTopicRequest extends AbstractAsyncRequest {
  protected CreateTopicParameters _parameters;

  public CreateTopicRequest() {
    super();
  }

  @Override
  protected OperationFuture handle(String uuid) {
    OperationFuture future = new OperationFuture("Create Topic");
    pending(future.operationProgress());
    _asyncKafkaCruiseControl.sessionExecutor().execute(new CreateTopicRunnable(_asyncKafkaCruiseControl, future, uuid, _parameters));
    return future;
  }

  @Override
  public CreateTopicParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return CreateTopicRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _parameters = (CreateTopicParameters) validateNotNull(configs.get(CREATE_TOPIC_PARAMETER_OBJECT_CONFIG),
            "Parameter configuration is missing from the request.");
  }
}
