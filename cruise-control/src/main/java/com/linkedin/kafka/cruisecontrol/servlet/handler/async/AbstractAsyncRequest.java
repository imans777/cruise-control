/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.async;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlEndPoints;
import com.linkedin.kafka.cruisecontrol.async.AsyncKafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.Pending;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.config.constants.WebServerConfig;
import com.linkedin.kafka.cruisecontrol.servlet.UserRequestException;
import com.linkedin.kafka.cruisecontrol.servlet.UserTaskManager;
import com.linkedin.kafka.cruisecontrol.servlet.handler.AbstractRequest;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.cruisecontrol.servlet.response.CruiseControlResponse;
import com.linkedin.kafka.cruisecontrol.servlet.handler.async.runnable.OperationFuture;
import com.linkedin.kafka.cruisecontrol.servlet.response.ProgressResult;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractAsyncRequest extends AbstractRequest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncRequest.class);
  protected AsyncKafkaCruiseControl _asyncKafkaCruiseControl;
  private ThreadLocal<Integer> _asyncOperationStep;
  private UserTaskManager _userTaskManager;
  private long _maxBlockMs;

  public AbstractAsyncRequest() {

  }

  /**
   * Handle the request with the given uuid and return the corresponding {@link OperationFuture}.
   *
   * @param uuid UUID string associated with the request.
   * @return The corresponding {@link OperationFuture}.
   */
  protected abstract OperationFuture handle(String uuid);

  @Override
  public CruiseControlResponse getResponse(CruiseControlRequestContext requestContext)
          throws Exception {
    LOG.info("Processing async request {}.", name());
    int step = _asyncOperationStep.get();
    List<OperationFuture>
            futures = _userTaskManager.getOrCreateUserTask(requestContext, this::handle, step, mapUserTaskToSession(), parameters());
    _asyncOperationStep.set(step + 1);
    CruiseControlResponse ccResponse;
    try {
      ccResponse = futures.get(step).get(_maxBlockMs, TimeUnit.MILLISECONDS);
      LOG.info("Computation is completed for async request: {}.", requestContext.getPathInfo());
    } catch (TimeoutException te) {
      ccResponse = new ProgressResult(futures, _asyncKafkaCruiseControl.config());
      LOG.info("Computation is in progress for async request: {}.", requestContext.getPathInfo());
    } catch (ExecutionException ee) {
      // Surface invalid user requests detected by the async operation as such (i.e. HTTP 400) rather than as a server error.
      if (ee.getCause() instanceof UserRequestException) {
        throw (UserRequestException) ee.getCause();
      }
      throw ee;
    }
    return ccResponse;
  }

  /**
   * Whether the user task created for this request can also be retrieved via the HTTP session of the request -- i.e. a subsequent
   * request with the same URL and query parameters within the same session (e.g. cookie) gets the same user task, even without
   * a User-Task-ID header. Requests whose payload is (partially) in the request body must return {@code false}, because the
   * session lookup does not take the body into account.
   *
   * @return {@code true} to map the created user task to the HTTP session of the request, {@code false} otherwise.
   */
  protected boolean mapUserTaskToSession() {
    return true;
  }

  @Override
  public abstract CruiseControlParameters parameters();

  public abstract String name();

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    KafkaCruiseControlEndPoints cruiseControlEndPoints = getCruiseControlEndpoints();
    _asyncKafkaCruiseControl = cruiseControlEndPoints.asyncKafkaCruiseControl();
    _asyncOperationStep = cruiseControlEndPoints.asyncOperationStep();
    _userTaskManager = cruiseControlEndPoints.userTaskManager();
    _maxBlockMs = cruiseControlEndPoints.config().getLong(WebServerConfig.WEBSERVER_REQUEST_MAX_BLOCK_TIME_MS_CONFIG);
  }

  protected KafkaCruiseControlEndPoints getCruiseControlEndpoints() {
    return _requestHandler.cruiseControlEndPoints();
  }

  protected void pending(OperationProgress progress) {
    progress.addStep(new Pending());
  }
}
