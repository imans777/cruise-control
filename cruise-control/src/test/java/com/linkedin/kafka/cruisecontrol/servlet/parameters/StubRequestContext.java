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

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.http.CruiseControlRequestContext;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;


/**
 * A stub of a request context to parse request parameters with: It serves the given query parameters, request body and user task id,
 * and captures the error responses written upon failures to parse the parameters.
 */
final class StubRequestContext {
  static final KafkaCruiseControlConfig CONFIG = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
  private final CruiseControlRequestContext _requestContext;
  private final Capture<Integer> _errorCodes;
  private final Capture<String> _errorMessages;

  /**
   * @param method HTTP method of the request.
   * @param endPoint Path of the request -- i.e. the endpoint.
   * @param queryParameters Query parameters of the request.
   * @param body JSON body of the request, or {@code null} if the request has no body.
   * @param userTaskId The User-Task-ID header of the request, or {@code null} if the request has no such header.
   */
  @SuppressWarnings("unchecked")
  StubRequestContext(String method, String endPoint, Map<String, String> queryParameters, String body, String userTaskId) {
    Map<String, String[]> parameterMap = new HashMap<>();
    queryParameters.forEach((name, value) -> parameterMap.put(name, new String[]{value}));
    _requestContext = EasyMock.createNiceMock(CruiseControlRequestContext.class);
    _errorCodes = EasyMock.newCapture(CaptureType.ALL);
    _errorMessages = EasyMock.newCapture(CaptureType.ALL);
    EasyMock.expect(_requestContext.getMethod()).andStubReturn(method);
    EasyMock.expect(_requestContext.getPathInfo()).andStubReturn("/" + endPoint);
    EasyMock.expect(_requestContext.getParameterMap()).andStubReturn(parameterMap);
    EasyMock.expect(_requestContext.getParameter(EasyMock.anyString())).andStubAnswer(() -> {
      String[] values = parameterMap.get((String) EasyMock.getCurrentArguments()[0]);
      return values == null ? null : values[0];
    });
    EasyMock.expect(_requestContext.getUserTaskIdString()).andStubReturn(userTaskId);
    try {
      EasyMock.expect(_requestContext.getJson()).andStubReturn(body == null ? null : new Gson().fromJson(body, Map.class));
      _requestContext.writeResponseToOutputStream(EasyMock.captureInt(_errorCodes), EasyMock.anyBoolean(), EasyMock.anyBoolean(),
                                                  EasyMock.capture(_errorMessages));
    } catch (IOException ioe) {
      throw new IllegalStateException(ioe);
    }
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(_requestContext);
  }

  /**
   * Parse the given parameters from this request context.
   *
   * @param parameters Parameters to parse.
   * @return {@code true} if the parameters could not be parsed -- i.e. an error response has been written, {@code false} otherwise.
   */
  boolean parse(AbstractParameters parameters) {
    parameters._requestContext = _requestContext;
    parameters._config = CONFIG;
    return parameters.parseParameters(_requestContext);
  }

  List<Integer> errorCodes() {
    return _errorCodes.getValues();
  }

  List<String> errorMessages() {
    return _errorMessages.getValues();
  }
}
