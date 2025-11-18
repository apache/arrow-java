/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightSqlClientHandlerTest {

  @ParameterizedTest
  @MethodSource
  public void testCloseHandlesFlightRuntimeException(
      boolean throwFromCloseSession, CallStatus callStatus, boolean shouldSuppress)
      throws Exception {
    FlightSqlClient sqlClient = mock(FlightSqlClient.class);
    String cacheKey = "cacheKey";
    Optional<String> catalog =
        throwFromCloseSession ? Optional.of("test_catalog") : Optional.empty();
    final Collection<CallOption> credentialOptions = new ArrayList<>();
    ArrowFlightSqlClientHandler.Builder builder = new ArrowFlightSqlClientHandler.Builder();

    if (throwFromCloseSession) {
      doThrow(callStatus.toRuntimeException())
          .when(sqlClient)
          .closeSession(any(CloseSessionRequest.class), any(CallOption[].class));
    } else {
      doThrow(callStatus.toRuntimeException()).when(sqlClient).close();
    }

    ArrowFlightSqlClientHandler sqlClientHandler =
        new ArrowFlightSqlClientHandler(
            cacheKey, sqlClient, builder, credentialOptions, catalog, null);

    if (shouldSuppress) {
      assertDoesNotThrow(sqlClientHandler::close);
    } else {
      assertThrows(SQLException.class, sqlClientHandler::close);
    }
  }

  private static Object[] testCloseHandlesFlightRuntimeException() {
    CallStatus benignInternalError =
        new CallStatus(FlightStatusCode.INTERNAL, null, "Connection closed after GOAWAY", null);
    CallStatus notBenignInternalError =
        new CallStatus(FlightStatusCode.INTERNAL, null, "Not a benign internal error", null);
    CallStatus unavailableError = new CallStatus(FlightStatusCode.UNAVAILABLE, null, null, null);
    CallStatus unknownError = new CallStatus(FlightStatusCode.UNKNOWN, null, null, null);
    return new Object[] {
      new Object[] {true, benignInternalError, true},
      new Object[] {false, benignInternalError, true},
      new Object[] {true, notBenignInternalError, false},
      new Object[] {false, notBenignInternalError, false},
      new Object[] {true, unavailableError, true},
      new Object[] {false, unavailableError, true},
      new Object[] {true, unknownError, false},
      new Object[] {false, unknownError, false},
    };
  }
}
