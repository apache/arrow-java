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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightSqlClientHandlerTest {

  @Test
  public void testGetStreamsRejectsUnencryptedEndpointOnEncryptedConnection() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      final FlightSqlClient sqlClient = mock(FlightSqlClient.class);
      try (ArrowFlightSqlClientHandler handler = encryptedHandler(sqlClient, allocator)) {
        // A server-supplied location that asks the driver to drop back to plaintext.
        final FlightInfo flightInfo =
            flightInfoWithLocations(Location.forGrpcInsecure("other.example.com", 443));

        // A connection attempt would fail with a different error, so the message shows that the
        // location was refused before connecting and sending the credentials.
        final SQLException ex =
            assertThrows(SQLException.class, () -> handler.getStreams(flightInfo));
        assertTrue(ex.getMessage().contains("without encryption"), ex.getMessage());
        // The stream is not fetched over the original connection instead.
        verifyNoInteractions(sqlClient);
      }
    }
  }

  @Test
  public void testGetStreamsFallsBackToNextLocationAfterUnencryptedEndpoint() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      final FlightSqlClient sqlClient = mock(FlightSqlClient.class);
      final FlightStream stream = mock(FlightStream.class);
      when(sqlClient.getStream(any(Ticket.class), any(CallOption[].class))).thenReturn(stream);
      try (ArrowFlightSqlClientHandler handler = encryptedHandler(sqlClient, allocator)) {
        final FlightInfo flightInfo =
            flightInfoWithLocations(
                Location.forGrpcInsecure("other.example.com", 443), Location.reuseConnection());

        final List<CloseableEndpointStreamPair> streams = handler.getStreams(flightInfo);
        assertEquals(1, streams.size());
        assertSame(stream, streams.get(0).getStream());
        AutoCloseables.close(streams);
      }
    }
  }

  private static ArrowFlightSqlClientHandler encryptedHandler(
      final FlightSqlClient sqlClient, final BufferAllocator allocator) {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("localhost")
            .withPort(443)
            .withEncryption(true)
            .withUsername("user")
            .withPassword("password")
            .withBufferAllocator(allocator);
    return new ArrowFlightSqlClientHandler(
        "cacheKey", sqlClient, builder, new ArrayList<>(), Optional.empty(), null);
  }

  private static FlightInfo flightInfoWithLocations(final Location... locations) {
    return new FlightInfo(
        new Schema(Collections.emptyList()),
        FlightDescriptor.command(new byte[0]),
        Collections.singletonList(new FlightEndpoint(new Ticket(new byte[0]), locations)),
        -1,
        -1);
  }

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
    verify(sqlClient).close();
  }

  @Test
  public void testCloseSessionWithoutCatalogIsBestEffortWithTimeout() throws Exception {
    FlightSqlClient sqlClient = mock(FlightSqlClient.class);
    doThrow(CallStatus.UNKNOWN.toRuntimeException())
        .when(sqlClient)
        .closeSession(any(CloseSessionRequest.class), any(CallOption[].class));
    ArrowFlightSqlClientHandler sqlClientHandler =
        new ArrowFlightSqlClientHandler(
            "cacheKey",
            sqlClient,
            new ArrowFlightSqlClientHandler.Builder(),
            new ArrayList<>(),
            Optional.empty(),
            null);

    assertDoesNotThrow(sqlClientHandler::close);
    verify(sqlClient)
        .closeSession(
            any(CloseSessionRequest.class), eq(ArrowFlightSqlClientHandler.CLOSE_SESSION_TIMEOUT));
    verify(sqlClient).close();
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
