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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.driver.jdbc.FlightServerTestExtension;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.LoggerFactory;

/** Integration tests for {@link ArrowFlightSqlClientHandler} error suppression functionality. */
public class ArrowFlightSqlClientHandlerIntegrationTest {

  /** Custom producer that can simulate various error conditions during close operations. */
  public static class ErrorSimulatingFlightSqlProducer extends NoOpFlightSqlProducer {
    
    private final AtomicBoolean simulateUnavailableOnClose = new AtomicBoolean(false);
    private final AtomicBoolean simulateGoAwayOnClose = new AtomicBoolean(false);
    private final AtomicBoolean simulateNonBenignOnClose = new AtomicBoolean(false);

    public void setSimulateUnavailableOnClose(boolean simulate) {
      simulateUnavailableOnClose.set(simulate);
    }

    public void setSimulateGoAwayOnClose(boolean simulate) {
      simulateGoAwayOnClose.set(simulate);
    }

    public void setSimulateNonBenignOnClose(boolean simulate) {
      simulateNonBenignOnClose.set(simulate);
    }

    @Override
    public void closeSession(CloseSessionRequest request, CallContext context, StreamListener<CloseSessionResult> listener) {
      if (simulateUnavailableOnClose.get()) {
        listener.onError(CallStatus.UNAVAILABLE.withDescription("Service unavailable during shutdown").toRuntimeException());
        return;
      }

      if (simulateGoAwayOnClose.get()) {
        listener.onError(CallStatus.INTERNAL.withDescription("Connection closed after GOAWAY").toRuntimeException());
        return;
      }

      if (simulateNonBenignOnClose.get()) {
        listener.onError(CallStatus.UNAUTHENTICATED.withDescription("Authentication failed").toRuntimeException());
        return;
      }

      // Normal successful close - just complete successfully
      listener.onCompleted();
    }
  }

  private static final ErrorSimulatingFlightSqlProducer PRODUCER = new ErrorSimulatingFlightSqlProducer();

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(PRODUCER);

  private static BufferAllocator allocator;
  private Logger logger;
  private ListAppender<ILoggingEvent> logAppender;

  @BeforeAll
  public static void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    AutoCloseables.close(PRODUCER, allocator);
  }

  @BeforeEach
  public void setUp() {
    // Set up logging capture
    logger = (Logger) LoggerFactory.getLogger(ArrowFlightSqlClientHandler.class);
    logAppender = new ListAppender<>();
    logAppender.start();
    logger.addAppender(logAppender);
    logger.setLevel(Level.DEBUG);

    // Reset producer state
    PRODUCER.setSimulateUnavailableOnClose(false);
    PRODUCER.setSimulateGoAwayOnClose(false);
    PRODUCER.setSimulateNonBenignOnClose(false);
  }

  @AfterEach
  public void tearDownLogging() {
    if (logger != null && logAppender != null) {
      logger.detachAppender(logAppender);
    }
  }

  @Test
  public void testClose_WithCatalog_UnavailableError_SuppressesException() throws Exception {
    // Arrange
    PRODUCER.setSimulateUnavailableOnClose(true);
    
    try (ArrowFlightSqlClientHandler client = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
        .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
        .withBufferAllocator(allocator)
        .withEncryption(false)
        .withCatalog("test-catalog") // This triggers CloseSession RPC
        .build()) {

      // Act & Assert - close() should not throw despite server error
      assertDoesNotThrow(() -> client.close());
    }
    
    // Verify error was logged as suppressed
    assertTrue(logAppender.list.stream()
        .anyMatch(event -> event.getFormattedMessage().contains("closing Flight SQL session")));
  }

  @Test
  public void testClose_WithCatalog_GoAwayError_SuppressesException() throws Exception {
    // Arrange
    PRODUCER.setSimulateGoAwayOnClose(true);
    
    try (ArrowFlightSqlClientHandler client = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
        .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
        .withBufferAllocator(allocator)
        .withEncryption(false)
        .withCatalog("test-catalog")
        .build()) {
      
      // Act & Assert
      assertDoesNotThrow(() -> client.close());
    }
    
    // Verify error was logged as suppressed
    assertTrue(logAppender.list.stream()
        .anyMatch(event -> event.getFormattedMessage().contains("closing Flight SQL session")));
  }

  @Test
  public void testClose_WithCatalog_NonBenignError_ThrowsSQLException() throws Exception {
    // Arrange
    PRODUCER.setSimulateNonBenignOnClose(true);
    
    ArrowFlightSqlClientHandler client = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
        .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
        .withBufferAllocator(allocator)
        .withEncryption(false)
        .withCatalog("test-catalog")
        .build();
    
    // Act & Assert - non-benign errors should be thrown
    SQLException thrown = assertThrows(SQLException.class, () -> client.close());
    assertTrue(thrown.getMessage().contains("Failed to close Flight SQL session"));
    assertTrue(thrown.getCause() instanceof FlightRuntimeException);
  }

  @Test
  public void testClose_WithoutCatalog_NoCloseSessionCall() throws Exception {
    // Arrange - no catalog means no CloseSession RPC
    PRODUCER.setSimulateUnavailableOnClose(true); // This won't be triggered
    
    try (ArrowFlightSqlClientHandler client = new ArrowFlightSqlClientHandler.Builder()
        .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
        .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
        .withBufferAllocator(allocator)
        .withEncryption(false)
        // No catalog set
        .build()) {
      
      // Act & Assert - should close successfully without any CloseSession RPC
      assertDoesNotThrow(() -> client.close());
    }
    
    // Verify no CloseSession-related logging occurred
    assertTrue(logAppender.list.stream()
        .noneMatch(event -> event.getFormattedMessage().contains("closing Flight SQL session")));
  }
}
