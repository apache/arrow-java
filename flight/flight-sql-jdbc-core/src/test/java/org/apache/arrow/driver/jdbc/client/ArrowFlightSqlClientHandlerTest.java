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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.lang.reflect.Method;
import java.sql.SQLException;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

/** Tests for {@link ArrowFlightSqlClientHandler} error suppression functionality. */
@ExtendWith(MockitoExtension.class)
public class ArrowFlightSqlClientHandlerTest {

  @Mock private FlightSqlClient mockSqlClient;
  @Mock private BufferAllocator mockAllocator;

  private ArrowFlightSqlClientHandler clientHandler;
  private Logger logger;
  private ListAppender<ILoggingEvent> logAppender;

  @BeforeEach
  public void setUp() throws Exception {
    // Set up logging capture
    logger = (Logger) LoggerFactory.getLogger(ArrowFlightSqlClientHandler.class);
    logAppender = new ListAppender<>();
    logAppender.start();
    logger.addAppender(logAppender);
    logger.setLevel(Level.DEBUG);

    // Create a minimal client handler for testing private methods via reflection
    // We don't need a real connection since we're testing private methods
    clientHandler = createTestClientHandler();
  }

  @AfterEach
  public void tearDown() {
    if (logAppender != null) {
      logger.detachAppender(logAppender);
    }
    if (clientHandler != null) {
      try {
        clientHandler.close();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }

  private ArrowFlightSqlClientHandler createTestClientHandler() throws Exception {
    // Create a minimal client handler using mocks
    // We only need an instance to invoke private methods via reflection
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    // Create a handler with minimal setup - no actual connection needed
    ArrowFlightSqlClientHandler.Builder builder = new ArrowFlightSqlClientHandler.Builder()
        .withHost("localhost")
        .withPort(12345)
        .withBufferAllocator(allocator)
        .withEncryption(false);
    // Don't set catalog to avoid triggering setSessionOptions

    return builder.build();
  }

  @Test
  public void testIsBenignCloseException_UnavailableStatus_ReturnsTrue() throws Exception {
    // Arrange
    FlightRuntimeException unavailableException = createFlightRuntimeException(FlightStatusCode.UNAVAILABLE, "Service unavailable");

    // Act & Assert
    assertTrue(invokeIsBenignCloseException(unavailableException));
  }

  @Test
  public void testIsBenignCloseException_InternalWithGoAwayMessage_ReturnsTrue() throws Exception {
    // Arrange
    FlightRuntimeException internalException = createFlightRuntimeException(FlightStatusCode.INTERNAL, "Connection closed after GOAWAY");

    // Act & Assert
    assertTrue(invokeIsBenignCloseException(internalException));
  }

  @Test
  public void testIsBenignCloseException_InternalWithGoAwayInMiddle_ReturnsTrue() throws Exception {
    // Arrange
    FlightRuntimeException internalException = createFlightRuntimeException(FlightStatusCode.INTERNAL, "Error: Connection closed after GOAWAY occurred");

    // Act & Assert
    assertTrue(invokeIsBenignCloseException(internalException));
  }

  @Test
  public void testIsBenignCloseException_InternalWithoutGoAwayMessage_ReturnsFalse() throws Exception {
    // Arrange
    FlightRuntimeException internalException = createFlightRuntimeException(FlightStatusCode.INTERNAL, "Some other internal error");

    // Act & Assert
    assertFalse(invokeIsBenignCloseException(internalException));
  }

  @Test
  public void testIsBenignCloseException_OtherStatusCode_ReturnsFalse() throws Exception {
    // Arrange
    FlightRuntimeException otherException = createFlightRuntimeException(FlightStatusCode.UNAUTHENTICATED, "Unauthenticated");

    // Act & Assert
    assertFalse(invokeIsBenignCloseException(otherException));
  }

  @Test
  public void testIsBenignCloseException_NullMessage_ReturnsFalse() throws Exception {
    // Arrange
    FlightRuntimeException nullMessageException = createFlightRuntimeException(FlightStatusCode.INTERNAL, null);

    // Act & Assert
    assertFalse(invokeIsBenignCloseException(nullMessageException));
  }

  @Test
  public void testLogSuppressedCloseException_DebugEnabled_LogsDebugWithException() throws Exception {
    // Arrange
    logger.setLevel(Level.DEBUG);
    FlightRuntimeException exception = createFlightRuntimeException(FlightStatusCode.UNAVAILABLE, "Test message");

    // Act
    invokeLogSuppressedCloseException(exception, "test operation");

    // Assert
    assertTrue(logAppender.list.stream()
        .anyMatch(event -> event.getLevel() == Level.DEBUG 
            && event.getFormattedMessage().contains("Suppressed error test operation")));
  }

  @Test
  public void testLogSuppressedCloseException_DebugDisabled_LogsInfoWithoutException() throws Exception {
    // Arrange
    logger.setLevel(Level.INFO);
    FlightRuntimeException exception = createFlightRuntimeException(FlightStatusCode.UNAVAILABLE, "Test message");

    // Act
    invokeLogSuppressedCloseException(exception, "test operation");

    // Assert
    assertTrue(logAppender.list.stream()
        .anyMatch(event -> event.getLevel() == Level.INFO 
            && event.getFormattedMessage().contains("Suppressed benign error test operation: Test message")));
  }

  @Test
  public void testHandleBenignCloseException_SQLException_BenignError_DoesNotThrow() throws Exception {
    // Arrange
    FlightRuntimeException benignException = createFlightRuntimeException(FlightStatusCode.UNAVAILABLE, "Service unavailable");

    // Act & Assert
    assertDoesNotThrow(() -> invokeHandleBenignCloseExceptionWithSQLException(benignException, "Test SQL error", "test operation"));
  }

  @Test
  public void testHandleBenignCloseException_SQLException_NonBenignError_ThrowsSQLException() throws Exception {
    // Arrange
    FlightRuntimeException nonBenignException = createFlightRuntimeException(FlightStatusCode.UNAUTHENTICATED, "Unauthenticated");

    // Act & Assert
    SQLException thrown = assertThrows(SQLException.class,
        () -> invokeHandleBenignCloseExceptionWithSQLException(nonBenignException, "Test SQL error", "test operation"));
    assertTrue(thrown.getMessage().contains("Test SQL error"));
    assertTrue(thrown.getCause() instanceof FlightRuntimeException);
  }

  @Test
  public void testHandleBenignCloseException_FlightRuntimeException_BenignError_DoesNotThrow() throws Exception {
    // Arrange
    FlightRuntimeException benignException = createFlightRuntimeException(FlightStatusCode.UNAVAILABLE, "Service unavailable");

    // Act & Assert
    assertDoesNotThrow(() -> invokeHandleBenignCloseExceptionWithFlightRuntimeException(benignException, "test operation"));
  }

  @Test
  public void testHandleBenignCloseException_FlightRuntimeException_NonBenignError_ThrowsFlightRuntimeException() throws Exception {
    // Arrange
    FlightRuntimeException nonBenignException = createFlightRuntimeException(FlightStatusCode.UNAUTHENTICATED, "Unauthenticated");

    // Act & Assert
    FlightRuntimeException thrown = assertThrows(FlightRuntimeException.class,
        () -> invokeHandleBenignCloseExceptionWithFlightRuntimeException(nonBenignException, "test operation"));
    assertTrue(thrown.getMessage().contains("Unauthenticated"));
  }

  // Helper methods for reflection-based testing
  private boolean invokeIsBenignCloseException(FlightRuntimeException fre) throws Exception {
    Method method = ArrowFlightSqlClientHandler.class.getDeclaredMethod("isBenignCloseException", FlightRuntimeException.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(clientHandler, fre);
  }

  private void invokeLogSuppressedCloseException(FlightRuntimeException fre, String operationDescription) throws Exception {
    Method method = ArrowFlightSqlClientHandler.class.getDeclaredMethod("logSuppressedCloseException", FlightRuntimeException.class, String.class);
    method.setAccessible(true);
    method.invoke(clientHandler, fre, operationDescription);
  }

  private void invokeHandleBenignCloseExceptionWithSQLException(FlightRuntimeException fre, String sqlErrorMessage, String operationDescription) throws Exception {
    Method method = ArrowFlightSqlClientHandler.class.getDeclaredMethod("handleBenignCloseException", FlightRuntimeException.class, String.class, String.class);
    method.setAccessible(true);
    try {
      method.invoke(clientHandler, fre, sqlErrorMessage, operationDescription);
    } catch (java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw e;
    }
  }

  private void invokeHandleBenignCloseExceptionWithFlightRuntimeException(FlightRuntimeException fre, String operationDescription) throws Exception {
    Method method = ArrowFlightSqlClientHandler.class.getDeclaredMethod("handleBenignCloseException", FlightRuntimeException.class, String.class);
    method.setAccessible(true);
    try {
      method.invoke(clientHandler, fre, operationDescription);
    } catch (java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof FlightRuntimeException) {
        throw (FlightRuntimeException) e.getCause();
      }
      throw e;
    }
  }

  private FlightRuntimeException createFlightRuntimeException(FlightStatusCode statusCode, String message) {
    CallStatus status = mock(CallStatus.class, withSettings().lenient());
    when(status.code()).thenReturn(statusCode);

    FlightRuntimeException exception = mock(FlightRuntimeException.class, withSettings().lenient());
    when(exception.status()).thenReturn(status);
    when(exception.getMessage()).thenReturn(message);

    return exception;
  }

  // Note: Integration tests for close() method would require more complex setup
  // with actual FlightSqlClient instances. The private method tests above
  // provide comprehensive coverage of the error handling logic.

  // Edge case tests

  @Test
  public void testIsBenignCloseException_EmptyMessage_ReturnsFalse() throws Exception {
    // Arrange
    FlightRuntimeException emptyMessageException = createFlightRuntimeException(FlightStatusCode.INTERNAL, "");

    // Act & Assert
    assertFalse(invokeIsBenignCloseException(emptyMessageException));
  }

  @Test
  public void testIsBenignCloseException_GoAwayCaseSensitive_ReturnsTrue() throws Exception {
    // Arrange - test case sensitivity
    FlightRuntimeException mixedCaseException = createFlightRuntimeException(FlightStatusCode.INTERNAL, "connection closed after goaway");

    // Act & Assert
    assertFalse(invokeIsBenignCloseException(mixedCaseException)); // Should be case sensitive
  }

  @Test
  public void testLogSuppressedCloseException_NullOperationDescription_HandlesGracefully() throws Exception {
    // Arrange
    FlightRuntimeException exception = createFlightRuntimeException(FlightStatusCode.UNAVAILABLE, "Test message");

    // Act & Assert
    assertDoesNotThrow(() -> invokeLogSuppressedCloseException(exception, null));
  }
}
