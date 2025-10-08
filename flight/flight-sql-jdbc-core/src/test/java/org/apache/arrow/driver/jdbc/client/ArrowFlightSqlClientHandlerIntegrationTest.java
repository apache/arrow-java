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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.arrow.driver.jdbc.FlightServerTestExtension;
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

  /** Minimal producer for integration tests. */
  public static class TestFlightSqlProducer extends NoOpFlightSqlProducer {
    // No custom behavior needed for these tests
  }

  private static final TestFlightSqlProducer PRODUCER = new TestFlightSqlProducer();

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
  }

  @AfterEach
  public void tearDownLogging() {
    if (logger != null && logAppender != null) {
      logger.detachAppender(logAppender);
    }
  }

  // Note: Integration tests for closeSession() with catalog are not included because
  // closeSession is a gRPC service method that's not routed through the FlightProducer,
  // making it difficult to simulate errors in a test environment. The unit tests
  // (ArrowFlightSqlClientHandlerTest) provide comprehensive coverage of the error
  // suppression logic using reflection to test the private methods directly.

  @Test
  public void testClose_WithoutCatalog_NoCloseSessionCall() throws Exception {
    // Arrange - no catalog means no CloseSession RPC
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
