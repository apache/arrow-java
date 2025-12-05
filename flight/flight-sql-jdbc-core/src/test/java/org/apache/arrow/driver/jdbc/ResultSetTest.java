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
package org.apache.arrow.driver.jdbc;

import static java.lang.String.format;
import static java.util.Collections.synchronizedSet;
import static org.apache.arrow.flight.Location.forGrpcInsecure;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.driver.jdbc.utils.FallbackFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.PartitionedFlightSqlProducer;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.UuidUtility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ResultSetTest {
  private static final Random RANDOM = new Random(10);

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(
          CoreMockedSqlProducers.getLegacyProducer());

  private static Connection connection;

  @BeforeAll
  public static void setup() throws SQLException {
    connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false);
  }

  @AfterAll
  public static void tearDown() throws SQLException {
    connection.close();
  }

  private static void resultSetNextUntilDone(ResultSet resultSet) throws SQLException {
    while (resultSet.next()) {
      // TODO: implement resultSet.last()
      // Pass to the next until resultSet is done
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} can run a query successfully.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuery() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet);
    }
  }

  @Test
  public void testShouldExecuteQueryNotBlockIfClosedBeforeEnd() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      for (int i = 0; i < 7500; i++) {
        assertTrue(resultSet.next());
      }
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} query only returns only the amount of value set
   * by {@link org.apache.calcite.avatica.AvaticaStatement#setMaxRows(int)}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuerySettingMaxRowLimit() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      final int maxRowsLimit = 3;
      statement.setMaxRows(maxRowsLimit);

      assertThat(statement.getMaxRows(), is(maxRowsLimit));

      int count = 0;
      int columns = 6;
      for (; resultSet.next(); count++) {
        for (int column = 1; column <= columns; column++) {
          resultSet.getObject(column);
        }
        assertThat("Test Name #" + count, is(resultSet.getString(2)));
      }

      assertThat(maxRowsLimit, is(count));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} fails upon attempting to run an invalid query.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldThrowExceptionUponAttemptingToExecuteAnInvalidSelectQuery() {
    assertThrows(
        SQLException.class,
        () -> {
          try (Statement statement = connection.createStatement();
              ResultSet resultSet = statement.executeQuery("SELECT * FROM SHOULD-FAIL")) {
            fail();
          }
        });
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} query only returns only the amount of value set
   * by {@link org.apache.calcite.avatica.AvaticaStatement#setLargeMaxRows(long)} (int)}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldRunSelectQuerySettingLargeMaxRowLimit() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      final long maxRowsLimit = 3;
      statement.setLargeMaxRows(maxRowsLimit);

      assertThat(statement.getLargeMaxRows(), is(maxRowsLimit));

      int count = 0;
      int columns = resultSet.getMetaData().getColumnCount();
      for (; resultSet.next(); count++) {
        for (int column = 1; column <= columns; column++) {
          resultSet.getObject(column);
        }
        assertEquals("Test Name #" + count, resultSet.getString(2));
      }

      assertEquals(maxRowsLimit, count);
    }
  }

  @Test
  public void testColumnCountShouldRemainConsistentForResultSetThroughoutEntireDuration()
      throws SQLException {
    final Set<Integer> counts = new HashSet<>();
    try (final Statement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      while (resultSet.next()) {
        counts.add(resultSet.getMetaData().getColumnCount());
      }
    }
    assertThat(counts, is(ImmutableSet.of(6)));
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} close the statement after complete ResultSet
   * when call {@link org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldCloseStatementWhenIsCloseOnCompletion() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      statement.closeOnCompletion();

      resultSetNextUntilDone(resultSet);

      assertThat(statement.isClosed(), is(true));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} close the statement after complete ResultSet
   * with max rows limit when call {@link
   * org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldCloseStatementWhenIsCloseOnCompletionWithMaxRowsLimit() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      final long maxRowsLimit = 3;
      statement.setLargeMaxRows(maxRowsLimit);
      statement.closeOnCompletion();

      resultSetNextUntilDone(resultSet);

      assertThat(statement.isClosed(), is(true));
    }
  }

  /**
   * Tests whether the {@link ArrowFlightJdbcDriver} not close the statement after complete
   * ResultSet with max rows limit when call {@link
   * org.apache.calcite.avatica.AvaticaStatement#closeOnCompletion()}.
   *
   * @throws Exception If the connection fails to be established.
   */
  @Test
  public void testShouldNotCloseStatementWhenIsNotCloseOnCompletionWithMaxRowsLimit()
      throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {

      final long maxRowsLimit = 3;
      statement.setLargeMaxRows(maxRowsLimit);

      assertThat(statement.isClosed(), is(false));
      resultSetNextUntilDone(resultSet);
      assertThat(resultSet.isClosed(), is(false));
      assertThat(resultSet, is(instanceOf(ArrowFlightJdbcFlightStreamResultSet.class)));
    }
  }

  @Test
  public void testShouldCancelQueryUponCancelAfterQueryingResultSet() throws SQLException {
    try (final Statement statement = connection.createStatement();
        final ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      final int column = RANDOM.nextInt(resultSet.getMetaData().getColumnCount()) + 1;
      assertThat(resultSet.isClosed(), is(false));
      assertThat(resultSet.next(), is(true));
      assertDoesNotThrow(() -> resultSet.getObject(column));
      statement.cancel();
      // Should reset `ResultSet`; keep both `ResultSet` and `Connection` open.
      assertThat(statement.isClosed(), is(false));
      assertThat(resultSet.isClosed(), is(false));
      assertThat(resultSet.getMetaData().getColumnCount(), is(0));
    }
  }

  @Test
  public void testShouldInterruptFlightStreamsIfQueryIsCancelledMidQuerying()
      throws SQLException, InterruptedException {
    try (final Statement statement = connection.createStatement()) {
      final CountDownLatch latch = new CountDownLatch(1);
      final Set<Exception> exceptions = synchronizedSet(new HashSet<>(1));
      final Thread thread =
          new Thread(
              () -> {
                try (final ResultSet resultSet =
                    statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
                  final int cachedColumnCount = resultSet.getMetaData().getColumnCount();
                  Thread.sleep(300);
                  while (resultSet.next()) {
                    resultSet.getObject(RANDOM.nextInt(cachedColumnCount) + 1);
                  }
                } catch (final SQLException | InterruptedException e) {
                  exceptions.add(e);
                } finally {
                  latch.countDown();
                }
              });
      thread.setName("Test Case: interrupt query execution before first retrieval");
      thread.start();
      statement.cancel();
      thread.join();
      assertThat(
          exceptions.stream()
              .map(Exception::getMessage)
              .map(StringBuilder::new)
              .reduce(StringBuilder::append)
              .orElseThrow(IllegalArgumentException::new)
              .toString(),
          is("Statement canceled"));
    }
  }

  @Test
  public void
      testShouldInterruptFlightStreamsIfQueryIsCancelledMidProcessingForTimeConsumingQueries()
          throws SQLException, InterruptedException {
    final String query = CoreMockedSqlProducers.LEGACY_CANCELLATION_SQL_CMD;
    try (final Statement statement = connection.createStatement()) {
      final Set<Exception> exceptions = synchronizedSet(new HashSet<>(1));
      final Thread thread =
          new Thread(
              () -> {
                try (final ResultSet ignored = statement.executeQuery(query)) {
                  fail();
                } catch (final SQLException e) {
                  exceptions.add(e);
                }
              });
      thread.setName("Test Case: interrupt query execution mid-process");
      thread.setPriority(Thread.MAX_PRIORITY);
      thread.start();
      Thread.sleep(5000); // Let the other thread attempt to retrieve results.
      statement.cancel();
      thread.join();
      assertThat(
          exceptions.stream()
              .map(Exception::getMessage)
              .map(StringBuilder::new)
              .reduce(StringBuilder::append)
              .orElseThrow(IllegalStateException::new)
              .toString(),
          anyOf(
              is(format("Error while executing SQL \"%s\": Query canceled", query)),
              allOf(
                  containsString(format("Error while executing SQL \"%s\"", query)),
                  anyOf(containsString("CANCELLED"), containsString("Cancelling")))));
    }
  }

  @Test
  public void testShouldInterruptFlightStreamsIfQueryTimeoutIsOver() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_CANCELLATION_SQL_CMD;
    final int timeoutValue = 2;
    final String timeoutUnit = "SECONDS";
    try (final Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(timeoutValue);
      final Set<Exception> exceptions = new HashSet<>(1);
      try {
        statement.executeQuery(query);
      } catch (final Exception e) {
        exceptions.add(e);
      }
      final Throwable comparisonCause =
          exceptions.stream().findFirst().orElseThrow(RuntimeException::new).getCause().getCause();
      assertThat(comparisonCause, is(instanceOf(SQLTimeoutException.class)));
      assertThat(
          comparisonCause.getMessage(),
          is(format("Query timed out after %d %s", timeoutValue, timeoutUnit)));
    }
  }

  @Test
  public void testFlightStreamsQueryShouldNotTimeout() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    final int timeoutValue = 5;
    try (Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(timeoutValue);
      try (ResultSet resultSet = statement.executeQuery(query)) {
        CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet);
      }
    }
  }

  @Test
  public void testPartitionedFlightServer() throws Exception {
    // Arrange
    final Schema schema =
        new Schema(
            Arrays.asList(Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot firstPartition = VectorSchemaRoot.create(schema, allocator);
        VectorSchemaRoot secondPartition = VectorSchemaRoot.create(schema, allocator)) {
      firstPartition.setRowCount(1);
      ((IntVector) firstPartition.getVector(0)).set(0, 1);
      secondPartition.setRowCount(1);
      ((IntVector) secondPartition.getVector(0)).set(0, 2);

      // Construct the data-only nodes first.
      FlightProducer firstProducer =
          new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(
              new Ticket("first".getBytes(StandardCharsets.UTF_8)), firstPartition);
      FlightProducer secondProducer =
          new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(
              new Ticket("second".getBytes(StandardCharsets.UTF_8)), secondPartition);

      final FlightServer.Builder firstBuilder =
          FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), firstProducer);

      final FlightServer.Builder secondBuilder =
          FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), secondProducer);

      // Run the data-only nodes so that we can get the Locations they are running at.
      try (FlightServer firstServer = firstBuilder.build();
          FlightServer secondServer = secondBuilder.build()) {
        firstServer.start();
        secondServer.start();
        final FlightEndpoint firstEndpoint =
            new FlightEndpoint(
                new Ticket("first".getBytes(StandardCharsets.UTF_8)), firstServer.getLocation());

        final FlightEndpoint secondEndpoint =
            new FlightEndpoint(
                new Ticket("second".getBytes(StandardCharsets.UTF_8)), secondServer.getLocation());

        // Finally start the root node.
        try (final PartitionedFlightSqlProducer rootProducer =
                new PartitionedFlightSqlProducer(schema, firstEndpoint, secondEndpoint);
            FlightServer rootServer =
                FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                    .build()
                    .start();
            Connection newConnection =
                DriverManager.getConnection(
                    String.format(
                        "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                        rootServer.getLocation().getUri().getHost(), rootServer.getPort()));
            Statement newStatement = newConnection.createStatement();
            // Act
            ResultSet result = newStatement.executeQuery("Select partitioned_data")) {
          List<Integer> resultData = new ArrayList<>();
          while (result.next()) {
            resultData.add(result.getInt(1));
          }

          // Assert
          assertEquals(
              firstPartition.getRowCount() + secondPartition.getRowCount(), resultData.size());
          assertTrue(resultData.contains(((IntVector) firstPartition.getVector(0)).get(0)));
          assertTrue(resultData.contains(((IntVector) secondPartition.getVector(0)).get(0)));
        }
      }
    }
  }

  @Test
  public void testPartitionedFlightServerIgnoreFailure() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(
                Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      final FlightEndpoint firstEndpoint =
          new FlightEndpoint(
              new Ticket("first".getBytes(StandardCharsets.UTF_8)),
              Location.forGrpcInsecure("127.0.0.2", 1234),
              Location.forGrpcInsecure("127.0.0.3", 1234));

      try (final PartitionedFlightSqlProducer rootProducer =
              new PartitionedFlightSqlProducer(schema, firstEndpoint);
          FlightServer rootServer =
              FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                  .build()
                  .start();
          Connection newConnection =
              DriverManager.getConnection(
                  String.format(
                      "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                      rootServer.getLocation().getUri().getHost(), rootServer.getPort()));
          Statement newStatement = newConnection.createStatement()) {
        final SQLException e =
            assertThrows(
                SQLException.class,
                () -> {
                  ResultSet result = newStatement.executeQuery("Select partitioned_data");
                  while (result.next()) {}
                });
        final Throwable cause = e.getCause();
        assertTrue(cause instanceof FlightRuntimeException);
        final FlightRuntimeException fre = (FlightRuntimeException) cause;
        assertEquals(FlightStatusCode.UNAVAILABLE, fre.status().code());
      }
    }
  }

  @Test
  public void testPartitionedFlightServerAllFailure() throws Exception {
    // Arrange
    final Schema schema =
        new Schema(
            Collections.singletonList(
                Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot firstPartition = VectorSchemaRoot.create(schema, allocator)) {
      firstPartition.setRowCount(1);
      ((IntVector) firstPartition.getVector(0)).set(0, 1);

      // Construct the data-only nodes first.
      FlightProducer firstProducer =
          new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(
              new Ticket("first".getBytes(StandardCharsets.UTF_8)), firstPartition);

      final FlightServer.Builder firstBuilder =
          FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), firstProducer);

      // Run the data-only nodes so that we can get the Locations they are running at.
      try (FlightServer firstServer = firstBuilder.build()) {
        firstServer.start();
        final Location badLocation = Location.forGrpcInsecure("127.0.0.2", 1234);
        final FlightEndpoint firstEndpoint =
            new FlightEndpoint(
                new Ticket("first".getBytes(StandardCharsets.UTF_8)),
                badLocation,
                firstServer.getLocation());

        // Finally start the root node.
        try (final PartitionedFlightSqlProducer rootProducer =
                new PartitionedFlightSqlProducer(schema, firstEndpoint);
            FlightServer rootServer =
                FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                    .build()
                    .start();
            Connection newConnection =
                DriverManager.getConnection(
                    String.format(
                        "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                        rootServer.getLocation().getUri().getHost(), rootServer.getPort()));
            Statement newStatement = newConnection.createStatement();
            // Act
            ResultSet result = newStatement.executeQuery("Select partitioned_data")) {
          List<Integer> resultData = new ArrayList<>();
          while (result.next()) {
            resultData.add(result.getInt(1));
          }

          // Assert
          assertEquals(firstPartition.getRowCount(), resultData.size());
          assertTrue(resultData.contains(((IntVector) firstPartition.getVector(0)).get(0)));
        }
      }
    }
  }

  @Test
  public void testFallbackFlightServer() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("int_column", Types.MinorType.INT.getType())));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot resultData = VectorSchemaRoot.create(schema, allocator)) {
      resultData.setRowCount(1);
      ((IntVector) resultData.getVector(0)).set(0, 1);

      try (final FallbackFlightSqlProducer rootProducer =
              new FallbackFlightSqlProducer(resultData);
          FlightServer rootServer =
              FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                  .build()
                  .start();
          Connection newConnection =
              DriverManager.getConnection(
                  String.format(
                      "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                      rootServer.getLocation().getUri().getHost(), rootServer.getPort()));
          Statement newStatement = newConnection.createStatement();
          ResultSet result = newStatement.executeQuery("fallback")) {
        List<Integer> actualData = new ArrayList<>();
        while (result.next()) {
          actualData.add(result.getInt(1));
        }

        // Assert
        assertEquals(resultData.getRowCount(), actualData.size());
        assertTrue(actualData.contains(((IntVector) resultData.getVector(0)).get(0)));
      }
    }
  }

  @Test
  public void testFallbackSecondFlightServer() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("int_column", Types.MinorType.INT.getType())));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot resultData = VectorSchemaRoot.create(schema, allocator)) {
      resultData.setRowCount(1);
      ((IntVector) resultData.getVector(0)).set(0, 1);

      try (final FallbackFlightSqlProducer rootProducer =
              new FallbackFlightSqlProducer(resultData);
          FlightServer rootServer =
              FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                  .build()
                  .start();
          Connection newConnection =
              DriverManager.getConnection(
                  String.format(
                      "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                      rootServer.getLocation().getUri().getHost(), rootServer.getPort()));
          Statement newStatement = newConnection.createStatement();
          ResultSet result = newStatement.executeQuery("fallback with error")) {
        List<Integer> actualData = new ArrayList<>();
        while (result.next()) {
          actualData.add(result.getInt(1));
        }

        // Assert
        assertEquals(resultData.getRowCount(), actualData.size());
        assertTrue(actualData.contains(((IntVector) resultData.getVector(0)).get(0)));
      }
    }
  }

  @Test
  public void testFallbackUnresolvableFlightServer() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("int_column", Types.MinorType.INT.getType())));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot resultData = VectorSchemaRoot.create(schema, allocator)) {
      resultData.setRowCount(1);
      ((IntVector) resultData.getVector(0)).set(0, 1);

      try (final FallbackFlightSqlProducer rootProducer =
              new FallbackFlightSqlProducer(resultData);
          FlightServer rootServer =
              FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                  .build()
                  .start();
          Connection newConnection =
              DriverManager.getConnection(
                  String.format(
                      "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false",
                      rootServer.getLocation().getUri().getHost(), rootServer.getPort()))) {
        // This first attempt should take a measurable amount of time.
        long start = System.nanoTime();
        try (Statement newStatement = newConnection.createStatement()) {
          try (ResultSet result = newStatement.executeQuery("fallback with unresolvable")) {
            List<Integer> actualData = new ArrayList<>();
            while (result.next()) {
              actualData.add(result.getInt(1));
            }

            // Assert
            assertEquals(resultData.getRowCount(), actualData.size());
            assertTrue(actualData.contains(((IntVector) resultData.getVector(0)).get(0)));
          }
        }
        long attempt1 = System.nanoTime();
        double elapsedMs = (attempt1 - start) / 1_000_000.;
        assertTrue(
            elapsedMs >= 5000.,
            String.format(
                "Expected first attempt to hit the timeout, but only %f ms elapsed", elapsedMs));

        // Once the client cache is implemented (GH-661), this second attempt should take less time,
        // since the failure from before should be cached.
        start = System.nanoTime();
        try (Statement newStatement = newConnection.createStatement()) {
          try (ResultSet result = newStatement.executeQuery("fallback with unresolvable")) {
            List<Integer> actualData = new ArrayList<>();
            while (result.next()) {
              actualData.add(result.getInt(1));
            }

            // Assert
            assertEquals(resultData.getRowCount(), actualData.size());
            assertTrue(actualData.contains(((IntVector) resultData.getVector(0)).get(0)));
          }
        }
        attempt1 = System.nanoTime();
        elapsedMs = (attempt1 - start) / 1_000_000.;
        assertTrue(
            elapsedMs < 5000.,
            String.format("Expected second attempt to be faster, but %f ms elapsed", elapsedMs));
      }
    }
  }

  @Test
  public void testFallbackUnresolvableFlightServerDisableCache() throws Exception {
    final Schema schema =
        new Schema(
            Collections.singletonList(Field.nullable("int_column", Types.MinorType.INT.getType())));
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot resultData = VectorSchemaRoot.create(schema, allocator)) {
      resultData.setRowCount(1);
      ((IntVector) resultData.getVector(0)).set(0, 1);

      try (final FallbackFlightSqlProducer rootProducer =
              new FallbackFlightSqlProducer(resultData);
          FlightServer rootServer =
              FlightServer.builder(allocator, forGrpcInsecure("localhost", 0), rootProducer)
                  .build()
                  .start();
          Connection newConnection =
              DriverManager.getConnection(
                  String.format(
                      "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false&useClientCache=false",
                      rootServer.getLocation().getUri().getHost(), rootServer.getPort()))) {
        // This first attempt should take a measurable amount of time.
        long start = System.nanoTime();
        try (Statement newStatement = newConnection.createStatement()) {
          try (ResultSet result = newStatement.executeQuery("fallback with unresolvable")) {
            List<Integer> actualData = new ArrayList<>();
            while (result.next()) {
              actualData.add(result.getInt(1));
            }

            // Assert
            assertEquals(resultData.getRowCount(), actualData.size());
            assertTrue(actualData.contains(((IntVector) resultData.getVector(0)).get(0)));
          }
        }
        long attempt1 = System.nanoTime();
        double elapsedMs = (attempt1 - start) / 1_000_000.;
        assertTrue(
            elapsedMs >= 5000.,
            String.format(
                "Expected first attempt to hit the timeout, but only %f ms elapsed", elapsedMs));

        // This second attempt should take a long time still, since we disabled the cache.
        start = System.nanoTime();
        try (Statement newStatement = newConnection.createStatement()) {
          try (ResultSet result = newStatement.executeQuery("fallback with unresolvable")) {
            List<Integer> actualData = new ArrayList<>();
            while (result.next()) {
              actualData.add(result.getInt(1));
            }

            // Assert
            assertEquals(resultData.getRowCount(), actualData.size());
            assertTrue(actualData.contains(((IntVector) resultData.getVector(0)).get(0)));
          }
        }
        attempt1 = System.nanoTime();
        elapsedMs = (attempt1 - start) / 1_000_000.;
        assertTrue(
            elapsedMs >= 5000.,
            String.format(
                "Expected second attempt to hit the timeout, but only %f ms elapsed", elapsedMs));
      }
    }
  }

  @Test
  public void testShouldRunSelectQueryWithEmptyVectorsEmbedded() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_WITH_EMPTY_SQL_CMD)) {
      long rowCount = 0;
      while (resultSet.next()) {
        ++rowCount;
      }
      assertEquals(2, rowCount);
    }
  }

  @Test
  public void testResultSetAppMetadata() throws Exception {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD)) {
      assertArrayEquals(
          ((ArrowFlightJdbcFlightStreamResultSet) resultSet).getAppMetadata(),
          "foo".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testSelectQueryWithUuidColumn() throws SQLException {
    // Expectations
    final int expectedRowCount = 4;
    final UUID[] expectedUuids = new UUID[] {
        CoreMockedSqlProducers.UUID_1,
        CoreMockedSqlProducers.UUID_2,
        CoreMockedSqlProducers.UUID_3,
        null
    };

    final Integer[] expectedIds = new Integer[] {1, 2, 3, 4};

    final List<UUID> actualUuids = new ArrayList<>(expectedRowCount);
    final List<Integer> actualIds = new ArrayList<>(expectedRowCount);


    // Query
    int actualRowCount = 0;
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      for (; resultSet.next(); actualRowCount++) {
        actualIds.add((Integer) resultSet.getObject("id"));
        actualUuids.add((UUID) resultSet.getObject("uuid_col"));
      }
    }

    // Assertions
    int finalActualRowCount = actualRowCount;
    assertAll(
        "UUID ResultSet values are as expected",
        () -> assertThat(finalActualRowCount, is(equalTo(expectedRowCount))),
        () -> assertThat(actualIds.toArray(new Integer[0]), is(expectedIds)),
        () -> assertThat(actualUuids.toArray(new UUID[0]), is(expectedUuids)));
  }

  @Test
  public void testGetObjectReturnsUuid() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      resultSet.next();
      Object result = resultSet.getObject("uuid_col");
      assertThat(result, instanceOf(UUID.class));
      assertThat(result, is(CoreMockedSqlProducers.UUID_1));
    }
  }

  @Test
  public void testGetObjectByIndexReturnsUuid() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      resultSet.next();
      Object result = resultSet.getObject(2);
      assertThat(result, instanceOf(UUID.class));
      assertThat(result, is(CoreMockedSqlProducers.UUID_1));
    }
  }

  @Test
  public void testGetStringReturnsHyphenatedFormat() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      resultSet.next();
      String result = resultSet.getString("uuid_col");
      assertThat(result, is(CoreMockedSqlProducers.UUID_1.toString()));
    }
  }

  @Test
  public void testGetBytesReturns16ByteArray() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      resultSet.next();
      byte[] result = resultSet.getBytes("uuid_col");
      assertThat(result.length, is(16));
      assertThat(result, is(UuidUtility.getBytesFromUUID(CoreMockedSqlProducers.UUID_1)));
    }
  }

  @Test
  public void testNullUuidHandling() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      // Skip to row 4 which has NULL UUID
      resultSet.next(); // row 1
      resultSet.next(); // row 2
      resultSet.next(); // row 3
      resultSet.next(); // row 4 (NULL UUID)

      Object objResult = resultSet.getObject("uuid_col");
      assertThat(objResult, nullValue());
      assertThat(resultSet.wasNull(), is(true));

      String strResult = resultSet.getString("uuid_col");
      assertThat(strResult, nullValue());
      assertThat(resultSet.wasNull(), is(true));

      byte[] bytesResult = resultSet.getBytes("uuid_col");
      assertThat(bytesResult, nullValue());
      assertThat(resultSet.wasNull(), is(true));
    }
  }

  @Test
  public void testMultipleUuidRows() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      resultSet.next();
      assertThat(resultSet.getObject("uuid_col"), is(CoreMockedSqlProducers.UUID_1));

      resultSet.next();
      assertThat(resultSet.getObject("uuid_col"), is(CoreMockedSqlProducers.UUID_2));

      resultSet.next();
      assertThat(resultSet.getObject("uuid_col"), is(CoreMockedSqlProducers.UUID_3));

      resultSet.next();
      assertThat(resultSet.getObject("uuid_col"), nullValue());
    }
  }

  @Test
  public void testUuidExtensionTypeInSchema() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(CoreMockedSqlProducers.UUID_SQL_CMD)) {
      ResultSetMetaData metaData = resultSet.getMetaData();

      assertThat(metaData.getColumnCount(), is(2));
      assertThat(metaData.getColumnName(1), is("id"));
      assertThat(metaData.getColumnName(2), is("uuid_col"));

      assertThat(metaData.getColumnType(2), is(java.sql.Types.OTHER));
    }
  }

  @Test
  public void testPreparedStatementWithUuidParameter() throws SQLException {
    try (PreparedStatement pstmt =
        connection.prepareStatement(CoreMockedSqlProducers.UUID_PREPARED_SELECT_SQL_CMD)) {
      pstmt.setObject(1, CoreMockedSqlProducers.UUID_1);
      try (ResultSet rs = pstmt.executeQuery()) {
        rs.next();
        assertThat(rs.getObject("uuid_col"), is(CoreMockedSqlProducers.UUID_1));
      }
    }
  }

  @Test
  public void testPreparedStatementWithUuidStringParameter() throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(CoreMockedSqlProducers.UUID_PREPARED_SELECT_SQL_CMD)) {
      pstmt.setString(1, CoreMockedSqlProducers.UUID_1.toString());
      try (ResultSet rs = pstmt.executeQuery()) {
        rs.next();
        assertThat(rs.getObject("uuid_col"), is(CoreMockedSqlProducers.UUID_1));
      }
    }
  }

  @Test
  public void testPreparedStatementUpdateWithUuid() throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(CoreMockedSqlProducers.UUID_PREPARED_UPDATE_SQL_CMD)) {
      pstmt.setObject(1, CoreMockedSqlProducers.UUID_3);
      pstmt.setInt(2, 1);
      int updated = pstmt.executeUpdate();
      assertThat(updated, is(1));
    }
  }
}
