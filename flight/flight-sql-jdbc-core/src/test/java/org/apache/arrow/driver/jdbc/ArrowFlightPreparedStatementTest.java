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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightPreparedStatementTest {

  public static final MockFlightSqlProducer PRODUCER = CoreMockedSqlProducers.getLegacyProducer();

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(PRODUCER);

  private static Connection connection;

  @BeforeAll
  public static void setup() throws SQLException {
    connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false);
  }

  @AfterAll
  public static void tearDown() throws SQLException {
    connection.close();
  }

  @BeforeEach
  public void before() {
    PRODUCER.clearActionTypeCounter();
  }

  @Test
  public void testSimpleQueryNoParameterBinding() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    try (final PreparedStatement preparedStatement = connection.prepareStatement(query);
        final ResultSet resultSet = preparedStatement.executeQuery()) {
      CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet);
    }
  }

  @Test
  public void testSimpleQueryNoParameterBindingWithExecute() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      boolean isResultSet = preparedStatement.execute();
      assertTrue(isResultSet);
      final ResultSet resultSet = preparedStatement.getResultSet();
      CoreMockedSqlProducers.assertLegacyRegularSqlResultSet(resultSet);
      assertFalse(preparedStatement.getMoreResults());
      assertEquals(-1, preparedStatement.getUpdateCount());
    }
  }

  @Test
  public void testQueryWithParameterBinding() throws SQLException {
    final String query = "Fake query with parameters";
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("", Types.MinorType.INT.getType())));
    final Schema parameterSchema =
        new Schema(
            Arrays.asList(
                Field.nullable("", ArrowType.Utf8.INSTANCE),
                new Field(
                    "",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(Field.nullable("", Types.MinorType.INT.getType())))));
    final List<List<Object>> expected =
        Collections.singletonList(Arrays.asList(new Text("foo"), new Integer[] {1, 2, null}));

    PRODUCER.addSelectQuery(
        query,
        schema,
        Collections.singletonList(
            listener -> {
              try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                ((IntVector) root.getVector(0)).setSafe(0, 10);
                root.setRowCount(1);
                listener.start(root);
                listener.putNext();
              } catch (final Throwable throwable) {
                listener.error(throwable);
              } finally {
                listener.completed();
              }
            }));

    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, "foo");
      preparedStatement.setArray(
          2, connection.createArrayOf("INTEGER", new Integer[] {1, 2, null}));

      try (final ResultSet resultSet = preparedStatement.executeQuery()) {
        resultSet.next();
        assert true;
      }
    }
  }

  @Test
  @Disabled("https://github.com/apache/arrow/issues/34741: flaky test")
  public void testPreparedStatementExecutionOnce() throws SQLException {
    final PreparedStatement statement =
        connection.prepareStatement(CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD);
    // Expect that there is one entry in the map -- {prepared statement action type, invocation
    // count}.
    assertEquals(PRODUCER.getActionTypeCounter().size(), 1);
    // Expect that the prepared statement was executed exactly once.
    assertEquals(
        PRODUCER
            .getActionTypeCounter()
            .get(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType()),
        1);
    statement.close();
  }

  @Test
  public void testReturnColumnCount() throws SQLException {
    final String query = CoreMockedSqlProducers.LEGACY_REGULAR_SQL_CMD;
    try (final PreparedStatement psmt = connection.prepareStatement(query)) {
      assertAll(
          "Column count is as expected",
          () -> assertThat("ID", equalTo(psmt.getMetaData().getColumnName(1))),
          () -> assertThat("Name", equalTo(psmt.getMetaData().getColumnName(2))),
          () -> assertThat("Age", equalTo(psmt.getMetaData().getColumnName(3))),
          () -> assertThat("Salary", equalTo(psmt.getMetaData().getColumnName(4))),
          () -> assertThat("Hire Date", equalTo(psmt.getMetaData().getColumnName(5))),
          () -> assertThat("Last Sale", equalTo(psmt.getMetaData().getColumnName(6))),
          () -> assertThat(6, equalTo(psmt.getMetaData().getColumnCount())));
    }
  }

  @Test
  public void testUpdateQuery() throws SQLException {
    String query = "Fake update";
    PRODUCER.addUpdateQuery(query, /*updatedRows*/ 42);
    try (final PreparedStatement stmt = connection.prepareStatement(query)) {
      int updated = stmt.executeUpdate();
      assertEquals(42, updated);
    }
  }

  @Test
  public void testUpdateQueryWithExecute() throws SQLException {
    String query = "Fake update with execute";
    PRODUCER.addUpdateQuery(query, /*updatedRows*/ 42);
    try (final PreparedStatement stmt = connection.prepareStatement(query)) {
      boolean isResultSet = stmt.execute();
      assertFalse(isResultSet);
      int updated = stmt.getUpdateCount();
      assertEquals(42, updated);
      assertFalse(stmt.getMoreResults());
      assertEquals(-1, stmt.getUpdateCount());
    }
  }

  @Test
  public void testUpdateQueryWithParameters() throws SQLException {
    String query = "Fake update with parameters";
    PRODUCER.addUpdateQuery(query, /*updatedRows*/ 42);
    PRODUCER.addExpectedParameters(
        query,
        new Schema(Collections.singletonList(Field.nullable("", ArrowType.Utf8.INSTANCE))),
        Collections.singletonList(
            Collections.singletonList(new Text("foo".getBytes(StandardCharsets.UTF_8)))));
    try (final PreparedStatement stmt = connection.prepareStatement(query)) {
      // TODO: make sure this is validated on the server too
      stmt.setString(1, "foo");
      int updated = stmt.executeUpdate();
      assertEquals(42, updated);
    }
  }

  @Test
  public void testUpdateQueryWithBatchedParameters() throws SQLException {
    String query = "Fake update with batched parameters";
    Schema parameterSchema =
        new Schema(
            Arrays.asList(
                Field.nullable("", ArrowType.Utf8.INSTANCE),
                new Field(
                    "",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(Field.nullable("", Types.MinorType.INT.getType())))));
    List<List<Object>> expected =
        Arrays.asList(
            Arrays.asList(new Text("foo"), new Integer[] {1, 2, null}),
            Arrays.asList(new Text("bar"), new Integer[] {0, -1, 100000}));

    PRODUCER.addUpdateQuery(query, /*updatedRows*/ 42);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (final PreparedStatement stmt = connection.prepareStatement(query)) {
      // TODO: make sure this is validated on the server too
      stmt.setString(1, "foo");
      stmt.setArray(2, connection.createArrayOf("INTEGER", new Integer[] {1, 2, null}));
      stmt.addBatch();
      stmt.setString(1, "bar");
      stmt.setArray(2, connection.createArrayOf("INTEGER", new Integer[] {0, -1, 100000}));
      stmt.addBatch();
      int[] updated = stmt.executeBatch();
      assertEquals(42, updated[0]);
    }
  }

  @Test
  public void testTimestampParameterWithMicrosecondPrecision() throws SQLException {
    String query = "Fake timestamp micro update";
    // Server schema declares parameter as TIMESTAMP(MICROSECOND, UTC)
    Schema parameterSchema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

    // TimeStampMicroTZVector.getObject() returns Long (raw epoch micros)
    // epochSeconds=1730637909, nanos=869885001 → micros = 1730637909 * 1_000_000 + 869885
    List<List<Object>> expected =
        Collections.singletonList(Collections.singletonList(1730637909869885L));

    PRODUCER.addUpdateQuery(query, 1);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      Timestamp ts = new Timestamp(1730637909869L);
      ts.setNanos(869885001); // .869885001 seconds — sub-ms precision
      stmt.setTimestamp(1, ts);
      int updated = stmt.executeUpdate();
      assertEquals(1, updated);
    }
  }

  @Test
  public void testTimestampParameterWithMillisecondPrecision() throws SQLException {
    String query = "Fake timestamp milli update";
    Schema parameterSchema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))));

    // TimeStampMilliTZVector.getObject() returns Long (raw epoch millis)
    // Sub-ms nanos are correctly truncated for MILLISECOND target
    List<List<Object>> expected =
        Collections.singletonList(Collections.singletonList(1730637909869L));

    PRODUCER.addUpdateQuery(query, 1);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      Timestamp ts = new Timestamp(1730637909869L);
      ts.setNanos(869885001);
      stmt.setTimestamp(1, ts);
      int updated = stmt.executeUpdate();
      assertEquals(1, updated);
    }
  }

  @Test
  public void testTimestampMutationAfterSetDoesNotAffectBoundValue() throws SQLException {
    String query = "Fake timestamp mutation test";
    Schema parameterSchema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

    // Original: epochSeconds=1730637909, nanos=869885001 → micros = 1730637909869885
    List<List<Object>> expected =
        Collections.singletonList(Collections.singletonList(1730637909869885L));

    PRODUCER.addUpdateQuery(query, 1);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      Timestamp ts = new Timestamp(1730637909869L);
      ts.setNanos(869885001);
      stmt.setTimestamp(1, ts);

      // Mutate the Timestamp after setTimestamp — should not affect the stored value
      ts.setNanos(0);
      ts.setTime(0);

      int updated = stmt.executeUpdate();
      assertEquals(1, updated);
    }
  }

  @Test
  public void testTimestampSetObjectFallsBackToMillis() throws SQLException {
    String query = "Fake timestamp setObject test";
    Schema parameterSchema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

    // setObject goes through Avatica's millis-only path: 1730637909869 * 1000 = 1730637909869000
    List<List<Object>> expected =
        Collections.singletonList(Collections.singletonList(1730637909869000L));

    PRODUCER.addUpdateQuery(query, 1);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      Timestamp ts = new Timestamp(1730637909869L);
      ts.setNanos(869885001); // sub-ms nanos present but will be lost via setObject
      stmt.setObject(1, ts);
      int updated = stmt.executeUpdate();
      assertEquals(1, updated);
    }
  }

  @Test
  public void testSetObjectAfterSetTimestampClearsRawTimestamp() throws SQLException {
    String query = "Fake timestamp setObject after setTimestamp";
    Schema parameterSchema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

    // After setObject replaces setTimestamp, millis-only path is used:
    // 1000L (epoch millis) * 1000 = 1000000 (epoch micros)
    List<List<Object>> expected = Collections.singletonList(Collections.singletonList(1000000L));

    PRODUCER.addUpdateQuery(query, 1);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      // First set with setTimestamp (populates rawTimestamps)
      Timestamp ts1 = new Timestamp(1730637909869L);
      ts1.setNanos(869885001);
      stmt.setTimestamp(1, ts1);

      // Then replace with setObject (should clear rawTimestamps for this index)
      Timestamp ts2 = new Timestamp(1000L);
      stmt.setObject(1, ts2);

      int updated = stmt.executeUpdate();
      assertEquals(1, updated);
    }
  }

  @Test
  public void testSetTimestampAfterSetObjectPreservesSubMillis() throws SQLException {
    String query = "Fake timestamp setTimestamp after setObject";
    Schema parameterSchema =
        new Schema(
            Collections.singletonList(
                Field.nullable("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));

    // setTimestamp called last, so rawTimestamp is used: epochSeconds=1730637909, nanos=869885001
    List<List<Object>> expected =
        Collections.singletonList(Collections.singletonList(1730637909869885L));

    PRODUCER.addUpdateQuery(query, 1);
    PRODUCER.addExpectedParameters(query, parameterSchema, expected);

    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      // First set with setObject (millis only)
      Timestamp ts1 = new Timestamp(1000L);
      stmt.setObject(1, ts1);

      // Then replace with setTimestamp (populates rawTimestamps with sub-ms precision)
      Timestamp ts2 = new Timestamp(1730637909869L);
      ts2.setNanos(869885001);
      stmt.setTimestamp(1, ts2);

      int updated = stmt.executeUpdate();
      assertEquals(1, updated);
    }
  }
}
