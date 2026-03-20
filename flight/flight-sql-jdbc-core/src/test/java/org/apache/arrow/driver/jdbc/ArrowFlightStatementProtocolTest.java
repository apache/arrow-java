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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.function.Consumer;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightStatementProtocolTest {
  private static final String SELECT_QUERY = "SELECT * FROM PROTOCOL_SELECT";
  private static final String UPDATE_QUERY = "UPDATE PROTOCOL_UPDATE";
  private static final Schema QUERY_SCHEMA =
      new Schema(Collections.singletonList(Field.nullable("id", MinorType.INT.getType())));

  private static final MockFlightSqlProducer PRODUCER = new MockFlightSqlProducer();

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(PRODUCER);

  private Connection connection;

  @BeforeAll
  public static void setUpBeforeClass() {
    PRODUCER.addSelectQuery(
        SELECT_QUERY,
        QUERY_SCHEMA,
        Collections.singletonList(
            listener -> {
              try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  final VectorSchemaRoot root = VectorSchemaRoot.create(QUERY_SCHEMA, allocator)) {
                IntVector vector = (IntVector) root.getVector("id");
                vector.setSafe(0, 1);
                root.setRowCount(1);
                listener.start(root);
                listener.putNext();
              } catch (final Throwable throwable) {
                listener.error(throwable);
              } finally {
                listener.completed();
              }
            }));
    PRODUCER.addUpdateQuery(UPDATE_QUERY, 1);

    final Message commandGetDbSchemas = CommandGetDbSchemas.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetSchemasResultProducer =
        listener -> {
          try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
              final VectorSchemaRoot root =
                  VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
            final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
            final VarCharVector schemaName = (VarCharVector) root.getVector("db_schema_name");
            catalogName.setSafe(0, new Text("catalog_name #0"));
            schemaName.setSafe(0, new Text("db_schema_name #0"));
            root.setRowCount(1);
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        };
    PRODUCER.addCatalogQuery(commandGetDbSchemas, commandGetSchemasResultProducer);
  }

  @BeforeEach
  public void setUp() throws SQLException {
    PRODUCER.clearActionTypeCounter();
    PRODUCER.clearCommandTypeCounter();
    connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection(false);
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(connection);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    AutoCloseables.close(PRODUCER);
  }

  @Test
  public void testStatementExecuteQueryUsesStatementProtocol() throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(SELECT_QUERY)) {
      assertTrue(resultSet.next());
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(0));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_QUERY, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_QUERY, 0),
        is(0));
  }

  @Test
  public void testStatementExecuteUsesPreparedProtocolForQuery() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertThat(statement.execute(SELECT_QUERY), is(true));
      try (ResultSet resultSet = statement.getResultSet()) {
        assertTrue(resultSet.next());
      }
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_QUERY, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_QUERY, 0),
        is(0));
  }

  @Test
  public void testStatementExecuteUpdateUsesStatementProtocol() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertThat(statement.executeUpdate(UPDATE_QUERY), is(1));
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(0));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_UPDATE, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_UPDATE, 0),
        is(0));
  }

  @Test
  public void testStatementExecuteUsesPreparedProtocolForUpdate() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertThat(statement.execute(UPDATE_QUERY), is(false));
      assertThat(statement.getUpdateCount(), is(1));
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_UPDATE, 0),
        is(1));
  }

  @Test
  public void testStatementExecuteThenExecuteUpdateUsesStatementProtocol() throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertThat(statement.execute(SELECT_QUERY), is(true));
      try (ResultSet resultSet = statement.getResultSet()) {
        assertTrue(resultSet.next());
      }
      assertThat(statement.executeUpdate(UPDATE_QUERY), is(1));
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_QUERY, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_UPDATE, 0),
        is(1));
  }

  @Test
  public void testStatementExecuteUpdateThenExecuteQueryUsesStatementProtocol()
      throws SQLException {
    try (Statement statement = connection.createStatement()) {
      assertThat(statement.execute(UPDATE_QUERY), is(false));
      assertThat(statement.getUpdateCount(), is(1));
      try (ResultSet resultSet = statement.executeQuery(SELECT_QUERY)) {
        assertTrue(resultSet.next());
      }
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_UPDATE, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_QUERY, 0),
        is(1));
  }

  @Test
  public void testPreparedStatementExecuteQueryUsesPreparedProtocol() throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SELECT_QUERY);
        ResultSet resultSet = statement.executeQuery()) {
      assertTrue(resultSet.next());
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_QUERY, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_QUERY, 0),
        is(0));
  }

  @Test
  public void testPreparedStatementExecuteUsesPreparedProtocolForQuery() throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SELECT_QUERY)) {
      assertThat(statement.execute(), is(true));
      try (ResultSet resultSet = statement.getResultSet()) {
        assertTrue(resultSet.next());
      }
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_QUERY, 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_STATEMENT_QUERY, 0),
        is(0));
  }

  @Test
  public void testPreparedStatementExecuteUpdateUsesPreparedProtocol() throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(UPDATE_QUERY)) {
      assertThat(statement.executeUpdate(), is(1));
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_UPDATE, 0),
        is(1));
  }

  @Test
  public void testPreparedStatementExecuteUsesPreparedProtocolForUpdate() throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(UPDATE_QUERY)) {
      assertThat(statement.execute(), is(false));
      assertThat(statement.getUpdateCount(), is(1));
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(1));
    assertThat(
        PRODUCER
            .getCommandTypeCounter()
            .getOrDefault(MockFlightSqlProducer.COMMAND_PREPARED_STATEMENT_UPDATE, 0),
        is(1));
  }

  @Test
  public void testMetadataGetSchemasUsesJdbcApi() throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet resultSet = metaData.getSchemas()) {
      assertTrue(resultSet.next());
    }

    assertThat(
        PRODUCER
            .getActionTypeCounter()
            .getOrDefault(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(), 0),
        is(0));
  }
}
