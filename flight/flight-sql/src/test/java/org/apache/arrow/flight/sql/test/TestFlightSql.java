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
package org.apache.arrow.flight.sql.test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.arrow.flight.sql.util.FlightStreamUtils.getResults;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.RenewFlightEndpointRequest;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableExistsOption;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions.TableNotExistOption;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.HamcrestCondition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Test direct usage of Flight SQL workflows. */
public class TestFlightSql {

  protected static final Schema SCHEMA_INT_TABLE =
      new Schema(
          asList(
              new Field("ID", new FieldType(false, MinorType.INT.getType(), null), null),
              Field.nullable("KEYNAME", MinorType.VARCHAR.getType()),
              Field.nullable("VALUE", MinorType.INT.getType()),
              Field.nullable("FOREIGNID", MinorType.INT.getType())));
  private static final List<List<String>> EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY =
      ImmutableList.of(
          asList("1", "one", "1", "1"),
          asList("2", "zero", "0", "1"),
          asList("3", "negative one", "-1", "1"));
  protected static final List<List<String>> EXPECTED_RESULTS_FOR_PARAMETER_BINDING =
      ImmutableList.of(asList("1", "one", "1", "1"));
  private static final Map<String, String> GET_SQL_INFO_EXPECTED_RESULTS_MAP =
      new LinkedHashMap<>();
  protected static final String LOCALHOST = "localhost";
  protected static BufferAllocator allocator;
  protected static FlightServer server;
  protected static FlightSqlClient sqlClient;

  private static void populateNext10RowsInIngestRootBatch(
      int startRowNumber,
      IntVector valueVector,
      VarCharVector keyNameVector,
      IntVector foreignIdVector,
      VarCharVector keyNamesToBeDeletedVector,
      VectorSchemaRoot ingestRoot) {

    final int NumRowsInBatch = 10;

    valueVector.reset();
    keyNameVector.reset();
    foreignIdVector.reset();

    final IntStream range = IntStream.range(1, NumRowsInBatch);

    range.forEach(
        i -> {
          valueVector.setSafe(i - 1, (i + startRowNumber - 1) * NumRowsInBatch);
          keyNameVector.setSafe(i - 1, new Text("value" + (i + startRowNumber - 1)));
          foreignIdVector.setSafe(i - 1, 1);
        });
    // put some comma and double-quote containing string as well
    valueVector.setSafe(NumRowsInBatch - 1, (NumRowsInBatch + startRowNumber - 1) * NumRowsInBatch);
    keyNameVector.setSafe(
        NumRowsInBatch - 1,
        new Text(
            String.format(
                "value%d, is \"%d\"",
                (NumRowsInBatch + startRowNumber - 1),
                (NumRowsInBatch + startRowNumber - 1) * NumRowsInBatch)));
    foreignIdVector.setSafe(NumRowsInBatch - 1, 1);
    ingestRoot.setRowCount(NumRowsInBatch);

    VectorBatchAppender.batchAppend(keyNamesToBeDeletedVector, keyNameVector);
  }

  @BeforeAll
  public static void setUp() throws Exception {
    setUpClientServer();
    setUpExpectedResultsMap();
  }

  private static void setUpClientServer() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    server =
        FlightServer.builder(
                allocator,
                serverLocation,
                new FlightSqlExample(serverLocation, FlightSqlExample.DB_NAME))
            .build()
            .start();

    final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, server.getPort());
    sqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
  }

  protected static void setUpExpectedResultsMap() {
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE), "Apache Derby");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE),
        "10.15.2.0 - (1873585)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE),
        "10.15.2.0 - (1873585)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE), "false");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_ALL_TABLES_ARE_SELECTABLE_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_NULL_ORDERING_VALUE),
        Integer.toString(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END_VALUE));
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_DDL_CATALOG_VALUE), "false");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_DDL_SCHEMA_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_DDL_TABLE_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_IDENTIFIER_CASE_VALUE),
        Integer.toString(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE_VALUE));
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR_VALUE), "\"");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE_VALUE),
        Integer.toString(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE_VALUE));
    GET_SQL_INFO_EXPECTED_RESULTS_MAP.put(
        Integer.toString(FlightSql.SqlInfo.SQL_MAX_COLUMNS_IN_TABLE_VALUE), "42");
  }

  @AfterAll
  public static void tearDown() throws Exception {
    close(sqlClient, server, allocator);
    FlightSqlExample.removeDerbyDatabaseIfExists(FlightSqlExample.DB_NAME);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(
      final List<? extends List<String>> results) {
    return getNonConformingResultsForGetSqlInfo(
        results,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY,
        FlightSql.SqlInfo.SQL_ALL_TABLES_ARE_SELECTABLE,
        FlightSql.SqlInfo.SQL_NULL_ORDERING,
        FlightSql.SqlInfo.SQL_DDL_CATALOG,
        FlightSql.SqlInfo.SQL_DDL_SCHEMA,
        FlightSql.SqlInfo.SQL_DDL_TABLE,
        FlightSql.SqlInfo.SQL_IDENTIFIER_CASE,
        FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
        FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE,
        FlightSql.SqlInfo.SQL_MAX_COLUMNS_IN_TABLE);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(
      final List<? extends List<String>> results, final FlightSql.SqlInfo... args) {
    final List<List<String>> nonConformingResults = new ArrayList<>();
    if (results.size() == args.length) {
      for (int index = 0; index < results.size(); index++) {
        final List<String> result = results.get(index);
        final String providedName = result.get(0);
        final String expectedName = Integer.toString(args[index].getNumber());
        System.err.println(expectedName);
        if (!(GET_SQL_INFO_EXPECTED_RESULTS_MAP.get(providedName).equals(result.get(1))
            && providedName.equals(expectedName))) {
          nonConformingResults.add(result);
          break;
        }
      }
    }
    return nonConformingResults;
  }

  @Test
  public void testGetTablesSchema() {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, true);
    Assertions.assertThat(info.getSchemaOptional())
        .isEqualTo(Optional.of(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA));
  }

  @Test
  public void testGetTablesSchemaExcludeSchema() {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, false);
    Assertions.assertThat(info.getSchemaOptional())
        .isEqualTo(Optional.of(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA));
  }

  @Test
  public void testGetTablesResultNoSchema() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(
            sqlClient.getTables(null, null, null, null, false).getEndpoints().get(0).getTicket())) {
      assertAll(
          () -> {
            Assertions.assertThat(stream.getSchema())
                .isEqualTo(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA);
          },
          () -> {
            final List<List<String>> results = getResults(stream);
            final List<List<String>> expectedResults =
                ImmutableList.of(
                    // catalog_name | schema_name | table_name | table_type | table_schema
                    asList(null /* TODO No catalog yet */, "SYS", "SYSALIASES", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSCHECKS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSCOLPERMS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSCOLUMNS", "SYSTEM TABLE"),
                    asList(
                        null /* TODO No catalog yet */, "SYS", "SYSCONGLOMERATES", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSCONSTRAINTS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSDEPENDS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSFILES", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSFOREIGNKEYS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSKEYS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSPERMS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSROLES", "SYSTEM TABLE"),
                    asList(
                        null /* TODO No catalog yet */, "SYS", "SYSROUTINEPERMS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSSCHEMAS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSSEQUENCES", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSSTATEMENTS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSSTATISTICS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSTABLEPERMS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSTABLES", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSTRIGGERS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSUSERS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYS", "SYSVIEWS", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "SYSIBM", "SYSDUMMY1", "SYSTEM TABLE"),
                    asList(null /* TODO No catalog yet */, "APP", "FOREIGNTABLE", "TABLE"),
                    asList(null /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
            Assertions.assertThat(results).isEqualTo(expectedResults);
          });
    }
  }

  @Test
  public void testGetTablesResultFilteredNoSchema() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(
            sqlClient
                .getTables(null, null, null, singletonList("TABLE"), false)
                .getEndpoints()
                .get(0)
                .getTicket())) {

      assertAll(
          () ->
              Assertions.assertThat(stream.getSchema())
                  .isEqualTo(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA),
          () -> {
            final List<List<String>> results = getResults(stream);
            final List<List<String>> expectedResults =
                ImmutableList.of(
                    // catalog_name | schema_name | table_name | table_type | table_schema
                    asList(null /* TODO No catalog yet */, "APP", "FOREIGNTABLE", "TABLE"),
                    asList(null /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
            Assertions.assertThat(results).isEqualTo(expectedResults);
          });
    }
  }

  @Test
  public void testGetTablesResultFilteredWithSchema() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(
            sqlClient
                .getTables(null, null, null, singletonList("TABLE"), true)
                .getEndpoints()
                .get(0)
                .getTicket())) {
      assertAll(
          () ->
              Assertions.assertThat(stream.getSchema())
                  .isEqualTo(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA),
          () -> {
            Assertions.assertThat(stream.getSchema())
                .isEqualTo(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA);
            final List<List<String>> results = getResults(stream);
            final List<List<String>> expectedResults =
                ImmutableList.of(
                    // catalog_name | schema_name | table_name | table_type | table_schema
                    asList(
                        null /* TODO No catalog yet */,
                        "APP",
                        "FOREIGNTABLE",
                        "TABLE",
                        new Schema(
                                asList(
                                    new Field(
                                        "ID",
                                        new FieldType(
                                            false,
                                            MinorType.INT.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("INTEGER")
                                                .schemaName("APP")
                                                .tableName("FOREIGNTABLE")
                                                .precision(10)
                                                .scale(0)
                                                .isAutoIncrement(true)
                                                .build()
                                                .getMetadataMap()),
                                        null),
                                    new Field(
                                        "FOREIGNNAME",
                                        new FieldType(
                                            true,
                                            MinorType.VARCHAR.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("VARCHAR")
                                                .schemaName("APP")
                                                .tableName("FOREIGNTABLE")
                                                .precision(100)
                                                .scale(0)
                                                .isAutoIncrement(false)
                                                .build()
                                                .getMetadataMap()),
                                        null),
                                    new Field(
                                        "VALUE",
                                        new FieldType(
                                            true,
                                            MinorType.INT.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("INTEGER")
                                                .schemaName("APP")
                                                .tableName("FOREIGNTABLE")
                                                .precision(10)
                                                .scale(0)
                                                .isAutoIncrement(false)
                                                .build()
                                                .getMetadataMap()),
                                        null)))
                            .toJson()),
                    asList(
                        null /* TODO No catalog yet */,
                        "APP",
                        "INTTABLE",
                        "TABLE",
                        new Schema(
                                asList(
                                    new Field(
                                        "ID",
                                        new FieldType(
                                            false,
                                            MinorType.INT.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("INTEGER")
                                                .schemaName("APP")
                                                .tableName("INTTABLE")
                                                .precision(10)
                                                .scale(0)
                                                .isAutoIncrement(true)
                                                .build()
                                                .getMetadataMap()),
                                        null),
                                    new Field(
                                        "KEYNAME",
                                        new FieldType(
                                            true,
                                            MinorType.VARCHAR.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("VARCHAR")
                                                .schemaName("APP")
                                                .tableName("INTTABLE")
                                                .precision(100)
                                                .scale(0)
                                                .isAutoIncrement(false)
                                                .build()
                                                .getMetadataMap()),
                                        null),
                                    new Field(
                                        "VALUE",
                                        new FieldType(
                                            true,
                                            MinorType.INT.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("INTEGER")
                                                .schemaName("APP")
                                                .tableName("INTTABLE")
                                                .precision(10)
                                                .scale(0)
                                                .isAutoIncrement(false)
                                                .build()
                                                .getMetadataMap()),
                                        null),
                                    new Field(
                                        "FOREIGNID",
                                        new FieldType(
                                            true,
                                            MinorType.INT.getType(),
                                            null,
                                            new FlightSqlColumnMetadata.Builder()
                                                .catalogName("")
                                                .typeName("INTEGER")
                                                .schemaName("APP")
                                                .tableName("INTTABLE")
                                                .precision(10)
                                                .scale(0)
                                                .isAutoIncrement(false)
                                                .build()
                                                .getMetadataMap()),
                                        null)))
                            .toJson()));
            Assertions.assertThat(results).isEqualTo(expectedResults);
          });
    }
  }

  @Test
  public void testSimplePreparedStatementSchema() throws Exception {
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable")) {
      assertAll(
          () -> {
            final Schema actualSchema = preparedStatement.getResultSetSchema();
            Assertions.assertThat(actualSchema).isEqualTo(SCHEMA_INT_TABLE);
          },
          () -> {
            final FlightInfo info = preparedStatement.execute();
            Assertions.assertThat(info.getSchemaOptional())
                .isEqualTo(Optional.of(SCHEMA_INT_TABLE));
          });
    }
  }

  @Test
  public void testSimplePreparedStatementResults() throws Exception {
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable");
        final FlightStream stream =
            sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
      assertAll(
          () -> Assertions.assertThat(stream.getSchema()).isEqualTo(SCHEMA_INT_TABLE),
          () ->
              Assertions.assertThat(getResults(stream))
                  .isEqualTo(EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY));
    }
  }

  @Test
  public void testSimplePreparedStatementResultsWithParameterBinding() throws Exception {
    try (PreparedStatement prepare = sqlClient.prepare("SELECT * FROM intTable WHERE id = ?")) {
      final Schema parameterSchema = prepare.getParameterSchema();
      try (final VectorSchemaRoot insertRoot =
          VectorSchemaRoot.create(parameterSchema, allocator)) {
        insertRoot.allocateNew();

        final IntVector valueVector = (IntVector) insertRoot.getVector(0);
        valueVector.setSafe(0, 1);
        insertRoot.setRowCount(1);

        prepare.setParameters(insertRoot);
        FlightInfo flightInfo = prepare.execute();

        FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

        assertAll(
            () -> Assertions.assertThat(stream.getSchema()).isEqualTo(SCHEMA_INT_TABLE),
            () ->
                Assertions.assertThat(getResults(stream))
                    .isEqualTo(EXPECTED_RESULTS_FOR_PARAMETER_BINDING));
      }
    }
  }

  @Test
  public void testSimplePreparedStatementUpdateResults() throws SQLException {
    try (PreparedStatement prepare =
            sqlClient.prepare("INSERT INTO INTTABLE (keyName, value ) VALUES (?, ?)");
        PreparedStatement deletePrepare =
            sqlClient.prepare("DELETE FROM INTTABLE WHERE keyName = ?")) {
      final Schema parameterSchema = prepare.getParameterSchema();
      try (final VectorSchemaRoot insertRoot =
          VectorSchemaRoot.create(parameterSchema, allocator)) {
        final VarCharVector varCharVector = (VarCharVector) insertRoot.getVector(0);
        final IntVector valueVector = (IntVector) insertRoot.getVector(1);
        final int counter = 10;
        insertRoot.allocateNew();

        final IntStream range = IntStream.range(0, counter);

        range.forEach(
            i -> {
              valueVector.setSafe(i, i * counter);
              varCharVector.setSafe(i, new Text("value" + i));
            });

        insertRoot.setRowCount(counter);

        prepare.setParameters(insertRoot);
        final long updatedRows = prepare.executeUpdate();

        final long deletedRows;
        try (final VectorSchemaRoot deleteRoot = VectorSchemaRoot.of(varCharVector)) {
          deletePrepare.setParameters(deleteRoot);
          deletedRows = deletePrepare.executeUpdate();
        }
        assertAll(
            () -> Assertions.assertThat(updatedRows).isEqualTo(10L),
            () -> Assertions.assertThat(deletedRows).isEqualTo(10L));
      }
    }
  }

  @Test
  public void testBulkIngest() throws IOException {
    // For bulk ingest DerbyDB requires uppercase column names
    var keyName = new Field("KEYNAME", FieldType.nullable(new ArrowType.Utf8()), null);
    var value = new Field("VALUE", FieldType.nullable(new ArrowType.Int(32, true)), null);
    var foreignId = new Field("FOREIGNID", FieldType.nullable(new ArrowType.Int(32, true)), null);

    Schema dataSchema = new Schema(List.of(keyName, value, foreignId));

    try (final VectorSchemaRoot ingestRoot = VectorSchemaRoot.create(dataSchema, allocator);
        final VarCharVector keyNamesToBeDeletedVector = new VarCharVector(keyName, allocator)) {
      final VarCharVector keyNameVector = (VarCharVector) ingestRoot.getVector(0);
      final IntVector valueVector = (IntVector) ingestRoot.getVector(1);
      final IntVector foreignIdVector = (IntVector) ingestRoot.getVector(2);
      ingestRoot.allocateNew();
      keyNamesToBeDeletedVector.allocateNew();

      try (PipedInputStream inPipe = new PipedInputStream(1024);
          PipedOutputStream outPipe = new PipedOutputStream(inPipe);
          ArrowStreamReader reader = new ArrowStreamReader(inPipe, allocator)) {

        new Thread(
                () -> {
                  try (ArrowStreamWriter writer =
                      new ArrowStreamWriter(ingestRoot, null, outPipe)) {
                    writer.start();
                    populateNext10RowsInIngestRootBatch(
                        1,
                        valueVector,
                        keyNameVector,
                        foreignIdVector,
                        keyNamesToBeDeletedVector,
                        ingestRoot);
                    writer.writeBatch();
                    populateNext10RowsInIngestRootBatch(
                        11,
                        valueVector,
                        keyNameVector,
                        foreignIdVector,
                        keyNamesToBeDeletedVector,
                        ingestRoot);
                    writer.writeBatch();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                })
            .start();

        // Ingest from a stream
        final long updatedRows =
            sqlClient.executeIngest(
                reader,
                new FlightSqlClient.ExecuteIngestOptions(
                    "INTTABLE",
                    TableDefinitionOptions.newBuilder()
                        .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                        .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
                        .build(),
                    null,
                    null,
                    null));

        Assertions.assertThat(updatedRows).isEqualTo(-1L);

        // Ingest directly using VectorSchemaRoot
        populateNext10RowsInIngestRootBatch(
            21, valueVector, keyNameVector, foreignIdVector, keyNamesToBeDeletedVector, ingestRoot);
        sqlClient.executeIngest(
            ingestRoot,
            new FlightSqlClient.ExecuteIngestOptions(
                "INTTABLE",
                TableDefinitionOptions.newBuilder()
                    .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                    .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
                    .build(),
                null,
                null,
                null));

        try (PreparedStatement deletePrepare =
            sqlClient.prepare("DELETE FROM INTTABLE WHERE keyName = ?")) {
          final long deletedRows;
          try (final VectorSchemaRoot deleteRoot = VectorSchemaRoot.of(keyNamesToBeDeletedVector)) {
            deletePrepare.setParameters(deleteRoot);
            deletedRows = deletePrepare.executeUpdate();
          }

          Assertions.assertThat(deletedRows).isEqualTo(30L);
        }
      }
    }
  }

  @Test
  public void testBulkIngestTransaction() {
    assertThrows(
        RuntimeException.class,
        () -> {
          sqlClient.executeIngest(
              VectorSchemaRoot.create(new Schema(List.of()), allocator),
              new FlightSqlClient.ExecuteIngestOptions(
                  "INTTABLE",
                  TableDefinitionOptions.newBuilder()
                      .setIfExists(TableExistsOption.TABLE_EXISTS_OPTION_APPEND)
                      .setIfNotExist(TableNotExistOption.TABLE_NOT_EXIST_OPTION_FAIL)
                      .build(),
                  null,
                  null,
                  null),
              new FlightSqlClient.Transaction("123".getBytes(StandardCharsets.UTF_8)));
        });
  }

  @Test
  public void testSimplePreparedStatementUpdateResultsWithoutParameters() throws SQLException {
    try (PreparedStatement prepare =
            sqlClient.prepare("INSERT INTO INTTABLE (keyName, value ) VALUES ('test', 1000)");
        PreparedStatement deletePrepare =
            sqlClient.prepare("DELETE FROM INTTABLE WHERE keyName = 'test'")) {
      final long updatedRows = prepare.executeUpdate();

      final long deletedRows = deletePrepare.executeUpdate();

      assertAll(
          () -> Assertions.assertThat(updatedRows).isEqualTo(1L),
          () -> Assertions.assertThat(deletedRows).isEqualTo(1L));
    }
  }

  @Test
  public void testSimplePreparedStatementClosesProperly() {
    final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable");
    assertAll(
        () -> {
          Assertions.assertThat(preparedStatement.isClosed()).isEqualTo(false);
        },
        () -> {
          preparedStatement.close();
          Assertions.assertThat(preparedStatement.isClosed()).isEqualTo(true);
        });
  }

  @Test
  public void testGetCatalogsSchema() {
    final FlightInfo info = sqlClient.getCatalogs();
    Assertions.assertThat(info.getSchemaOptional())
        .isEqualTo(Optional.of(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA));
  }

  @Test
  public void testGetCatalogsResults() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(sqlClient.getCatalogs().getEndpoints().get(0).getTicket())) {
      assertAll(
          () ->
              Assertions.assertThat(stream.getSchema())
                  .isEqualTo(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA),
          () -> {
            List<List<String>> catalogs = getResults(stream);
            Assertions.assertThat(catalogs).isEqualTo(emptyList());
          });
    }
  }

  @Test
  public void testGetTableTypesSchema() {
    final FlightInfo info = sqlClient.getTableTypes();
    Assertions.assertThat(info.getSchemaOptional())
        .isEqualTo(Optional.of(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA));
  }

  @Test
  public void testGetTableTypesResult() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(sqlClient.getTableTypes().getEndpoints().get(0).getTicket())) {
      assertAll(
          () -> {
            Assertions.assertThat(stream.getSchema())
                .isEqualTo(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA);
          },
          () -> {
            final List<List<String>> tableTypes = getResults(stream);
            final List<List<String>> expectedTableTypes =
                ImmutableList.of(
                    // table_type
                    singletonList("SYNONYM"),
                    singletonList("SYSTEM TABLE"),
                    singletonList("TABLE"),
                    singletonList("VIEW"));
            Assertions.assertThat(tableTypes).isEqualTo(expectedTableTypes);
          });
    }
  }

  @Test
  public void testGetSchemasSchema() {
    final FlightInfo info = sqlClient.getSchemas(null, null);
    Assertions.assertThat(info.getSchemaOptional())
        .isEqualTo(Optional.of(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA));
  }

  @Test
  public void testGetSchemasResult() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(sqlClient.getSchemas(null, null).getEndpoints().get(0).getTicket())) {
      assertAll(
          () -> {
            Assertions.assertThat(stream.getSchema())
                .isEqualTo(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA);
          },
          () -> {
            final List<List<String>> schemas = getResults(stream);
            final List<List<String>> expectedSchemas =
                ImmutableList.of(
                    // catalog_name | schema_name
                    asList(null /* TODO Add catalog. */, "APP"),
                    asList(null /* TODO Add catalog. */, "NULLID"),
                    asList(null /* TODO Add catalog. */, "SQLJ"),
                    asList(null /* TODO Add catalog. */, "SYS"),
                    asList(null /* TODO Add catalog. */, "SYSCAT"),
                    asList(null /* TODO Add catalog. */, "SYSCS_DIAG"),
                    asList(null /* TODO Add catalog. */, "SYSCS_UTIL"),
                    asList(null /* TODO Add catalog. */, "SYSFUN"),
                    asList(null /* TODO Add catalog. */, "SYSIBM"),
                    asList(null /* TODO Add catalog. */, "SYSPROC"),
                    asList(null /* TODO Add catalog. */, "SYSSTAT"));
            Assertions.assertThat(schemas).isEqualTo(expectedSchemas);
          });
    }
  }

  @Test
  public void testGetPrimaryKey() {
    final FlightInfo flightInfo = sqlClient.getPrimaryKeys(TableRef.of(null, null, "INTTABLE"));
    final FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    assertAll(
        () -> Assertions.assertThat(results.size()).isEqualTo(1),
        () -> {
          final List<String> result = results.get(0);
          assertAll(
              () -> Assertions.assertThat(result.get(0)).isEqualTo(""),
              () -> Assertions.assertThat(result.get(1)).isEqualTo("APP"),
              () -> Assertions.assertThat(result.get(2)).isEqualTo("INTTABLE"),
              () -> Assertions.assertThat(result.get(3)).isEqualTo("ID"),
              () -> Assertions.assertThat(result.get(4)).isEqualTo("1"),
              () -> Assertions.assertThat(result.get(5)).isNotNull());
        });
  }

  @Test
  public void testGetSqlInfoSchema() {
    final FlightInfo info = sqlClient.getSqlInfo();
    Assertions.assertThat(info.getSchemaOptional())
        .isEqualTo(Optional.of(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
  }

  @Test
  public void testGetSqlInfoResults() throws Exception {
    final FlightInfo info = sqlClient.getSqlInfo();
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      assertAll(
          () ->
              Assertions.assertThat(stream.getSchema())
                  .isEqualTo(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA),
          () ->
              Assertions.assertThat(getNonConformingResultsForGetSqlInfo(getResults(stream)))
                  .isEqualTo(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithSingleArg() throws Exception {
    final FlightSql.SqlInfo arg = FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME;
    final FlightInfo info = sqlClient.getSqlInfo(arg);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      assertAll(
          () ->
              Assertions.assertThat(stream.getSchema())
                  .isEqualTo(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA),
          () ->
              Assertions.assertThat(getNonConformingResultsForGetSqlInfo(getResults(stream), arg))
                  .isEqualTo(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithManyArgs() throws Exception {
    final FlightSql.SqlInfo[] args = {
      FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
      FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
      FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION,
      FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY,
      FlightSql.SqlInfo.SQL_ALL_TABLES_ARE_SELECTABLE,
      FlightSql.SqlInfo.SQL_NULL_ORDERING,
      FlightSql.SqlInfo.SQL_DDL_CATALOG,
      FlightSql.SqlInfo.SQL_DDL_SCHEMA,
      FlightSql.SqlInfo.SQL_DDL_TABLE,
      FlightSql.SqlInfo.SQL_IDENTIFIER_CASE,
      FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
      FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE,
      FlightSql.SqlInfo.SQL_MAX_COLUMNS_IN_TABLE
    };
    final FlightInfo info = sqlClient.getSqlInfo(args);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      assertAll(
          () ->
              Assertions.assertThat(stream.getSchema())
                  .isEqualTo(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA),
          () ->
              Assertions.assertThat(getNonConformingResultsForGetSqlInfo(getResults(stream), args))
                  .isEqualTo(emptyList()));
    }
  }

  @Test
  public void testGetCommandExportedKeys() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(
            sqlClient
                .getExportedKeys(TableRef.of(null, null, "FOREIGNTABLE"))
                .getEndpoints()
                .get(0)
                .getTicket())) {

      final List<List<String>> results = getResults(stream);

      final List<Condition<String>> matchers =
          asList(
              new HamcrestCondition<>(nullValue(String.class)), // pk_catalog_name
              new HamcrestCondition<>(is("APP")), // pk_schema_name
              new HamcrestCondition<>(is("FOREIGNTABLE")), // pk_table_name
              new HamcrestCondition<>(is("ID")), // pk_column_name
              new HamcrestCondition<>(nullValue(String.class)), // fk_catalog_name
              new HamcrestCondition<>(is("APP")), // fk_schema_name
              new HamcrestCondition<>(is("INTTABLE")), // fk_table_name
              new HamcrestCondition<>(is("FOREIGNID")), // fk_column_name
              new HamcrestCondition<>(is("1")), // key_sequence
              new HamcrestCondition<>(containsString("SQL")), // fk_key_name
              new HamcrestCondition<>(containsString("SQL")), // pk_key_name
              new HamcrestCondition<>(is("3")), // update_rule
              new HamcrestCondition<>(is("3"))); // delete_rule

      final List<Executable> assertions = new ArrayList<>();
      assertEquals(1, results.size());
      for (int i = 0; i < matchers.size(); i++) {
        final String actual = results.get(0).get(i);
        final Condition<String> expected = matchers.get(i);
        assertions.add(() -> Assertions.assertThat(actual).satisfies(expected));
      }
      assertAll(assertions);
    }
  }

  @Test
  public void testGetCommandImportedKeys() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(
            sqlClient
                .getImportedKeys(TableRef.of(null, null, "INTTABLE"))
                .getEndpoints()
                .get(0)
                .getTicket())) {

      final List<List<String>> results = getResults(stream);

      final List<Condition<String>> matchers =
          asList(
              new HamcrestCondition<>(nullValue(String.class)), // pk_catalog_name
              new HamcrestCondition<>(is("APP")), // pk_schema_name
              new HamcrestCondition<>(is("FOREIGNTABLE")), // pk_table_name
              new HamcrestCondition<>(is("ID")), // pk_column_name
              new HamcrestCondition<>(nullValue(String.class)), // fk_catalog_name
              new HamcrestCondition<>(is("APP")), // fk_schema_name
              new HamcrestCondition<>(is("INTTABLE")), // fk_table_name
              new HamcrestCondition<>(is("FOREIGNID")), // fk_column_name
              new HamcrestCondition<>(is("1")), // key_sequence
              new HamcrestCondition<>(containsString("SQL")), // fk_key_name
              new HamcrestCondition<>(containsString("SQL")), // pk_key_name
              new HamcrestCondition<>(is("3")), // update_rule
              new HamcrestCondition<>(is("3"))); // delete_rule

      assertEquals(1, results.size());
      final List<Executable> assertions = new ArrayList<>();
      for (int i = 0; i < matchers.size(); i++) {
        final String actual = results.get(0).get(i);
        final Condition<String> expected = matchers.get(i);
        assertions.add(() -> Assertions.assertThat(actual).satisfies(expected));
      }
      assertAll(assertions);
    }
  }

  @Test
  public void testGetTypeInfo() throws Exception {
    FlightInfo flightInfo = sqlClient.getXdbcTypeInfo();

    try (FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

      final List<List<String>> results = getResults(stream);

      final List<List<String>> matchers =
          ImmutableList.of(
              asList(
                  "BIGINT",
                  "-5",
                  "19",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "true",
                  "BIGINT",
                  "0",
                  "0",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "LONG VARCHAR FOR BIT DATA",
                  "-4",
                  "32700",
                  "X'",
                  "'",
                  emptyList().toString(),
                  "1",
                  "false",
                  "0",
                  "true",
                  "false",
                  "false",
                  "LONG VARCHAR FOR BIT DATA",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "VARCHAR () FOR BIT DATA",
                  "-3",
                  "32672",
                  "X'",
                  "'",
                  singletonList("length").toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "VARCHAR () FOR BIT DATA",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "CHAR () FOR BIT DATA",
                  "-2",
                  "254",
                  "X'",
                  "'",
                  singletonList("length").toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "CHAR () FOR BIT DATA",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "LONG VARCHAR",
                  "-1",
                  "32700",
                  "'",
                  "'",
                  emptyList().toString(),
                  "1",
                  "true",
                  "1",
                  "true",
                  "false",
                  "false",
                  "LONG VARCHAR",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "CHAR",
                  "1",
                  "254",
                  "'",
                  "'",
                  singletonList("length").toString(),
                  "1",
                  "true",
                  "3",
                  "true",
                  "false",
                  "false",
                  "CHAR",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "NUMERIC",
                  "2",
                  "31",
                  null,
                  null,
                  Arrays.asList("precision", "scale").toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "true",
                  "false",
                  "NUMERIC",
                  "0",
                  "31",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "DECIMAL",
                  "3",
                  "31",
                  null,
                  null,
                  Arrays.asList("precision", "scale").toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "true",
                  "false",
                  "DECIMAL",
                  "0",
                  "31",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "INTEGER",
                  "4",
                  "10",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "true",
                  "INTEGER",
                  "0",
                  "0",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "SMALLINT",
                  "5",
                  "5",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "true",
                  "SMALLINT",
                  "0",
                  "0",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "FLOAT",
                  "6",
                  "52",
                  null,
                  null,
                  singletonList("precision").toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "false",
                  "FLOAT",
                  null,
                  null,
                  null,
                  null,
                  "2",
                  null),
              asList(
                  "REAL",
                  "7",
                  "23",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "false",
                  "REAL",
                  null,
                  null,
                  null,
                  null,
                  "2",
                  null),
              asList(
                  "DOUBLE",
                  "8",
                  "52",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "false",
                  "DOUBLE",
                  null,
                  null,
                  null,
                  null,
                  "2",
                  null),
              asList(
                  "VARCHAR",
                  "12",
                  "32672",
                  "'",
                  "'",
                  singletonList("length").toString(),
                  "1",
                  "true",
                  "3",
                  "true",
                  "false",
                  "false",
                  "VARCHAR",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "BOOLEAN",
                  "16",
                  "1",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "BOOLEAN",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "DATE",
                  "91",
                  "10",
                  "DATE'",
                  "'",
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "DATE",
                  "0",
                  "0",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "TIME",
                  "92",
                  "8",
                  "TIME'",
                  "'",
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "TIME",
                  "0",
                  "0",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "TIMESTAMP",
                  "93",
                  "29",
                  "TIMESTAMP'",
                  "'",
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "TIMESTAMP",
                  "0",
                  "9",
                  null,
                  null,
                  "10",
                  null),
              asList(
                  "OBJECT",
                  "2000",
                  null,
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "true",
                  "false",
                  "false",
                  "OBJECT",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "BLOB",
                  "2004",
                  "2147483647",
                  null,
                  null,
                  singletonList("length").toString(),
                  "1",
                  "false",
                  "0",
                  null,
                  "false",
                  null,
                  "BLOB",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "CLOB",
                  "2005",
                  "2147483647",
                  "'",
                  "'",
                  singletonList("length").toString(),
                  "1",
                  "true",
                  "1",
                  null,
                  "false",
                  null,
                  "CLOB",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null),
              asList(
                  "XML",
                  "2009",
                  null,
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "true",
                  "0",
                  "false",
                  "false",
                  "false",
                  "XML",
                  null,
                  null,
                  null,
                  null,
                  null,
                  null));
      Assertions.assertThat(results).isEqualTo(matchers);
    }
  }

  @Test
  public void testGetTypeInfoWithFiltering() throws Exception {
    FlightInfo flightInfo = sqlClient.getXdbcTypeInfo(-5);

    try (FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

      final List<List<String>> results = getResults(stream);

      final List<List<String>> matchers =
          ImmutableList.of(
              asList(
                  "BIGINT",
                  "-5",
                  "19",
                  null,
                  null,
                  emptyList().toString(),
                  "1",
                  "false",
                  "2",
                  "false",
                  "false",
                  "true",
                  "BIGINT",
                  "0",
                  "0",
                  null,
                  null,
                  "10",
                  null));
      Assertions.assertThat(results).isEqualTo(matchers);
    }
  }

  @Test
  public void testGetCommandCrossReference() throws Exception {
    final FlightInfo flightInfo =
        sqlClient.getCrossReference(
            TableRef.of(null, null, "FOREIGNTABLE"), TableRef.of(null, null, "INTTABLE"));
    try (final FlightStream stream =
        sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

      final List<List<String>> results = getResults(stream);

      final List<Condition<String>> matchers =
          asList(
              new HamcrestCondition<>(nullValue(String.class)), // pk_catalog_name
              new HamcrestCondition<>(is("APP")), // pk_schema_name
              new HamcrestCondition<>(is("FOREIGNTABLE")), // pk_table_name
              new HamcrestCondition<>(is("ID")), // pk_column_name
              new HamcrestCondition<>(nullValue(String.class)), // fk_catalog_name
              new HamcrestCondition<>(is("APP")), // fk_schema_name
              new HamcrestCondition<>(is("INTTABLE")), // fk_table_name
              new HamcrestCondition<>(is("FOREIGNID")), // fk_column_name
              new HamcrestCondition<>(is("1")), // key_sequence
              new HamcrestCondition<>(containsString("SQL")), // fk_key_name
              new HamcrestCondition<>(containsString("SQL")), // pk_key_name
              new HamcrestCondition<>(is("3")), // update_rule
              new HamcrestCondition<>(is("3"))); // delete_rule

      assertEquals(1, results.size());
      final List<Executable> assertions = new ArrayList<>();
      for (int i = 0; i < matchers.size(); i++) {
        final String actual = results.get(0).get(i);
        final Condition<String> expected = matchers.get(i);
        assertions.add(() -> Assertions.assertThat(actual).satisfies(expected));
      }
      assertAll(assertions);
    }
  }

  @Test
  public void testCreateStatementSchema() throws Exception {
    final FlightInfo info = sqlClient.execute("SELECT * FROM intTable");
    Assertions.assertThat(info.getSchemaOptional()).isEqualTo(Optional.of(SCHEMA_INT_TABLE));

    // Consume statement to close connection before cache eviction
    try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      while (stream.next()) {
        // Do nothing
      }
    }
  }

  @Test
  public void testCreateStatementResults() throws Exception {
    try (final FlightStream stream =
        sqlClient.getStream(
            sqlClient.execute("SELECT * FROM intTable").getEndpoints().get(0).getTicket())) {
      assertAll(
          () -> {
            Assertions.assertThat(stream.getSchema()).isEqualTo(SCHEMA_INT_TABLE);
          },
          () -> {
            Assertions.assertThat(getResults(stream))
                .isEqualTo(EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY);
          });
    }
  }

  @Test
  public void testExecuteUpdate() {
    assertAll(
        () -> {
          long insertedCount =
              sqlClient.executeUpdate(
                  "INSERT INTO INTTABLE (keyName, value) VALUES "
                      + "('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)");
          Assertions.assertThat(insertedCount).isEqualTo(3L);
        },
        () -> {
          long updatedCount =
              sqlClient.executeUpdate(
                  "UPDATE INTTABLE SET keyName = 'KEYNAME1' "
                      + "WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'");
          Assertions.assertThat(updatedCount).isEqualTo(2L);
        },
        () -> {
          long deletedCount =
              sqlClient.executeUpdate("DELETE FROM INTTABLE WHERE keyName = 'KEYNAME1'");
          Assertions.assertThat(deletedCount).isEqualTo(3L);
        });
  }

  @Test
  public void testQueryWithNoResultsShouldNotHang() throws Exception {
    try (final PreparedStatement preparedStatement =
            sqlClient.prepare("SELECT * FROM intTable WHERE 1 = 0");
        final FlightStream stream =
            sqlClient.getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
      assertAll(
          () -> Assertions.assertThat(stream.getSchema()).isEqualTo(SCHEMA_INT_TABLE),
          () -> {
            final List<List<String>> result = getResults(stream);
            Assertions.assertThat(result).isEqualTo(emptyList());
          });
    }
  }

  @Test
  public void testCancelFlightInfo() {
    FlightInfo info = sqlClient.getSqlInfo();
    CancelFlightInfoRequest request = new CancelFlightInfoRequest(info);
    FlightRuntimeException fre =
        assertThrows(FlightRuntimeException.class, () -> sqlClient.cancelFlightInfo(request));
    assertEquals(FlightStatusCode.UNIMPLEMENTED, fre.status().code());
  }

  @Test
  public void testCancelQuery() {
    FlightInfo info = sqlClient.getSqlInfo();
    FlightRuntimeException fre =
        assertThrows(FlightRuntimeException.class, () -> sqlClient.cancelQuery(info));
    assertEquals(FlightStatusCode.UNIMPLEMENTED, fre.status().code());
  }

  @Test
  public void testRenewEndpoint() {
    FlightInfo info = sqlClient.getSqlInfo();
    FlightRuntimeException fre =
        assertThrows(
            FlightRuntimeException.class,
            () ->
                sqlClient.renewFlightEndpoint(
                    new RenewFlightEndpointRequest(info.getEndpoints().get(0))));
    assertEquals(FlightStatusCode.UNIMPLEMENTED, fre.status().code());
  }
}
