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
package org.apache.arrow.driver.jdbc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link AvaticaParameterBinder}. */
public class AvaticaParameterBinderTest {

  private static final Schema PARAMETER_SCHEMA =
      new Schema(
          Arrays.asList(
              new Field("text", FieldType.notNullable(new ArrowType.Utf8()), null),
              new Field("number", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator();
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testFailedBindReleasesPreviouslyBoundValues() {
    for (int i = 0; i < 3; i++) {
      try (StubPreparedStatement statement = new StubPreparedStatement()) {
        AvaticaParameterBinder binder = new AvaticaParameterBinder(statement, allocator);
        UnsupportedOperationException e =
            assertThrows(
                UnsupportedOperationException.class,
                () ->
                    binder.bind(
                        Arrays.asList(
                            TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "O'Reilly"), null)));
        assertEquals("Can't set null on non-nullable parameter", e.getMessage());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  public void testFailedBindOnTypeMismatchReleasesPreviouslyBoundValues() {
    try (StubPreparedStatement statement = new StubPreparedStatement()) {
      AvaticaParameterBinder binder = new AvaticaParameterBinder(statement, allocator);
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              binder.bind(
                  Arrays.asList(
                      TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "text"),
                      TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "not a number"))));
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  @Test
  public void testFailedBatchBindAfterHandOffKeepsStatementParameters() {
    try (StubPreparedStatement statement = new StubPreparedStatement()) {
      AvaticaParameterBinder binder = new AvaticaParameterBinder(statement, allocator);
      binder.bind(
          Arrays.asList(
              TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "text"),
              TypedValue.ofLocal(ColumnMetaData.Rep.LONG, 42L)),
          0);
      VectorSchemaRoot handedOff = statement.parameters;
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              binder.bind(
                  Arrays.asList(TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "more"), null), 1));
      // The statement owns the root after the first row, so it must still be usable.
      assertSame(handedOff, statement.parameters);
      assertEquals(1, statement.parameters.getRowCount());
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  @Test
  public void testFailedSetRowCountReleasesBoundValues() {
    Schema nullableFixedSizeListSchema =
        new Schema(
            Collections.singletonList(
                new Field(
                    "list",
                    FieldType.nullable(new ArrowType.FixedSizeList(4)),
                    Collections.singletonList(
                        new Field(
                            "item", FieldType.nullable(new ArrowType.Int(64, true)), null)))));
    // Binding null to a FixedSizeListVector only allocates its validity buffer (512 bytes) during
    // bind(), while setRowCount(1) allocates the child vector's buffers (32 KiB).
    try (BufferAllocator cappedAllocator = allocator.newChildAllocator("capped", 0, 1024);
        StubPreparedStatement statement = new StubPreparedStatement(nullableFixedSizeListSchema)) {
      AvaticaParameterBinder binder = new AvaticaParameterBinder(statement, cappedAllocator);
      assertThrows(OutOfMemoryException.class, () -> binder.bind(Collections.singletonList(null)));
      assertEquals(0, cappedAllocator.getAllocatedMemory());
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  @Test
  public void testFailedSetParametersReleasesBoundValues() {
    try (StubPreparedStatement statement =
        new StubPreparedStatement(PARAMETER_SCHEMA) {
          @Override
          public void setParameters(VectorSchemaRoot parameters) {
            throw new IllegalStateException("Statement closed");
          }
        }) {
      AvaticaParameterBinder binder = new AvaticaParameterBinder(statement, allocator);
      assertThrows(
          IllegalStateException.class,
          () ->
              binder.bind(
                  Arrays.asList(
                      TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "text"),
                      TypedValue.ofLocal(ColumnMetaData.Rep.LONG, 42L))));
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** A prepared statement that owns the parameters it is given, like the Flight SQL client. */
  private static class StubPreparedStatement
      implements ArrowFlightSqlClientHandler.PreparedStatement {
    private final Schema parameterSchema;
    private VectorSchemaRoot parameters;

    StubPreparedStatement() {
      this(PARAMETER_SCHEMA);
    }

    StubPreparedStatement(Schema parameterSchema) {
      this.parameterSchema = parameterSchema;
    }

    @Override
    public FlightInfo executeQuery() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long executeUpdate() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Meta.StatementType getType() {
      return Meta.StatementType.SELECT;
    }

    @Override
    public Schema getDataSetSchema() {
      return new Schema(Collections.emptyList());
    }

    @Override
    public Schema getParameterSchema() {
      return parameterSchema;
    }

    @Override
    public Boolean isUpdate() {
      return false;
    }

    @Override
    public void setParameters(VectorSchemaRoot parameters) {
      this.parameters = parameters;
    }

    @Override
    public void close() {
      if (parameters != null) {
        parameters.close();
      }
    }
  }
}
