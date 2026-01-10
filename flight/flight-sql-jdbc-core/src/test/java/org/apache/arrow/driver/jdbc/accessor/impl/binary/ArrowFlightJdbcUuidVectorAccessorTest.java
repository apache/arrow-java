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
package org.apache.arrow.driver.jdbc.accessor.impl.binary;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.UUID;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.util.UuidUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link ArrowFlightJdbcUuidVectorAccessor}.
 *
 * <p>Verifies that the accessor correctly handles UUID values from Arrow's UUID extension type,
 * following PostgreSQL JDBC driver conventions.
 */
public class ArrowFlightJdbcUuidVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private static final UUID UUID_1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
  private static final UUID UUID_2 = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
  private static final UUID UUID_3 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

  private UuidVector vector;
  private ArrowFlightJdbcUuidVectorAccessor accessor;
  private boolean wasNullCalled;
  private boolean wasNullValue;

  @BeforeEach
  public void setUp() {
    vector = rootAllocatorTestExtension.createUuidVector();
    wasNullCalled = false;
    wasNullValue = false;
    ArrowFlightJdbcAccessorFactory.WasNullConsumer wasNullConsumer =
        (wasNull) -> {
          wasNullCalled = true;
          wasNullValue = wasNull;
        };
    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, wasNullConsumer);
  }

  @AfterEach
  public void tearDown() {
    vector.close();
  }

  @Test
  public void testGetObjectReturnsUuid() {
    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    Object result = accessor.getObject();
    assertThat(result, is(UUID_1));
    assertThat(accessor.wasNull(), is(false));
  }

  @Test
  public void testGetObjectReturnsCorrectUuidForEachRow() {
    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    assertThat(accessor.getObject(), is(UUID_1));

    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 1, (wasNull) -> {});
    assertThat(accessor.getObject(), is(UUID_2));

    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 2, (wasNull) -> {});
    assertThat(accessor.getObject(), is(UUID_3));
  }

  @Test
  public void testGetObjectReturnsNullForNullValue() {
    vector.reset();
    vector.allocateNew(1);
    vector.setNull(0);
    vector.setValueCount(1);

    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    Object result = accessor.getObject();
    assertThat(result, nullValue());
    assertThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testGetObjectClassReturnsUuidClass() {
    assertThat(accessor.getObjectClass(), equalTo(UUID.class));
  }

  @Test
  public void testGetStringReturnsHyphenatedFormat() {
    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    String result = accessor.getString();
    assertThat(result, is("550e8400-e29b-41d4-a716-446655440000"));
    assertThat(accessor.wasNull(), is(false));
  }

  @Test
  public void testGetStringReturnsNullForNullValue() {
    vector.reset();
    vector.allocateNew(1);
    vector.setNull(0);
    vector.setValueCount(1);

    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    String result = accessor.getString();
    assertThat(result, nullValue());
    assertThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testGetBytesReturns16ByteArray() {
    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    byte[] result = accessor.getBytes();
    assertThat(result.length, is(16));
    assertThat(result, is(UuidUtility.getBytesFromUUID(UUID_1)));
    assertThat(accessor.wasNull(), is(false));
  }

  @Test
  public void testGetBytesReturnsNullForNullValue() {
    vector.reset();
    vector.allocateNew(1);
    vector.setNull(0);
    vector.setValueCount(1);

    accessor = new ArrowFlightJdbcUuidVectorAccessor(vector, () -> 0, (wasNull) -> {});
    byte[] result = accessor.getBytes();
    assertThat(result, nullValue());
    assertThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testWasNullConsumerIsCalled() {
    accessor =
        new ArrowFlightJdbcUuidVectorAccessor(
            vector,
            () -> 0,
            (wasNull) -> {
              wasNullCalled = true;
              wasNullValue = wasNull;
            });
    accessor.getObject();
    assertThat(wasNullCalled, is(true));
    assertThat(wasNullValue, is(false));
  }

  @Test
  public void testWasNullConsumerIsCalledWithTrueForNull() {
    vector.reset();
    vector.allocateNew(1);
    vector.setNull(0);
    vector.setValueCount(1);

    accessor =
        new ArrowFlightJdbcUuidVectorAccessor(
            vector,
            () -> 0,
            (wasNull) -> {
              wasNullCalled = true;
              wasNullValue = wasNull;
            });
    accessor.getObject();
    assertThat(wasNullCalled, is(true));
    assertThat(wasNullValue, is(true));
  }
}
