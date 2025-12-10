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
package org.apache.arrow.driver.jdbc.converter.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.util.UUID;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.UuidUtility;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link UuidAvaticaParameterConverter}.
 *
 * <p>Verifies that the converter correctly handles UUID parameter binding from JDBC to Arrow's UUID
 * extension type.
 */
public class UuidAvaticaParameterConverterTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private static final UUID TEST_UUID = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

  private UuidVector vector;
  private UuidAvaticaParameterConverter converter;

  @BeforeEach
  public void setUp() {
    vector = new UuidVector("uuid_param", rootAllocatorTestExtension.getRootAllocator());
    vector.allocateNew(5);
    converter = new UuidAvaticaParameterConverter();
  }

  @AfterEach
  public void tearDown() {
    vector.close();
  }

  @Test
  public void testBindParameterWithUuidObject() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.OBJECT, TEST_UUID);

    boolean result = converter.bindParameter(vector, typedValue, 0);

    assertTrue(result);
    assertThat(vector.getObject(0), is(TEST_UUID));
  }

  @Test
  public void testBindParameterWithUuidString() {
    String uuidString = "550e8400-e29b-41d4-a716-446655440000";
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.STRING, uuidString);

    boolean result = converter.bindParameter(vector, typedValue, 0);

    assertTrue(result);
    assertThat(vector.getObject(0), is(TEST_UUID));
  }

  @Test
  public void testBindParameterWithByteArray() {
    byte[] uuidBytes = UuidUtility.getBytesFromUUID(TEST_UUID);
    ByteString byteString = new ByteString(uuidBytes);
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.BYTE_STRING, byteString);

    boolean result = converter.bindParameter(vector, typedValue, 0);

    assertTrue(result);
    assertThat(vector.getObject(0), is(TEST_UUID));
  }

  @Test
  public void testBindParameterWithNullValue() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.OBJECT, null);

    boolean result = converter.bindParameter(vector, typedValue, 0);

    assertTrue(result);
    assertTrue(vector.isNull(0));
    assertThat(vector.getObject(0), nullValue());
  }

  @Test
  public void testBindParameterWithInvalidByteArrayLength() {
    byte[] invalidBytes = new byte[8]; // Should be 16 bytes
    ByteString byteString = new ByteString(invalidBytes);
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.BYTE_STRING, byteString);

    assertThrows(
        IllegalArgumentException.class, () -> converter.bindParameter(vector, typedValue, 0));
  }

  @Test
  public void testBindParameterWithInvalidType() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.INTEGER, 12345);

    assertThrows(
        IllegalArgumentException.class, () -> converter.bindParameter(vector, typedValue, 0));
  }

  @Test
  public void testBindParameterMultipleValues() {
    UUID uuid1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    UUID uuid2 = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    UUID uuid3 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

    converter.bindParameter(vector, TypedValue.ofLocal(ColumnMetaData.Rep.OBJECT, uuid1), 0);
    converter.bindParameter(vector, TypedValue.ofLocal(ColumnMetaData.Rep.OBJECT, uuid2), 1);
    converter.bindParameter(vector, TypedValue.ofLocal(ColumnMetaData.Rep.OBJECT, uuid3), 2);

    assertThat(vector.getObject(0), is(uuid1));
    assertThat(vector.getObject(1), is(uuid2));
    assertThat(vector.getObject(2), is(uuid3));
  }

  @Test
  public void testCreateParameter() {
    Field uuidField = new Field("uuid_col", new FieldType(true, UuidType.INSTANCE, null), null);

    AvaticaParameter parameter = converter.createParameter(uuidField);

    assertThat(parameter.name, is("uuid_col"));
    assertThat(parameter.parameterType, is(Types.OTHER));
    assertThat(parameter.typeName, is("OTHER"));
    assertThat(parameter.className, equalTo(UUID.class.getCanonicalName()));
  }
}
