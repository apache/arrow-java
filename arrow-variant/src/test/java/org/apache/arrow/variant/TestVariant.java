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
package org.apache.arrow.variant;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.parquet.variant.VariantBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVariant {

  private BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  static Variant buildVariant(VariantBuilder builder) {
    org.apache.parquet.variant.Variant parquetVariant = builder.build();
    ByteBuffer valueBuf = parquetVariant.getValueBuffer();
    ByteBuffer metaBuf = parquetVariant.getMetadataBuffer();
    byte[] valueBytes = new byte[valueBuf.remaining()];
    byte[] metaBytes = new byte[metaBuf.remaining()];
    valueBuf.get(valueBytes);
    metaBuf.get(metaBytes);
    return new Variant(metaBytes, valueBytes);
  }

  public static Variant variantString(String value) {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString(value);
    return buildVariant(builder);
  }

  @Test
  void testConstructionWithArrowBuf() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendInt(42);
    Variant source = buildVariant(builder);
    int metaLen = source.getMetadataBuffer().remaining();
    int valueLen = source.getValueBuffer().remaining();

    try (ArrowBuf metadataArrowBuf = allocator.buffer(metaLen + 2);
        ArrowBuf valueArrowBuf = allocator.buffer(valueLen + 3)) {
      metadataArrowBuf.setBytes(2, source.getMetadataBuffer());
      valueArrowBuf.setBytes(3, source.getValueBuffer());

      Variant variant =
          new Variant(metadataArrowBuf, 2, 2 + metaLen, valueArrowBuf, 3, 3 + valueLen);

      assertEquals(Variant.Type.INT, variant.getType());
      assertEquals(42, variant.getInt());
    }
  }

  @Test
  void testNullType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendNull();
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.NULL, variant.getType());
  }

  @Test
  void testBooleanType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendBoolean(true);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.BOOLEAN, variant.getType());
    assertTrue(variant.getBoolean());

    builder = new VariantBuilder();
    builder.appendBoolean(false);
    variant = buildVariant(builder);

    assertEquals(Variant.Type.BOOLEAN, variant.getType());
    assertFalse(variant.getBoolean());
  }

  @Test
  void testByteType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendByte((byte) 42);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.BYTE, variant.getType());
    assertEquals((byte) 42, variant.getByte());
  }

  @Test
  void testShortType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendShort((short) 1234);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.SHORT, variant.getType());
    assertEquals((short) 1234, variant.getShort());
  }

  @Test
  void testIntType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendInt(123456);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.INT, variant.getType());
    assertEquals(123456, variant.getInt());
  }

  @Test
  void testLongType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendLong(9876543210L);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.LONG, variant.getType());
    assertEquals(9876543210L, variant.getLong());
  }

  @Test
  void testFloatType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendFloat(3.14f);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.FLOAT, variant.getType());
    assertEquals(3.14f, variant.getFloat(), 0.001f);
  }

  @Test
  void testDoubleType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDouble(3.14159265359);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.DOUBLE, variant.getType());
    assertEquals(3.14159265359, variant.getDouble(), 0.0000001);
  }

  @Test
  void testStringType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString("hello world");
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.STRING, variant.getType());
    assertEquals("hello world", variant.getString());
  }

  @Test
  void testDecimalType() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendDecimal(new BigDecimal("123.456"));
    Variant variant = buildVariant(builder);

    assertTrue(
        variant.getType() == Variant.Type.DECIMAL4
            || variant.getType() == Variant.Type.DECIMAL8
            || variant.getType() == Variant.Type.DECIMAL16);
    assertEquals(new BigDecimal("123.456"), variant.getDecimal());
  }

  @Test
  void testBinaryType() {
    VariantBuilder builder = new VariantBuilder();
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    builder.appendBinary(ByteBuffer.wrap(data));
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.BINARY, variant.getType());
    ByteBuffer result = variant.getBinary();
    byte[] resultBytes = new byte[result.remaining()];
    result.get(resultBytes);
    assertArrayEquals(data, resultBytes);
  }

  @Test
  void testUuidType() {
    VariantBuilder builder = new VariantBuilder();
    UUID uuid = UUID.randomUUID();
    builder.appendUUID(uuid);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.UUID, variant.getType());
    assertEquals(uuid, variant.getUUID());
  }

  @Test
  void testDateType() {
    VariantBuilder builder = new VariantBuilder();
    int daysSinceEpoch = 19000;
    builder.appendDate(daysSinceEpoch);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.DATE, variant.getType());
  }

  @Test
  void testTimestampTzType() {
    VariantBuilder builder = new VariantBuilder();
    long micros = System.currentTimeMillis() * 1000;
    builder.appendTimestampTz(micros);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.TIMESTAMP_TZ, variant.getType());
  }

  @Test
  void testTimestampNtzType() {
    VariantBuilder builder = new VariantBuilder();
    long micros = System.currentTimeMillis() * 1000;
    builder.appendTimestampNtz(micros);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.TIMESTAMP_NTZ, variant.getType());
  }

  @Test
  void testTimeType() {
    VariantBuilder builder = new VariantBuilder();
    long micros = 12345678L;
    builder.appendTime(micros);
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.TIME, variant.getType());
  }

  @Test
  void testObjectType() {
    VariantBuilder builder = new VariantBuilder();
    var objBuilder = builder.startObject();
    objBuilder.appendKey("name");
    objBuilder.appendString("test");
    objBuilder.appendKey("value");
    objBuilder.appendInt(42);
    builder.endObject();
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.OBJECT, variant.getType());
    assertEquals(2, variant.numObjectElements());

    Variant nameField = variant.getFieldByKey("name");
    assertNotNull(nameField);
    assertEquals(Variant.Type.STRING, nameField.getType());
    assertEquals("test", nameField.getString());

    Variant valueField = variant.getFieldByKey("value");
    assertNotNull(valueField);
    assertEquals(Variant.Type.INT, valueField.getType());
    assertEquals(42, valueField.getInt());

    assertNull(variant.getFieldByKey("nonexistent"));

    // Empty object
    builder = new VariantBuilder();
    builder.startObject();
    builder.endObject();
    Variant emptyObj = buildVariant(builder);
    assertEquals(Variant.Type.OBJECT, emptyObj.getType());
    assertEquals(0, emptyObj.numObjectElements());
  }

  @Test
  void testObjectFieldAtIndex() {
    VariantBuilder builder = new VariantBuilder();
    var objBuilder = builder.startObject();
    objBuilder.appendKey("alpha");
    objBuilder.appendInt(1);
    objBuilder.appendKey("beta");
    objBuilder.appendInt(2);
    builder.endObject();
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.OBJECT, variant.getType());
    assertEquals(2, variant.numObjectElements());

    Variant.ObjectField field0 = variant.getFieldAtIndex(0);
    assertNotNull(field0);
    assertNotNull(field0.key);
    assertNotNull(field0.value);

    Variant.ObjectField field1 = variant.getFieldAtIndex(1);
    assertNotNull(field1);
    assertNotNull(field1.key);
    assertNotNull(field1.value);
  }

  @Test
  void testArrayType() {
    VariantBuilder builder = new VariantBuilder();
    var arrayBuilder = builder.startArray();
    arrayBuilder.appendInt(1);
    arrayBuilder.appendInt(2);
    arrayBuilder.appendInt(3);
    builder.endArray();
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.ARRAY, variant.getType());
    assertEquals(3, variant.numArrayElements());

    Variant elem0 = variant.getElementAtIndex(0);
    assertNotNull(elem0);
    assertEquals(Variant.Type.INT, elem0.getType());
    assertEquals(1, elem0.getInt());

    Variant elem1 = variant.getElementAtIndex(1);
    assertEquals(2, elem1.getInt());

    Variant elem2 = variant.getElementAtIndex(2);
    assertEquals(3, elem2.getInt());

    assertNull(variant.getElementAtIndex(-1));
    assertNull(variant.getElementAtIndex(3));

    // Empty array
    builder = new VariantBuilder();
    builder.startArray();
    builder.endArray();
    Variant emptyArr = buildVariant(builder);
    assertEquals(Variant.Type.ARRAY, emptyArr.getType());
    assertEquals(0, emptyArr.numArrayElements());
  }

  @Test
  void testNestedStructure() {
    VariantBuilder builder = new VariantBuilder();
    var objBuilder = builder.startObject();
    objBuilder.appendKey("items");
    var arrayBuilder = objBuilder.startArray();
    arrayBuilder.appendString("a");
    arrayBuilder.appendString("b");
    objBuilder.endArray();
    builder.endObject();
    Variant variant = buildVariant(builder);

    assertEquals(Variant.Type.OBJECT, variant.getType());
    Variant items = variant.getFieldByKey("items");
    assertNotNull(items);
    assertEquals(Variant.Type.ARRAY, items.getType());
    assertEquals(2, items.numArrayElements());
    assertEquals("a", items.getElementAtIndex(0).getString());
    assertEquals("b", items.getElementAtIndex(1).getString());
  }

  @Test
  void testEquals() {
    VariantBuilder builder1 = new VariantBuilder();
    builder1.appendString("test");
    Variant variant1 = buildVariant(builder1);

    VariantBuilder builder2 = new VariantBuilder();
    builder2.appendString("test");
    Variant variant2 = buildVariant(builder2);

    VariantBuilder builder3 = new VariantBuilder();
    builder3.appendString("different");
    Variant variant3 = buildVariant(builder3);

    assertEquals(variant1, variant1);
    assertEquals(variant1, variant2);
    assertNotEquals(variant1, variant3);
    assertNotEquals(variant1, null);
    assertNotEquals(variant1, "not a variant");
  }

  @Test
  void testHashCode() {
    VariantBuilder builder1 = new VariantBuilder();
    builder1.appendInt(42);
    Variant variant1 = buildVariant(builder1);

    VariantBuilder builder2 = new VariantBuilder();
    builder2.appendInt(42);
    Variant variant2 = buildVariant(builder2);

    assertEquals(variant1.hashCode(), variant2.hashCode());
  }

  @Test
  void testToString() {
    VariantBuilder builder = new VariantBuilder();
    builder.appendString("test");
    Variant variant = buildVariant(builder);

    String str = variant.toString();
    assertNotNull(str);
    assertTrue(str.contains("type="));
  }

  @Test
  void testTypeEnumsMatch() {
    for (Variant.Type arrowType : Variant.Type.values()) {
      org.apache.parquet.variant.Variant.Type parquetType =
          org.apache.parquet.variant.Variant.Type.valueOf(arrowType.name());
      assertEquals(arrowType, Variant.Type.fromParquet(parquetType));
    }
    for (org.apache.parquet.variant.Variant.Type parquetType :
        org.apache.parquet.variant.Variant.Type.values()) {
      Variant.Type arrowType = Variant.Type.valueOf(parquetType.name());
      assertEquals(parquetType.name(), arrowType.name());
    }
  }
}
