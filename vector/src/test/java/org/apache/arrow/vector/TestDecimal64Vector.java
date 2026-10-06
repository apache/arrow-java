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
package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.Channels;
import java.util.Collections;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.Decimal64HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableDecimal64HolderReaderImpl;
import org.apache.arrow.vector.holders.Decimal64Holder;
import org.apache.arrow.vector.holders.NullableDecimal64Holder;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.validate.ValidateUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDecimal64Vector {

  private static long[] intValues;

  static {
    intValues = new long[60];
    for (int i = 0; i < intValues.length / 2; i++) {
      intValues[i] = 1L << (i + 1);
      intValues[2 * i] = -1L * (1 << (i + 1));
    }
  }

  private int scale = 3;

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testValuesWriteRead() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, scale, 64), allocator)) {

      try (Decimal64Vector oldConstructor = new Decimal64Vector("decimal", allocator, 18, scale)) {
        assertEquals(decimalVector.getField().getType(), oldConstructor.getField().getType());
      }

      decimalVector.allocateNew();
      BigDecimal[] values = new BigDecimal[intValues.length];
      for (int i = 0; i < intValues.length; i++) {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(intValues[i]), scale);
        values[i] = decimal;
        decimalVector.setSafe(i, decimal);
      }

      decimalVector.setValueCount(intValues.length);

      for (int i = 0; i < intValues.length; i++) {
        BigDecimal value = decimalVector.getObject(i);
        assertEquals(values[i], value, "unexpected data at index: " + i);
      }
    }
  }

  @Test
  public void testDecimal64DifferentScaleAndPrecision() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(4, 2, 64), allocator)) {
      decimalVector.allocateNew();

      // test Decimal64 with different scale
      {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(0), 3);
        UnsupportedOperationException ue =
            assertThrows(
                UnsupportedOperationException.class, () -> decimalVector.setSafe(0, decimal));
        assertEquals(
            "BigDecimal scale must equal that in the Arrow vector: 3 != 2", ue.getMessage());
      }

      // test BigDecimal with larger precision than initialized
      {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(12345), 2);
        UnsupportedOperationException ue =
            assertThrows(
                UnsupportedOperationException.class, () -> decimalVector.setSafe(0, decimal));
        assertEquals(
            "BigDecimal precision cannot be greater than that in the Arrow vector: 5 > 4",
            ue.getMessage());
      }
      decimalVector.setValueCount(1);
      assertTrue(decimalVector.isNull(0));
    }
  }

  @Test
  public void testWriteBigEndian() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 9, 64), allocator)) {
      decimalVector.allocateNew();
      BigDecimal decimal1 = new BigDecimal("123456789.000000000");
      BigDecimal decimal2 = new BigDecimal("11.123456789");
      BigDecimal decimal3 = new BigDecimal("1.000000000");
      BigDecimal decimal4 = new BigDecimal("0.111111111");
      BigDecimal decimal5 = new BigDecimal("987654321.123456789");
      BigDecimal decimal6 = new BigDecimal("-123456789.123456789");
      BigDecimal decimal7 = new BigDecimal("-1.000000001");
      BigDecimal decimal8 = new BigDecimal("55.343434343");

      byte[] decimalValue1 = decimal1.unscaledValue().toByteArray();
      byte[] decimalValue2 = decimal2.unscaledValue().toByteArray();
      byte[] decimalValue3 = decimal3.unscaledValue().toByteArray();
      byte[] decimalValue4 = decimal4.unscaledValue().toByteArray();
      byte[] decimalValue5 = decimal5.unscaledValue().toByteArray();
      byte[] decimalValue6 = decimal6.unscaledValue().toByteArray();
      byte[] decimalValue7 = decimal7.unscaledValue().toByteArray();
      byte[] decimalValue8 = decimal8.unscaledValue().toByteArray();

      decimalVector.setBigEndian(0, decimalValue1);
      decimalVector.setBigEndian(1, decimalValue2);
      decimalVector.setBigEndian(2, decimalValue3);
      decimalVector.setBigEndian(3, decimalValue4);
      decimalVector.setBigEndian(4, decimalValue5);
      decimalVector.setBigEndian(5, decimalValue6);
      decimalVector.setBigEndian(6, decimalValue7);
      decimalVector.setBigEndian(7, decimalValue8);

      decimalVector.setValueCount(8);
      assertEquals(8, decimalVector.getValueCount());
      assertEquals(decimal1, decimalVector.getObject(0));
      assertEquals(decimal2, decimalVector.getObject(1));
      assertEquals(decimal3, decimalVector.getObject(2));
      assertEquals(decimal4, decimalVector.getObject(3));
      assertEquals(decimal5, decimalVector.getObject(4));
      assertEquals(decimal6, decimalVector.getObject(5));
      assertEquals(decimal7, decimalVector.getObject(6));
      assertEquals(decimal8, decimalVector.getObject(7));
    }
  }

  @Test
  public void testLongReadWrite() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 0, 64), allocator)) {
      decimalVector.allocateNew();

      long[] longValues = {0L, -2L, 999999999999999999L, -999999999999999999L, 187L};

      for (int i = 0; i < longValues.length; ++i) {
        decimalVector.set(i, longValues[i]);
      }

      decimalVector.setValueCount(longValues.length);

      for (int i = 0; i < longValues.length; ++i) {
        assertEquals(new BigDecimal(longValues[i]), decimalVector.getObject(i));
      }

      decimalVector.set(0, Long.MAX_VALUE);
      decimalVector.set(1, Long.MIN_VALUE);
      decimalVector.setValueCount(2);
      assertThrows(ValidateUtil.ValidateException.class, decimalVector::validateScalars);
    }
  }

  @Test
  public void testHolderReaderUsesDecimalByteOrder() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 2, 64), allocator)) {
      decimalVector.allocateNew();
      decimalVector.set(0, new BigDecimal("1.23"));
      BigDecimal expected = new BigDecimal("1234567890123456.78");
      decimalVector.set(1, expected);
      decimalVector.setValueCount(2);

      NullableDecimal64Holder nullableHolder = new NullableDecimal64Holder();
      decimalVector.get(1, nullableHolder);
      assertEquals(
          expected, new NullableDecimal64HolderReaderImpl(nullableHolder).readBigDecimal());

      Decimal64Holder holder = new Decimal64Holder();
      holder.buffer = nullableHolder.buffer;
      holder.start = nullableHolder.start;
      holder.precision = nullableHolder.precision;
      holder.scale = nullableHolder.scale;
      assertEquals(expected, new Decimal64HolderReaderImpl(holder).readBigDecimal());
    }
  }

  @Test
  public void testBigDecimalReadWrite() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 9, 64), allocator)) {
      decimalVector.allocateNew();
      BigDecimal decimal1 = new BigDecimal("123456789.000000000");
      BigDecimal decimal2 = new BigDecimal("11.123456789");
      BigDecimal decimal3 = new BigDecimal("1.000000000");
      BigDecimal decimal4 = new BigDecimal("-0.111111111");
      BigDecimal decimal5 = new BigDecimal("-987654321.123456789");
      BigDecimal decimal6 = new BigDecimal("-2.222222222");
      BigDecimal decimal7 = new BigDecimal("7.666666667");
      BigDecimal decimal8 = new BigDecimal("121212121.343434343");

      decimalVector.set(0, decimal1);
      decimalVector.set(1, decimal2);
      decimalVector.set(2, decimal3);
      decimalVector.set(3, decimal4);
      decimalVector.set(4, decimal5);
      decimalVector.set(5, decimal6);
      decimalVector.set(6, decimal7);
      decimalVector.set(7, decimal8);

      decimalVector.setValueCount(8);
      assertEquals(8, decimalVector.getValueCount());
      assertEquals(decimal1, decimalVector.getObject(0));
      assertEquals(decimal2, decimalVector.getObject(1));
      assertEquals(decimal3, decimalVector.getObject(2));
      assertEquals(decimal4, decimalVector.getObject(3));
      assertEquals(decimal5, decimalVector.getObject(4));
      assertEquals(decimal6, decimalVector.getObject(5));
      assertEquals(decimal7, decimalVector.getObject(6));
      assertEquals(decimal8, decimalVector.getObject(7));
    }
  }

  /**
   * Test {@link Decimal64Vector#setBigEndian(int, byte[])} which takes BE layout input and stores
   * in native-endian (NE) layout. Cases to cover: input byte array in different lengths in range
   * [1-8] and negative values.
   */
  @Test
  public void decimalBE2NE() {
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 2, 64), allocator)) {
      decimalVector.allocateNew();

      BigInteger[] testBigInts =
          new BigInteger[] {
            new BigInteger("0"),
            new BigInteger("-1"),
            new BigInteger("23"),
            new BigInteger("234234"),
            new BigInteger("-234234234"),
            new BigInteger("234234234234"),
            new BigInteger("-56345345345345"),
            new BigInteger("999999999999999999"), // 18 nines, fits in 8 bytes
            new BigInteger("-999999999999999999"),
            new BigInteger("-345345"),
            new BigInteger("754533")
          };

      int insertionIdx = 0;
      insertionIdx++; // insert a null
      for (BigInteger val : testBigInts) {
        decimalVector.setBigEndian(insertionIdx++, val.toByteArray());
      }
      insertionIdx++; // insert a null
      // insert a zero length buffer
      decimalVector.setBigEndian(insertionIdx++, new byte[0]);

      // Try inserting a buffer larger than 8 bytes and expect a failure
      final int insertionIdxCapture = insertionIdx;
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> decimalVector.setBigEndian(insertionIdxCapture, new byte[9]));
      assertTrue(
          ex.getMessage().equals("Invalid decimal value length. Valid length in [0 - 8], got 9"));
      decimalVector.setValueCount(insertionIdx);

      // retrieve values and check if they are correct
      int outputIdx = 0;
      assertTrue(decimalVector.isNull(outputIdx++));
      for (BigInteger expected : testBigInts) {
        final BigDecimal actual = decimalVector.getObject(outputIdx++);
        assertEquals(expected, actual.unscaledValue());
      }
      assertTrue(decimalVector.isNull(outputIdx++));
      assertEquals(BigInteger.valueOf(0), decimalVector.getObject(outputIdx).unscaledValue());
    }
  }

  @Test
  public void setUsingArrowLongLEBytes() {
    try (Decimal64Vector decimalVector =
            TestUtils.newVector(
                Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 0, 64), allocator);
        ArrowBuf buf = allocator.buffer(16)) {
      decimalVector.allocateNew();

      long val = Long.MAX_VALUE;
      buf.setLong(0, val);
      decimalVector.setSafe(0, 0, buf, 8);

      val = Long.MIN_VALUE;
      buf.setLong(8, val);
      decimalVector.setSafe(1, 8, buf, 8);

      decimalVector.setValueCount(2);

      BigDecimal[] expectedValues =
          new BigDecimal[] {BigDecimal.valueOf(Long.MAX_VALUE), BigDecimal.valueOf(Long.MIN_VALUE)};
      for (int i = 0; i < 2; i++) {
        BigDecimal value = decimalVector.getObject(i);
        assertEquals(expectedValues[i], value);
      }
    }
  }

  /** Round-trip a Decimal64 vector through the Arrow IPC stream format. */
  @Test
  public void testIpcRoundtrip() throws Exception {
    Field field = new Field("decimal", FieldType.nullable(new ArrowType.Decimal(18, 4, 64)), null);
    Schema schema = new Schema(Collections.singletonList(field));
    byte[] serialized;

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      Decimal64Vector vector = (Decimal64Vector) root.getVector("decimal");
      vector.allocateNew();
      vector.set(0, new BigDecimal("12345.6789"));
      vector.setNull(1);
      vector.set(2, new BigDecimal("-98765.4321"));
      root.setRowCount(3);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      serialized = out.toByteArray();
    }

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(serialized), allocator)) {
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
      ArrowType.Decimal readType =
          (ArrowType.Decimal) readRoot.getSchema().getFields().get(0).getType();
      assertEquals(64, readType.getBitWidth());
      assertEquals(18, readType.getPrecision());
      assertEquals(4, readType.getScale());

      assertTrue(reader.loadNextBatch());
      Decimal64Vector vector = (Decimal64Vector) readRoot.getVector("decimal");
      assertEquals(3, vector.getValueCount());
      assertEquals(new BigDecimal("12345.6789"), vector.getObject(0));
      assertTrue(vector.isNull(1));
      assertEquals(new BigDecimal("-98765.4321"), vector.getObject(2));
    }
  }

  @Test
  public void testGetTransferPairWithField() {
    final Decimal64Vector fromVector = new Decimal64Vector("decimal", allocator, 10, scale);
    final TransferPair transferPair = fromVector.getTransferPair(fromVector.getField(), allocator);
    final Decimal64Vector toVector = (Decimal64Vector) transferPair.getTo();
    // Field inside a new vector created by reusing a field should be the same in memory as the
    // original field.
    assertSame(fromVector.getField(), toVector.getField());
    fromVector.close();
    toVector.close();
  }

  @Test
  public void testGetTransferPairWithoutField() {
    final Decimal64Vector fromVector = new Decimal64Vector("decimal", allocator, 10, scale);
    final TransferPair transferPair =
        fromVector.getTransferPair(fromVector.getField().getName(), allocator);
    final Decimal64Vector toVector = (Decimal64Vector) transferPair.getTo();
    // A new Field created inside a new vector should reuse the field type (should be the same in
    // memory as the original Field's field type).
    assertSame(fromVector.getField().getFieldType(), toVector.getField().getFieldType());
    fromVector.close();
    toVector.close();
  }

  @Test
  public void testValidateScalarsAtMaxPrecision() {
    // A value whose precision exactly equals the vector's precision must validate cleanly.
    // Regression: DecimalUtility.checkPrecisionAndScaleNoThrow used a strict '<' comparison, so a
    // value that set() accepts was wrongly rejected by validateScalars() at the precision boundary.
    try (Decimal64Vector decimalVector =
        TestUtils.newVector(
            Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 2, 64), allocator)) {
      decimalVector.allocateNew();
      decimalVector.set(0, new BigDecimal("9999999999999999.99")); // precision 18 == vector prec
      decimalVector.setValueCount(1);
      decimalVector.validateScalars(); // must not throw
    }
  }

  @Test
  public void testSetSafeRejectsTooLongLength() {
    // Regression: setSafe/setBigEndianSafe with length > TYPE_WIDTH used to write past the slot
    // before any validation, silently corrupting neighbouring values. They must now reject the
    // length up front without mutating the vector.
    try (Decimal64Vector decimalVector =
            TestUtils.newVector(
                Decimal64Vector.class, "decimal", new ArrowType.Decimal(18, 0, 64), allocator);
        ArrowBuf buf = allocator.buffer(16)) {
      decimalVector.allocateNew(2);
      decimalVector.set(1, 123L); // neighbour that must remain intact
      buf.setLong(0, 0x0102030405060708L);

      final int tooLong = Decimal64Vector.TYPE_WIDTH + 1;
      assertThrows(IllegalArgumentException.class, () -> decimalVector.setSafe(0, 0, buf, tooLong));
      assertThrows(
          IllegalArgumentException.class, () -> decimalVector.setBigEndianSafe(0, 0, buf, tooLong));
      assertThrows(IllegalArgumentException.class, () -> decimalVector.setSafe(0, 0, buf, 0));
      assertThrows(
          IllegalArgumentException.class, () -> decimalVector.setBigEndianSafe(0, 0, buf, 0));
      assertThrows(IllegalArgumentException.class, () -> decimalVector.setSafe(0, 0, buf, -1));
      assertThrows(
          IllegalArgumentException.class, () -> decimalVector.setBigEndianSafe(0, 0, buf, -1));

      decimalVector.setValueCount(2);
      assertTrue(decimalVector.isNull(0)); // rejected write left index 0 untouched
      assertEquals(new BigDecimal(123), decimalVector.getObject(1));
    }
  }
}
