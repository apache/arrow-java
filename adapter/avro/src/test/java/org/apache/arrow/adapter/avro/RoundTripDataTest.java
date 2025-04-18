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
package org.apache.arrow.adapter.avro;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RoundTripDataTest {

  @TempDir public static File TMP;

  private static AvroToArrowConfig basicConfig(BufferAllocator allocator) {
    return new AvroToArrowConfig(allocator, 1000, null, Collections.emptySet(), false);
  }

  private static VectorSchemaRoot readDataFile(
      Schema schema, File dataFile, BufferAllocator allocator) throws Exception {

    try (FileInputStream fis = new FileInputStream(dataFile)) {
      BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(fis, null);
      return AvroToArrow.avroToArrow(schema, decoder, basicConfig(allocator));
    }
  }

  private static void roundTripTest(
      VectorSchemaRoot root, BufferAllocator allocator, File dataFile, int rowCount)
      throws Exception {

    // Write an AVRO block using the producer classes
    try (FileOutputStream fos = new FileOutputStream(dataFile)) {
      BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
      CompositeAvroProducer producer =
          ArrowToAvroUtils.createCompositeProducer(root.getFieldVectors());
      for (int row = 0; row < rowCount; row++) {
        producer.produce(encoder);
      }
      encoder.flush();
    }

    // Generate AVRO schema
    Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());

    // Read back in and compare
    try (VectorSchemaRoot roundTrip = readDataFile(schema, dataFile, allocator)) {

      assertEquals(root.getSchema(), roundTrip.getSchema());
      assertEquals(rowCount, roundTrip.getRowCount());

      // Read and check values
      for (int row = 0; row < rowCount; row++) {
        assertEquals(root.getVector(0).getObject(row), roundTrip.getVector(0).getObject(row));
      }
    }
  }

  private static void roundTripByteArrayTest(
      VectorSchemaRoot root, BufferAllocator allocator, File dataFile, int rowCount)
      throws Exception {

    // Write an AVRO block using the producer classes
    try (FileOutputStream fos = new FileOutputStream(dataFile)) {
      BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
      CompositeAvroProducer producer =
          ArrowToAvroUtils.createCompositeProducer(root.getFieldVectors());
      for (int row = 0; row < rowCount; row++) {
        producer.produce(encoder);
      }
      encoder.flush();
    }

    // Generate AVRO schema
    Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());

    // Read back in and compare
    try (VectorSchemaRoot roundTrip = readDataFile(schema, dataFile, allocator)) {

      assertEquals(root.getSchema(), roundTrip.getSchema());
      assertEquals(rowCount, roundTrip.getRowCount());

      // Read and check values
      for (int row = 0; row < rowCount; row++) {
        byte[] rootBytes = (byte[]) root.getVector(0).getObject(row);
        byte[] roundTripBytes = (byte[]) roundTrip.getVector(0).getObject(row);
        assertArrayEquals(rootBytes, roundTripBytes);
      }
    }
  }

  // Data round trip for primitive types, nullable and non-nullable

  @Test
  public void testRoundTripNullColumn() throws Exception {

    // The current read implementation expects EOF, which never happens for a single null vector
    // Include a boolean vector with this test for now, so that EOF exception will be triggered

    // Field definition
    FieldType nullField = new FieldType(false, new ArrowType.Null(), null);
    FieldType booleanField = new FieldType(false, new ArrowType.Bool(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    NullVector nullVector = new NullVector(new Field("nullColumn", nullField, null));
    BitVector booleanVector = new BitVector(new Field("boolean", booleanField, null), allocator);

    int rowCount = 10;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(nullVector, booleanVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set all values to null
      for (int row = 0; row < rowCount; row++) {
        nullVector.setNull(row);
        booleanVector.set(row, 0);
      }

      File dataFile = new File(TMP, "testRoundTripNullColumn.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripBooleans() throws Exception {

    // Field definition
    FieldType booleanField = new FieldType(false, new ArrowType.Bool(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    BitVector booleanVector = new BitVector(new Field("boolean", booleanField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(booleanVector);
    int rowCount = 10;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < rowCount; row++) {
        booleanVector.set(row, row % 2 == 0 ? 1 : 0);
      }

      File dataFile = new File(TMP, "testRoundTripBooleans.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableBooleans() throws Exception {

    // Field definition
    FieldType booleanField = new FieldType(true, new ArrowType.Bool(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    BitVector booleanVector = new BitVector(new Field("boolean", booleanField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(booleanVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null value
      booleanVector.setNull(0);

      // False value
      booleanVector.set(1, 0);

      // True value
      booleanVector.set(2, 1);

      File dataFile = new File(TMP, "testRoundTripNullableBooleans.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripIntegers() throws Exception {

    // Field definitions
    FieldType int32Field = new FieldType(false, new ArrowType.Int(32, true), null);
    FieldType int64Field = new FieldType(false, new ArrowType.Int(64, true), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(int32Vector, int64Vector);

    int rowCount = 12;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < 10; row++) {
        int32Vector.set(row, 513 * row * (row % 2 == 0 ? 1 : -1));
        int64Vector.set(row, 3791L * row * (row % 2 == 0 ? 1 : -1));
      }

      // Min values
      int32Vector.set(10, Integer.MIN_VALUE);
      int64Vector.set(10, Long.MIN_VALUE);

      // Max values
      int32Vector.set(11, Integer.MAX_VALUE);
      int64Vector.set(11, Long.MAX_VALUE);

      File dataFile = new File(TMP, "testRoundTripIntegers.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableIntegers() throws Exception {

    // Field definitions
    FieldType int32Field = new FieldType(true, new ArrowType.Int(32, true), null);
    FieldType int64Field = new FieldType(true, new ArrowType.Int(64, true), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(int32Vector, int64Vector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null values
      int32Vector.setNull(0);
      int64Vector.setNull(0);

      // Zero values
      int32Vector.set(1, 0);
      int64Vector.set(1, 0);

      // Non-zero values
      int32Vector.set(2, Integer.MAX_VALUE);
      int64Vector.set(2, Long.MAX_VALUE);

      File dataFile = new File(TMP, "testRoundTripNullableIntegers.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripFloatingPoints() throws Exception {

    // Field definitions
    FieldType float32Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float4Vector float32Vector =
        new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector =
        new Float8Vector(new Field("float64", float64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(float32Vector, float64Vector);
    int rowCount = 15;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < 10; row++) {
        float32Vector.set(row, 37.6f * row * (row % 2 == 0 ? 1 : -1));
        float64Vector.set(row, 37.6d * row * (row % 2 == 0 ? 1 : -1));
      }

      float32Vector.set(10, Float.MIN_VALUE);
      float64Vector.set(10, Double.MIN_VALUE);

      float32Vector.set(11, Float.MAX_VALUE);
      float64Vector.set(11, Double.MAX_VALUE);

      float32Vector.set(12, Float.NaN);
      float64Vector.set(12, Double.NaN);

      float32Vector.set(13, Float.POSITIVE_INFINITY);
      float64Vector.set(13, Double.POSITIVE_INFINITY);

      float32Vector.set(14, Float.NEGATIVE_INFINITY);
      float64Vector.set(14, Double.NEGATIVE_INFINITY);

      File dataFile = new File(TMP, "testRoundTripFloatingPoints.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableFloatingPoints() throws Exception {

    // Field definitions
    FieldType float32Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float4Vector float32Vector =
        new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector =
        new Float8Vector(new Field("float64", float64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(float32Vector, float64Vector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null values
      float32Vector.setNull(0);
      float64Vector.setNull(0);

      // Zero values
      float32Vector.set(1, 0.0f);
      float64Vector.set(1, 0.0);

      // Non-zero values
      float32Vector.set(2, 1.0f);
      float64Vector.set(2, 1.0);

      File dataFile = new File(TMP, "testRoundTripNullableFloatingPoints.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripStrings() throws Exception {

    // Field definition
    FieldType stringField = new FieldType(false, new ArrowType.Utf8(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarCharVector stringVector =
        new VarCharVector(new Field("string", stringField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(stringVector);
    int rowCount = 5;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      stringVector.setSafe(0, "Hello world!".getBytes());
      stringVector.setSafe(1, "<%**\r\n\t\\abc\0$$>".getBytes());
      stringVector.setSafe(2, "你好世界".getBytes());
      stringVector.setSafe(3, "مرحبا بالعالم".getBytes());
      stringVector.setSafe(4, "(P ∧ P ⇒ Q) ⇒ Q".getBytes());

      File dataFile = new File(TMP, "testRoundTripStrings.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableStrings() throws Exception {

    // Field definition
    FieldType stringField = new FieldType(true, new ArrowType.Utf8(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarCharVector stringVector =
        new VarCharVector(new Field("string", stringField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(stringVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      stringVector.setNull(0);
      stringVector.setSafe(1, "".getBytes());
      stringVector.setSafe(2, "not empty".getBytes());

      File dataFile = new File(TMP, "testRoundTripNullableStrings.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripBinary() throws Exception {

    // Field definition
    FieldType binaryField = new FieldType(false, new ArrowType.Binary(), null);
    FieldType fixedField = new FieldType(false, new ArrowType.FixedSizeBinary(5), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarBinaryVector binaryVector =
        new VarBinaryVector(new Field("binary", binaryField, null), allocator);
    FixedSizeBinaryVector fixedVector =
        new FixedSizeBinaryVector(new Field("fixed", fixedField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(binaryVector, fixedVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      binaryVector.setSafe(0, new byte[] {1, 2, 3});
      binaryVector.setSafe(1, new byte[] {4, 5, 6, 7});
      binaryVector.setSafe(2, new byte[] {8, 9});

      fixedVector.setSafe(0, new byte[] {1, 2, 3, 4, 5});
      fixedVector.setSafe(1, new byte[] {4, 5, 6, 7, 8, 9});
      fixedVector.setSafe(2, new byte[] {8, 9, 10, 11, 12});

      File dataFile = new File(TMP, "testRoundTripBinary.avro");

      roundTripByteArrayTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableBinary() throws Exception {

    // Field definition
    FieldType binaryField = new FieldType(true, new ArrowType.Binary(), null);
    FieldType fixedField = new FieldType(true, new ArrowType.FixedSizeBinary(5), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarBinaryVector binaryVector =
        new VarBinaryVector(new Field("binary", binaryField, null), allocator);
    FixedSizeBinaryVector fixedVector =
        new FixedSizeBinaryVector(new Field("fixed", fixedField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(binaryVector, fixedVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      binaryVector.setNull(0);
      binaryVector.setSafe(1, new byte[] {});
      binaryVector.setSafe(2, new byte[] {10, 11, 12});

      fixedVector.setNull(0);
      fixedVector.setSafe(1, new byte[] {0, 0, 0, 0, 0});
      fixedVector.setSafe(2, new byte[] {10, 11, 12, 13, 14});

      File dataFile = new File(TMP, "testRoundTripNullableBinary.avro");

      roundTripByteArrayTest(root, allocator, dataFile, rowCount);
    }
  }

  // Data round trip for logical types, nullable and non-nullable

  @Test
  public void testRoundTripDecimals() throws Exception {

    // Field definitions
    FieldType decimal128Field1 = new FieldType(false, new ArrowType.Decimal(38, 10, 128), null);
    FieldType decimal128Field2 = new FieldType(false, new ArrowType.Decimal(38, 5, 128), null);
    FieldType decimal256Field1 = new FieldType(false, new ArrowType.Decimal(76, 20, 256), null);
    FieldType decimal256Field2 = new FieldType(false, new ArrowType.Decimal(76, 10, 256), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    DecimalVector decimal128Vector1 =
        new DecimalVector(new Field("decimal128_1", decimal128Field1, null), allocator);
    DecimalVector decimal128Vector2 =
        new DecimalVector(new Field("decimal128_2", decimal128Field2, null), allocator);
    Decimal256Vector decimal256Vector1 =
        new Decimal256Vector(new Field("decimal256_1", decimal256Field1, null), allocator);
    Decimal256Vector decimal256Vector2 =
        new Decimal256Vector(new Field("decimal256_2", decimal256Field2, null), allocator);

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(decimal128Vector1, decimal128Vector2, decimal256Vector1, decimal256Vector2);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      decimal128Vector1.setSafe(
          0, new BigDecimal("12345.67890").setScale(10, RoundingMode.UNNECESSARY));
      decimal128Vector1.setSafe(
          1, new BigDecimal("-98765.43210").setScale(10, RoundingMode.UNNECESSARY));
      decimal128Vector1.setSafe(
          2, new BigDecimal("54321.09876").setScale(10, RoundingMode.UNNECESSARY));

      decimal128Vector2.setSafe(
          0, new BigDecimal("12345.67890").setScale(5, RoundingMode.UNNECESSARY));
      decimal128Vector2.setSafe(
          1, new BigDecimal("-98765.43210").setScale(5, RoundingMode.UNNECESSARY));
      decimal128Vector2.setSafe(
          2, new BigDecimal("54321.09876").setScale(5, RoundingMode.UNNECESSARY));

      decimal256Vector1.setSafe(
          0,
          new BigDecimal("12345678901234567890.12345678901234567890")
              .setScale(20, RoundingMode.UNNECESSARY));
      decimal256Vector1.setSafe(
          1,
          new BigDecimal("-98765432109876543210.98765432109876543210")
              .setScale(20, RoundingMode.UNNECESSARY));
      decimal256Vector1.setSafe(
          2,
          new BigDecimal("54321098765432109876.54321098765432109876")
              .setScale(20, RoundingMode.UNNECESSARY));

      decimal256Vector2.setSafe(
          0,
          new BigDecimal("12345678901234567890.1234567890").setScale(10, RoundingMode.UNNECESSARY));
      decimal256Vector2.setSafe(
          1,
          new BigDecimal("-98765432109876543210.9876543210")
              .setScale(10, RoundingMode.UNNECESSARY));
      decimal256Vector2.setSafe(
          2,
          new BigDecimal("54321098765432109876.5432109876").setScale(10, RoundingMode.UNNECESSARY));

      File dataFile = new File(TMP, "testRoundTripDecimals.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableDecimals() throws Exception {

    // Field definitions
    FieldType decimal128Field1 = new FieldType(true, new ArrowType.Decimal(38, 10, 128), null);
    FieldType decimal128Field2 = new FieldType(true, new ArrowType.Decimal(38, 5, 128), null);
    FieldType decimal256Field1 = new FieldType(true, new ArrowType.Decimal(76, 20, 256), null);
    FieldType decimal256Field2 = new FieldType(true, new ArrowType.Decimal(76, 10, 256), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    DecimalVector decimal128Vector1 =
        new DecimalVector(new Field("decimal128_1", decimal128Field1, null), allocator);
    DecimalVector decimal128Vector2 =
        new DecimalVector(new Field("decimal128_2", decimal128Field2, null), allocator);
    Decimal256Vector decimal256Vector1 =
        new Decimal256Vector(new Field("decimal256_1", decimal256Field1, null), allocator);
    Decimal256Vector decimal256Vector2 =
        new Decimal256Vector(new Field("decimal256_2", decimal256Field2, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(decimal128Vector1, decimal128Vector2, decimal256Vector1, decimal256Vector2);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      decimal128Vector1.setNull(0);
      decimal128Vector1.setSafe(1, BigDecimal.ZERO.setScale(10, RoundingMode.UNNECESSARY));
      decimal128Vector1.setSafe(
          2, new BigDecimal("12345.67890").setScale(10, RoundingMode.UNNECESSARY));

      decimal128Vector2.setNull(0);
      decimal128Vector2.setSafe(1, BigDecimal.ZERO.setScale(5, RoundingMode.UNNECESSARY));
      decimal128Vector2.setSafe(
          2, new BigDecimal("98765.43210").setScale(5, RoundingMode.UNNECESSARY));

      decimal256Vector1.setNull(0);
      decimal256Vector1.setSafe(1, BigDecimal.ZERO.setScale(20, RoundingMode.UNNECESSARY));
      decimal256Vector1.setSafe(
          2,
          new BigDecimal("12345678901234567890.12345678901234567890")
              .setScale(20, RoundingMode.UNNECESSARY));

      decimal256Vector2.setNull(0);
      decimal256Vector2.setSafe(1, BigDecimal.ZERO.setScale(10, RoundingMode.UNNECESSARY));
      decimal256Vector2.setSafe(
          2,
          new BigDecimal("98765432109876543210.9876543210").setScale(10, RoundingMode.UNNECESSARY));

      File dataFile = new File(TMP, "testRoundTripNullableDecimals.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripDates() throws Exception {

    // Field definitions
    FieldType dateDayField = new FieldType(false, new ArrowType.Date(DateUnit.DAY), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    DateDayVector dateDayVector =
        new DateDayVector(new Field("dateDay", dateDayField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(dateDayVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      dateDayVector.setSafe(0, (int) LocalDate.now().toEpochDay());
      dateDayVector.setSafe(1, (int) LocalDate.now().toEpochDay() + 1);
      dateDayVector.setSafe(2, (int) LocalDate.now().toEpochDay() + 2);

      File dataFile = new File(TMP, "testRoundTripDates.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableDates() throws Exception {

    // Field definitions
    FieldType dateDayField = new FieldType(true, new ArrowType.Date(DateUnit.DAY), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    DateDayVector dateDayVector =
        new DateDayVector(new Field("dateDay", dateDayField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(dateDayVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      dateDayVector.setNull(0);
      dateDayVector.setSafe(1, 0);
      dateDayVector.setSafe(2, (int) LocalDate.now().toEpochDay());

      File dataFile = new File(TMP, "testRoundTripNullableDates.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripTimes() throws Exception {

    // Field definitions
    FieldType timeMillisField =
        new FieldType(false, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
    FieldType timeMicrosField =
        new FieldType(false, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeMilliVector timeMillisVector =
        new TimeMilliVector(new Field("timeMillis", timeMillisField, null), allocator);
    TimeMicroVector timeMicrosVector =
        new TimeMicroVector(new Field("timeMicros", timeMicrosField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(timeMillisVector, timeMicrosVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timeMillisVector.setSafe(
          0, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000));
      timeMillisVector.setSafe(
          1, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000) - 1000);
      timeMillisVector.setSafe(
          2, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000) - 2000);

      timeMicrosVector.setSafe(0, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000);
      timeMicrosVector.setSafe(1, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000 - 1000000);
      timeMicrosVector.setSafe(2, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000 - 2000000);

      File dataFile = new File(TMP, "testRoundTripTimes.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableTimes() throws Exception {

    // Field definitions
    FieldType timeMillisField =
        new FieldType(true, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
    FieldType timeMicrosField =
        new FieldType(true, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeMilliVector timeMillisVector =
        new TimeMilliVector(new Field("timeMillis", timeMillisField, null), allocator);
    TimeMicroVector timeMicrosVector =
        new TimeMicroVector(new Field("timeMicros", timeMicrosField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(timeMillisVector, timeMicrosVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timeMillisVector.setNull(0);
      timeMillisVector.setSafe(1, 0);
      timeMillisVector.setSafe(
          2, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000));

      timeMicrosVector.setNull(0);
      timeMicrosVector.setSafe(1, 0);
      timeMicrosVector.setSafe(2, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000);

      File dataFile = new File(TMP, "testRoundTripNullableTimes.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripZoneAwareTimestamps() throws Exception {

    // Field definitions
    FieldType timestampMillisField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null);
    FieldType timestampMicrosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null);
    FieldType timestampNanosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampMilliTZVector timestampMillisVector =
        new TimeStampMilliTZVector(
            new Field("timestampMillis", timestampMillisField, null), allocator);
    TimeStampMicroTZVector timestampMicrosVector =
        new TimeStampMicroTZVector(
            new Field("timestampMicros", timestampMicrosField, null), allocator);
    TimeStampNanoTZVector timestampNanosVector =
        new TimeStampNanoTZVector(
            new Field("timestampNanos", timestampNanosField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(timestampMillisVector, timestampMicrosVector, timestampNanosVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampMillisVector.setSafe(0, (int) Instant.now().toEpochMilli());
      timestampMillisVector.setSafe(1, (int) Instant.now().toEpochMilli() - 1000);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli() - 2000);

      timestampMicrosVector.setSafe(0, Instant.now().toEpochMilli() * 1000);
      timestampMicrosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000);
      timestampMicrosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000);

      timestampNanosVector.setSafe(0, Instant.now().toEpochMilli() * 1000000);
      timestampNanosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000000);
      timestampNanosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000000);

      File dataFile = new File(TMP, "testRoundTripZoneAwareTimestamps.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableZoneAwareTimestamps() throws Exception {

    // Field definitions
    FieldType timestampMillisField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null);
    FieldType timestampMicrosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null);
    FieldType timestampNanosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampMilliTZVector timestampMillisVector =
        new TimeStampMilliTZVector(
            new Field("timestampMillis", timestampMillisField, null), allocator);
    TimeStampMicroTZVector timestampMicrosVector =
        new TimeStampMicroTZVector(
            new Field("timestampMicros", timestampMicrosField, null), allocator);
    TimeStampNanoTZVector timestampNanosVector =
        new TimeStampNanoTZVector(
            new Field("timestampNanos", timestampNanosField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(timestampMillisVector, timestampMicrosVector, timestampNanosVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampMillisVector.setNull(0);
      timestampMillisVector.setSafe(1, 0);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli());

      timestampMicrosVector.setNull(0);
      timestampMicrosVector.setSafe(1, 0);
      timestampMicrosVector.setSafe(2, Instant.now().toEpochMilli() * 1000);

      timestampNanosVector.setNull(0);
      timestampNanosVector.setSafe(1, 0);
      timestampNanosVector.setSafe(2, Instant.now().toEpochMilli() * 1000000);

      File dataFile = new File(TMP, "testRoundTripNullableZoneAwareTimestamps.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripLocalTimestamps() throws Exception {

    // Field definitions
    FieldType timestampMillisField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null);
    FieldType timestampMicrosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null);
    FieldType timestampNanosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampMilliVector timestampMillisVector =
        new TimeStampMilliVector(
            new Field("timestampMillis", timestampMillisField, null), allocator);
    TimeStampMicroVector timestampMicrosVector =
        new TimeStampMicroVector(
            new Field("timestampMicros", timestampMicrosField, null), allocator);
    TimeStampNanoVector timestampNanosVector =
        new TimeStampNanoVector(new Field("timestampNanos", timestampNanosField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(timestampMillisVector, timestampMicrosVector, timestampNanosVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampMillisVector.setSafe(0, (int) Instant.now().toEpochMilli());
      timestampMillisVector.setSafe(1, (int) Instant.now().toEpochMilli() - 1000);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli() - 2000);

      timestampMicrosVector.setSafe(0, Instant.now().toEpochMilli() * 1000);
      timestampMicrosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000);
      timestampMicrosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000);

      timestampNanosVector.setSafe(0, Instant.now().toEpochMilli() * 1000000);
      timestampNanosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000000);
      timestampNanosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000000);

      File dataFile = new File(TMP, "testRoundTripLocalTimestamps.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableLocalTimestamps() throws Exception {

    // Field definitions
    FieldType timestampMillisField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null);
    FieldType timestampMicrosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null);
    FieldType timestampNanosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampMilliVector timestampMillisVector =
        new TimeStampMilliVector(
            new Field("timestampMillis", timestampMillisField, null), allocator);
    TimeStampMicroVector timestampMicrosVector =
        new TimeStampMicroVector(
            new Field("timestampMicros", timestampMicrosField, null), allocator);
    TimeStampNanoVector timestampNanosVector =
        new TimeStampNanoVector(new Field("timestampNanos", timestampNanosField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(timestampMillisVector, timestampMicrosVector, timestampNanosVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampMillisVector.setNull(0);
      timestampMillisVector.setSafe(1, 0);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli());

      timestampMicrosVector.setNull(0);
      timestampMicrosVector.setSafe(1, 0);
      timestampMicrosVector.setSafe(2, Instant.now().toEpochMilli() * 1000);

      timestampNanosVector.setNull(0);
      timestampNanosVector.setSafe(1, 0);
      timestampNanosVector.setSafe(2, Instant.now().toEpochMilli() * 1000000);

      File dataFile = new File(TMP, "testRoundTripNullableLocalTimestamps.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  // Data round trip for containers of primitive and logical types, nullable and non-nullable

  @Test
  public void testRoundTripLists() throws Exception {

    // Field definitions
    FieldType intListField = new FieldType(false, new ArrowType.List(), null);
    FieldType stringListField = new FieldType(false, new ArrowType.List(), null);
    FieldType dateListField = new FieldType(false, new ArrowType.List(), null);

    Field intField = new Field("item", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field stringField = new Field("item", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field dateField =
        new Field("item", FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    ListVector intListVector = new ListVector("intList", allocator, intListField, null);
    ListVector stringListVector = new ListVector("stringList", allocator, stringListField, null);
    ListVector dateListVector = new ListVector("dateList", allocator, dateListField, null);

    intListVector.initializeChildrenFromFields(Arrays.asList(intField));
    stringListVector.initializeChildrenFromFields(Arrays.asList(stringField));
    dateListVector.initializeChildrenFromFields(Arrays.asList(dateField));

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(intListVector, stringListVector, dateListVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      FieldWriter intListWriter = intListVector.getWriter();
      FieldWriter stringListWriter = stringListVector.getWriter();
      FieldWriter dateListWriter = dateListVector.getWriter();

      // Set test data for intList
      for (int i = 0; i < rowCount; i++) {
        intListWriter.startList();
        for (int j = 0; j < 5 - i; j++) {
          intListWriter.writeInt(j);
        }
        intListWriter.endList();
      }

      // Set test data for stringList
      for (int i = 0; i < rowCount; i++) {
        stringListWriter.startList();
        for (int j = 0; j < 5 - i; j++) {
          stringListWriter.writeVarChar("string" + j);
        }
        stringListWriter.endList();
      }

      // Set test data for dateList
      for (int i = 0; i < rowCount; i++) {
        dateListWriter.startList();
        for (int j = 0; j < 5 - i; j++) {
          dateListWriter.writeDateDay((int) LocalDate.now().plusDays(j).toEpochDay());
        }
        dateListWriter.endList();
      }

      // Update count for the vectors
      intListVector.setValueCount(rowCount);
      stringListVector.setValueCount(rowCount);
      dateListVector.setValueCount(rowCount);

      File dataFile = new File(TMP, "testRoundTripLists.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableLists() throws Exception {

    // Field definitions
    FieldType nullListType = new FieldType(true, new ArrowType.List(), null);
    FieldType nonNullListType = new FieldType(false, new ArrowType.List(), null);

    Field nullFieldType = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field nonNullFieldType =
        new Field("item", FieldType.notNullable(new ArrowType.Int(32, true)), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    ListVector nullEntriesVector =
        new ListVector("nullEntriesVector", allocator, nonNullListType, null);
    ListVector nullListVector = new ListVector("nullListVector", allocator, nullListType, null);
    ListVector nullBothVector = new ListVector("nullBothVector", allocator, nullListType, null);

    nullEntriesVector.initializeChildrenFromFields(Arrays.asList(nullFieldType));
    nullListVector.initializeChildrenFromFields(Arrays.asList(nonNullFieldType));
    nullBothVector.initializeChildrenFromFields(Arrays.asList(nullFieldType));

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(nullEntriesVector, nullListVector, nullBothVector);
    int rowCount = 4;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data for nullEntriesVector
      FieldWriter nullEntriesWriter = nullEntriesVector.getWriter();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeNull();
      nullEntriesWriter.integer().writeNull();
      nullEntriesWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(0);
      nullEntriesWriter.integer().writeInt(0);
      nullEntriesWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(123);
      nullEntriesWriter.integer().writeInt(456);
      nullEntriesWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(789);
      nullEntriesWriter.integer().writeInt(789);
      nullEntriesWriter.endList();

      // Set test data for nullListVector
      FieldWriter nullListWriter = nullListVector.getWriter();
      nullListWriter.writeNull();
      nullListWriter.setPosition(1); // writeNull() does not inc. idx() on list vector
      nullListWriter.startList();
      nullListWriter.integer().writeInt(0);
      nullListWriter.integer().writeInt(0);
      nullListWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(123);
      nullEntriesWriter.integer().writeInt(456);
      nullEntriesWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(789);
      nullEntriesWriter.integer().writeInt(789);
      nullEntriesWriter.endList();

      // Set test data for nullBothVector
      FieldWriter nullBothWriter = nullBothVector.getWriter();
      nullBothWriter.writeNull();
      nullBothWriter.setPosition(1);
      nullBothWriter.startList();
      nullBothWriter.integer().writeNull();
      nullBothWriter.integer().writeNull();
      nullBothWriter.endList();
      nullListWriter.startList();
      nullListWriter.integer().writeInt(0);
      nullListWriter.integer().writeInt(0);
      nullListWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(123);
      nullEntriesWriter.integer().writeInt(456);
      nullEntriesWriter.endList();

      // Update count for the vectors
      nullListVector.setValueCount(4);
      nullEntriesVector.setValueCount(4);
      nullBothVector.setValueCount(4);

      File dataFile = new File(TMP, "testRoundTripNullableLists.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripMap() throws Exception {

    // Field definitions
    FieldType intMapField = new FieldType(false, new ArrowType.Map(false), null);
    FieldType stringMapField = new FieldType(false, new ArrowType.Map(false), null);
    FieldType dateMapField = new FieldType(false, new ArrowType.Map(false), null);

    Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field intField = new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field stringField = new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field dateField =
        new Field("value", FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null);

    Field intEntryField =
        new Field(
            "entries",
            FieldType.notNullable(new ArrowType.Struct()),
            Arrays.asList(keyField, intField));
    Field stringEntryField =
        new Field(
            "entries",
            FieldType.notNullable(new ArrowType.Struct()),
            Arrays.asList(keyField, stringField));
    Field dateEntryField =
        new Field(
            "entries",
            FieldType.notNullable(new ArrowType.Struct()),
            Arrays.asList(keyField, dateField));

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    MapVector intMapVector = new MapVector("intMap", allocator, intMapField, null);
    MapVector stringMapVector = new MapVector("stringMap", allocator, stringMapField, null);
    MapVector dateMapVector = new MapVector("dateMap", allocator, dateMapField, null);

    intMapVector.initializeChildrenFromFields(Arrays.asList(intEntryField));
    stringMapVector.initializeChildrenFromFields(Arrays.asList(stringEntryField));
    dateMapVector.initializeChildrenFromFields(Arrays.asList(dateEntryField));

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(intMapVector, stringMapVector, dateMapVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Total number of entries that will be writen to each vector
      int entryCount = 5 + 4 + 3;

      // Set test data for intList
      BaseWriter.MapWriter writer = intMapVector.getWriter();
      for (int i = 0; i < rowCount; i++) {
        writer.startMap();
        for (int j = 0; j < 5 - i; j++) {
          writer.startEntry();
          writer.key().varChar().writeVarChar("key" + j);
          writer.value().integer().writeInt(j);
          writer.endEntry();
        }
        writer.endMap();
      }

      // Update count for data vector (map writer does not do this)
      intMapVector.getDataVector().setValueCount(entryCount);

      // Set test data for stringList
      BaseWriter.MapWriter stringWriter = stringMapVector.getWriter();
      for (int i = 0; i < rowCount; i++) {
        stringWriter.startMap();
        for (int j = 0; j < 5 - i; j++) {
          stringWriter.startEntry();
          stringWriter.key().varChar().writeVarChar("key" + j);
          stringWriter.value().varChar().writeVarChar("string" + j);
          stringWriter.endEntry();
        }
        stringWriter.endMap();
      }

      // Update count for the vectors
      intMapVector.setValueCount(rowCount);
      stringMapVector.setValueCount(rowCount);
      dateMapVector.setValueCount(rowCount);

      // Update count for data vector (map writer does not do this)
      stringMapVector.getDataVector().setValueCount(entryCount);

      // Set test data for dateList
      BaseWriter.MapWriter dateWriter = dateMapVector.getWriter();
      for (int i = 0; i < rowCount; i++) {
        dateWriter.startMap();
        for (int j = 0; j < 5 - i; j++) {
          dateWriter.startEntry();
          dateWriter.key().varChar().writeVarChar("key" + j);
          dateWriter.value().dateDay().writeDateDay((int) LocalDate.now().plusDays(j).toEpochDay());
          dateWriter.endEntry();
        }
        dateWriter.endMap();
      }

      // Update count for data vector (map writer does not do this)
      dateMapVector.getDataVector().setValueCount(entryCount);

      File dataFile = new File(TMP, "testRoundTripMap.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableMap() throws Exception {

    // Field definitions
    FieldType nullMapType = new FieldType(true, new ArrowType.Map(false), null);
    FieldType nonNullMapType = new FieldType(false, new ArrowType.Map(false), null);

    Field keyField = new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field nullFieldType = new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field nonNullFieldType =
        new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field nullEntryField =
        new Field(
            "entries",
            FieldType.notNullable(new ArrowType.Struct()),
            Arrays.asList(keyField, nullFieldType));
    Field nonNullEntryField =
        new Field(
            "entries",
            FieldType.notNullable(new ArrowType.Struct()),
            Arrays.asList(keyField, nonNullFieldType));

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    MapVector nullEntriesVector =
        new MapVector("nullEntriesVector", allocator, nonNullMapType, null);
    MapVector nullMapVector = new MapVector("nullMapVector", allocator, nullMapType, null);
    MapVector nullBothVector = new MapVector("nullBothVector", allocator, nullMapType, null);

    nullEntriesVector.initializeChildrenFromFields(Arrays.asList(nullEntryField));
    nullMapVector.initializeChildrenFromFields(Arrays.asList(nonNullEntryField));
    nullBothVector.initializeChildrenFromFields(Arrays.asList(nullEntryField));

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(nullEntriesVector, nullMapVector, nullBothVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data for intList
      BaseWriter.MapWriter writer = nullEntriesVector.getWriter();
      writer.startMap();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key0");
      writer.value().integer().writeNull();
      writer.endEntry();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key1");
      writer.value().integer().writeNull();
      writer.endEntry();
      writer.endMap();
      writer.startMap();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key2");
      writer.value().integer().writeInt(0);
      writer.endEntry();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key3");
      writer.value().integer().writeInt(0);
      writer.endEntry();
      writer.endMap();
      writer.startMap();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key4");
      writer.value().integer().writeInt(123);
      writer.endEntry();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key5");
      writer.value().integer().writeInt(456);
      writer.endEntry();
      writer.endMap();

      // Set test data for stringList
      BaseWriter.MapWriter nullMapWriter = nullMapVector.getWriter();
      nullMapWriter.writeNull();
      nullMapWriter.setPosition(1); // writeNull() does not inc. idx() on map (list) vector
      nullMapWriter.startMap();
      nullMapWriter.startEntry();
      nullMapWriter.key().varChar().writeVarChar("key2");
      nullMapWriter.value().integer().writeInt(0);
      nullMapWriter.endEntry();
      writer.startMap();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key3");
      writer.value().integer().writeInt(0);
      writer.endEntry();
      nullMapWriter.endMap();
      nullMapWriter.startMap();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key4");
      writer.value().integer().writeInt(123);
      writer.endEntry();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key5");
      writer.value().integer().writeInt(456);
      writer.endEntry();
      nullMapWriter.endMap();

      // Set test data for dateList
      BaseWriter.MapWriter nullBothWriter = nullBothVector.getWriter();
      nullBothWriter.writeNull();
      nullBothWriter.setPosition(1);
      nullBothWriter.startMap();
      nullBothWriter.startEntry();
      nullBothWriter.key().varChar().writeVarChar("key2");
      nullBothWriter.value().integer().writeNull();
      nullBothWriter.endEntry();
      nullBothWriter.startEntry();
      nullBothWriter.key().varChar().writeVarChar("key3");
      nullBothWriter.value().integer().writeNull();
      nullBothWriter.endEntry();
      nullBothWriter.endMap();
      nullBothWriter.startMap();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key4");
      writer.value().integer().writeInt(123);
      writer.endEntry();
      writer.startEntry();
      writer.key().varChar().writeVarChar("key5");
      writer.value().integer().writeInt(456);
      writer.endEntry();
      nullBothWriter.endMap();

      // Update count for the vectors
      nullEntriesVector.setValueCount(3);
      nullMapVector.setValueCount(3);
      nullBothVector.setValueCount(3);

      File dataFile = new File(TMP, "testRoundTripNullableMap.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripStruct() throws Exception {

    // Field definitions
    FieldType structFieldType = new FieldType(false, new ArrowType.Struct(), null);
    Field intField =
        new Field("intField", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field stringField = new Field("stringField", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field dateField =
        new Field("dateField", FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    StructVector structVector = new StructVector("struct", allocator, structFieldType, null);
    structVector.initializeChildrenFromFields(Arrays.asList(intField, stringField, dateField));

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(structVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      BaseWriter.StructWriter structWriter = structVector.getWriter();

      for (int i = 0; i < rowCount; i++) {
        structWriter.start();
        structWriter.integer("intField").writeInt(i);
        structWriter.varChar("stringField").writeVarChar("string" + i);
        structWriter.dateDay("dateField").writeDateDay((int) LocalDate.now().toEpochDay() + i);
        structWriter.end();
      }

      File dataFile = new File(TMP, "testRoundTripStruct.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableStructs() throws Exception {

    // Field definitions
    FieldType structFieldType = new FieldType(false, new ArrowType.Struct(), null);
    FieldType nullableStructFieldType = new FieldType(true, new ArrowType.Struct(), null);
    Field intField =
        new Field("intField", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field nullableIntField =
        new Field("nullableIntField", FieldType.nullable(new ArrowType.Int(32, true)), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    StructVector structVector = new StructVector("struct", allocator, structFieldType, null);
    StructVector nullableStructVector =
        new StructVector("nullableStruct", allocator, nullableStructFieldType, null);
    structVector.initializeChildrenFromFields(Arrays.asList(intField, nullableIntField));
    nullableStructVector.initializeChildrenFromFields(Arrays.asList(intField, nullableIntField));

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(structVector, nullableStructVector);
    int rowCount = 4;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data for structVector
      BaseWriter.StructWriter structWriter = structVector.getWriter();
      for (int i = 0; i < rowCount; i++) {
        structWriter.setPosition(i);
        structWriter.start();
        structWriter.integer("intField").writeInt(i);
        if (i % 2 == 0) {
          structWriter.integer("nullableIntField").writeInt(i * 10);
        } else {
          structWriter.integer("nullableIntField").writeNull();
        }
        structWriter.end();
      }

      // Set test data for nullableStructVector
      BaseWriter.StructWriter nullableStructWriter = nullableStructVector.getWriter();
      for (int i = 0; i < rowCount; i++) {
        nullableStructWriter.setPosition(i);
        if (i >= 2) {
          nullableStructWriter.start();
          nullableStructWriter.integer("intField").writeInt(i);
          if (i % 2 == 0) {
            nullableStructWriter.integer("nullableIntField").writeInt(i * 10);
          } else {
            nullableStructWriter.integer("nullableIntField").writeNull();
          }
          nullableStructWriter.end();
        } else {
          nullableStructWriter.writeNull();
        }
      }

      // Update count for the vector
      structVector.setValueCount(rowCount);
      nullableStructVector.setValueCount(rowCount);

      File dataFile = new File(TMP, "testRoundTripNullableStructs.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }
}
