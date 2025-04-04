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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
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
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ArrowToAvroDataTest {

  @TempDir public static File TMP;

  // Data production for primitive types, nullable and non-nullable

  @Test
  public void testWriteNullColumn() throws Exception {

    // Field definition
    FieldType nullField = new FieldType(false, new ArrowType.Null(), null);

    // Create empty vector
    NullVector nullVector = new NullVector(new Field("nullColumn", nullField, null));

    int rowCount = 10;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(nullVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set all values to null
      for (int row = 0; row < rowCount; row++) {
        nullVector.setNull(row);
      }

      File dataFile = new File(TMP, "testWriteNullColumn.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertNull(record.get("nullColumn"));
        }
      }
    }
  }

  @Test
  public void testWriteBooleans() throws Exception {

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

      File dataFile = new File(TMP, "testWriteBooleans.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(booleanVector.get(row) == 1, record.get("boolean"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableBooleans() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableBooleans.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("boolean"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(booleanVector.get(row) == 1, record.get("boolean"));
        }
      }
    }
  }

  @Test
  public void testWriteIntegers() throws Exception {

    // Field definitions
    FieldType int8Field = new FieldType(false, new ArrowType.Int(8, true), null);
    FieldType int16Field = new FieldType(false, new ArrowType.Int(16, true), null);
    FieldType int32Field = new FieldType(false, new ArrowType.Int(32, true), null);
    FieldType int64Field = new FieldType(false, new ArrowType.Int(64, true), null);
    FieldType uint8Field = new FieldType(false, new ArrowType.Int(8, false), null);
    FieldType uint16Field = new FieldType(false, new ArrowType.Int(16, false), null);
    FieldType uint32Field = new FieldType(false, new ArrowType.Int(32, false), null);
    FieldType uint64Field = new FieldType(false, new ArrowType.Int(64, false), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TinyIntVector int8Vector = new TinyIntVector(new Field("int8", int8Field, null), allocator);
    SmallIntVector int16Vector =
        new SmallIntVector(new Field("int16", int16Field, null), allocator);
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);
    UInt1Vector uint8Vector = new UInt1Vector(new Field("uint8", uint8Field, null), allocator);
    UInt2Vector uint16Vector = new UInt2Vector(new Field("uint16", uint16Field, null), allocator);
    UInt4Vector uint32Vector = new UInt4Vector(new Field("uint32", uint32Field, null), allocator);
    UInt8Vector uint64Vector = new UInt8Vector(new Field("uint64", uint64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(
            int8Vector,
            int16Vector,
            int32Vector,
            int64Vector,
            uint8Vector,
            uint16Vector,
            uint32Vector,
            uint64Vector);

    int rowCount = 12;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < 10; row++) {
        int8Vector.set(row, 11 * row * (row % 2 == 0 ? 1 : -1));
        int16Vector.set(row, 63 * row * (row % 2 == 0 ? 1 : -1));
        int32Vector.set(row, 513 * row * (row % 2 == 0 ? 1 : -1));
        int64Vector.set(row, 3791L * row * (row % 2 == 0 ? 1 : -1));
        uint8Vector.set(row, 11 * row);
        uint16Vector.set(row, 63 * row);
        uint32Vector.set(row, 513 * row);
        uint64Vector.set(row, 3791L * row);
      }

      // Min values
      int8Vector.set(10, Byte.MIN_VALUE);
      int16Vector.set(10, Short.MIN_VALUE);
      int32Vector.set(10, Integer.MIN_VALUE);
      int64Vector.set(10, Long.MIN_VALUE);
      uint8Vector.set(10, 0);
      uint16Vector.set(10, 0);
      uint32Vector.set(10, 0);
      uint64Vector.set(10, 0);

      // Max values
      int8Vector.set(11, Byte.MAX_VALUE);
      int16Vector.set(11, Short.MAX_VALUE);
      int32Vector.set(11, Integer.MAX_VALUE);
      int64Vector.set(11, Long.MAX_VALUE);
      uint8Vector.set(11, 0xff);
      uint16Vector.set(11, 0xffff);
      uint32Vector.set(11, 0xffffffff);
      uint64Vector.set(11, Long.MAX_VALUE); // Max that can be encoded

      File dataFile = new File(TMP, "testWriteIntegers.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals((int) int8Vector.get(row), record.get("int8"));
          assertEquals((int) int16Vector.get(row), record.get("int16"));
          assertEquals(int32Vector.get(row), record.get("int32"));
          assertEquals(int64Vector.get(row), record.get("int64"));
          assertEquals(Byte.toUnsignedInt(uint8Vector.get(row)), record.get("uint8"));
          assertEquals(Short.toUnsignedInt((short) uint16Vector.get(row)), record.get("uint16"));
          assertEquals(Integer.toUnsignedLong(uint32Vector.get(row)), record.get("uint32"));
          assertEquals(uint64Vector.get(row), record.get("uint64"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableIntegers() throws Exception {

    // Field definitions
    FieldType int8Field = new FieldType(true, new ArrowType.Int(8, true), null);
    FieldType int16Field = new FieldType(true, new ArrowType.Int(16, true), null);
    FieldType int32Field = new FieldType(true, new ArrowType.Int(32, true), null);
    FieldType int64Field = new FieldType(true, new ArrowType.Int(64, true), null);
    FieldType uint8Field = new FieldType(true, new ArrowType.Int(8, false), null);
    FieldType uint16Field = new FieldType(true, new ArrowType.Int(16, false), null);
    FieldType uint32Field = new FieldType(true, new ArrowType.Int(32, false), null);
    FieldType uint64Field = new FieldType(true, new ArrowType.Int(64, false), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TinyIntVector int8Vector = new TinyIntVector(new Field("int8", int8Field, null), allocator);
    SmallIntVector int16Vector =
        new SmallIntVector(new Field("int16", int16Field, null), allocator);
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);
    UInt1Vector uint8Vector = new UInt1Vector(new Field("uint8", uint8Field, null), allocator);
    UInt2Vector uint16Vector = new UInt2Vector(new Field("uint16", uint16Field, null), allocator);
    UInt4Vector uint32Vector = new UInt4Vector(new Field("uint32", uint32Field, null), allocator);
    UInt8Vector uint64Vector = new UInt8Vector(new Field("uint64", uint64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(
            int8Vector,
            int16Vector,
            int32Vector,
            int64Vector,
            uint8Vector,
            uint16Vector,
            uint32Vector,
            uint64Vector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null values
      int8Vector.setNull(0);
      int16Vector.setNull(0);
      int32Vector.setNull(0);
      int64Vector.setNull(0);
      uint8Vector.setNull(0);
      uint16Vector.setNull(0);
      uint32Vector.setNull(0);
      uint64Vector.setNull(0);

      // Zero values
      int8Vector.set(1, 0);
      int16Vector.set(1, 0);
      int32Vector.set(1, 0);
      int64Vector.set(1, 0);
      uint8Vector.set(1, 0);
      uint16Vector.set(1, 0);
      uint32Vector.set(1, 0);
      uint64Vector.set(1, 0);

      // Non-zero values
      int8Vector.set(2, Byte.MAX_VALUE);
      int16Vector.set(2, Short.MAX_VALUE);
      int32Vector.set(2, Integer.MAX_VALUE);
      int64Vector.set(2, Long.MAX_VALUE);
      uint8Vector.set(2, Byte.MAX_VALUE);
      uint16Vector.set(2, Short.MAX_VALUE);
      uint32Vector.set(2, Integer.MAX_VALUE);
      uint64Vector.set(2, Long.MAX_VALUE);

      File dataFile = new File(TMP, "testWriteNullableIntegers.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("int8"));
        assertNull(record.get("int16"));
        assertNull(record.get("int32"));
        assertNull(record.get("int64"));
        assertNull(record.get("uint8"));
        assertNull(record.get("uint16"));
        assertNull(record.get("uint32"));
        assertNull(record.get("uint64"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals((int) int8Vector.get(row), record.get("int8"));
          assertEquals((int) int16Vector.get(row), record.get("int16"));
          assertEquals(int32Vector.get(row), record.get("int32"));
          assertEquals(int64Vector.get(row), record.get("int64"));
          assertEquals(Byte.toUnsignedInt(uint8Vector.get(row)), record.get("uint8"));
          assertEquals(Short.toUnsignedInt((short) uint16Vector.get(row)), record.get("uint16"));
          assertEquals(Integer.toUnsignedLong(uint32Vector.get(row)), record.get("uint32"));
          assertEquals(uint64Vector.get(row), record.get("uint64"));
        }
      }
    }
  }

  @Test
  public void testWriteFloatingPoints() throws Exception {

    // Field definitions
    FieldType float16Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), null);
    FieldType float32Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float2Vector float16Vector =
        new Float2Vector(new Field("float16", float16Field, null), allocator);
    Float4Vector float32Vector =
        new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector =
        new Float8Vector(new Field("float64", float64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(float16Vector, float32Vector, float64Vector);
    int rowCount = 15;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < 10; row++) {
        float16Vector.set(row, Float16.toFloat16(3.6f * row * (row % 2 == 0 ? 1.0f : -1.0f)));
        float32Vector.set(row, 37.6f * row * (row % 2 == 0 ? 1 : -1));
        float64Vector.set(row, 37.6d * row * (row % 2 == 0 ? 1 : -1));
      }

      float16Vector.set(10, Float16.toFloat16(Float.MIN_VALUE));
      float32Vector.set(10, Float.MIN_VALUE);
      float64Vector.set(10, Double.MIN_VALUE);

      float16Vector.set(11, Float16.toFloat16(Float.MAX_VALUE));
      float32Vector.set(11, Float.MAX_VALUE);
      float64Vector.set(11, Double.MAX_VALUE);

      float16Vector.set(12, Float16.toFloat16(Float.NaN));
      float32Vector.set(12, Float.NaN);
      float64Vector.set(12, Double.NaN);

      float16Vector.set(13, Float16.toFloat16(Float.POSITIVE_INFINITY));
      float32Vector.set(13, Float.POSITIVE_INFINITY);
      float64Vector.set(13, Double.POSITIVE_INFINITY);

      float16Vector.set(14, Float16.toFloat16(Float.NEGATIVE_INFINITY));
      float32Vector.set(14, Float.NEGATIVE_INFINITY);
      float64Vector.set(14, Double.NEGATIVE_INFINITY);

      File dataFile = new File(TMP, "testWriteFloatingPoints.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(float16Vector.getValueAsFloat(row), record.get("float16"));
          assertEquals(float32Vector.get(row), record.get("float32"));
          assertEquals(float64Vector.get(row), record.get("float64"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableFloatingPoints() throws Exception {

    // Field definitions
    FieldType float16Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), null);
    FieldType float32Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float2Vector float16Vector =
        new Float2Vector(new Field("float16", float16Field, null), allocator);
    Float4Vector float32Vector =
        new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector =
        new Float8Vector(new Field("float64", float64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(float16Vector, float32Vector, float64Vector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null values
      float16Vector.setNull(0);
      float32Vector.setNull(0);
      float64Vector.setNull(0);

      // Zero values
      float16Vector.setSafeWithPossibleTruncate(1, 0.0f);
      float32Vector.set(1, 0.0f);
      float64Vector.set(1, 0.0);

      // Non-zero values
      float16Vector.setSafeWithPossibleTruncate(2, 1.0f);
      float32Vector.set(2, 1.0f);
      float64Vector.set(2, 1.0);

      File dataFile = new File(TMP, "testWriteNullableFloatingPoints.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("float16"));
        assertNull(record.get("float32"));
        assertNull(record.get("float64"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(float16Vector.getValueAsFloat(row), record.get("float16"));
          assertEquals(float32Vector.get(row), record.get("float32"));
          assertEquals(float64Vector.get(row), record.get("float64"));
        }
      }
    }
  }

  @Test
  public void testWriteStrings() throws Exception {

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

      File dataFile = new File(TMP, "testWriteStrings.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(stringVector.getObject(row).toString(), record.get("string").toString());
        }
      }
    }
  }

  @Test
  public void testWriteNullableStrings() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableStrings.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("string"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(stringVector.getObject(row).toString(), record.get("string").toString());
        }
      }
    }
  }

  @Test
  public void testWriteBinary() throws Exception {

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

      File dataFile = new File(TMP, "testWriteBinary.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          ByteBuffer buf = ((ByteBuffer) record.get("binary"));
          byte[] bytes = new byte[buf.remaining()];
          buf.get(bytes);
          byte[] fixedBytes = ((GenericData.Fixed) record.get("fixed")).bytes();
          assertArrayEquals(binaryVector.getObject(row), bytes);
          assertArrayEquals(fixedVector.getObject(row), fixedBytes);
        }
      }
    }
  }

  @Test
  public void testWriteNullableBinary() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableBinary.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("binary"));
        assertNull(record.get("fixed"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          ByteBuffer buf = ((ByteBuffer) record.get("binary"));
          byte[] bytes = new byte[buf.remaining()];
          buf.get(bytes);
          byte[] fixedBytes = ((GenericData.Fixed) record.get("fixed")).bytes();
          assertArrayEquals(binaryVector.getObject(row), bytes);
          assertArrayEquals(fixedVector.getObject(row), fixedBytes);
        }
      }
    }
  }

  // Data production for logical types, nullable and non-nullable

  @Test
  public void testWriteDecimals() throws Exception {

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

      File dataFile = new File(TMP, "testWriteDecimals.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(
              decimal128Vector1.getObject(row), decodeFixedDecimal(record, "decimal128_1"));
          assertEquals(
              decimal128Vector2.getObject(row), decodeFixedDecimal(record, "decimal128_2"));
          assertEquals(
              decimal256Vector1.getObject(row), decodeFixedDecimal(record, "decimal256_1"));
          assertEquals(
              decimal256Vector2.getObject(row), decodeFixedDecimal(record, "decimal256_2"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableDecimals() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableDecimals.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("decimal128_1"));
        assertNull(record.get("decimal128_2"));
        assertNull(record.get("decimal256_1"));
        assertNull(record.get("decimal256_2"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(
              decimal128Vector1.getObject(row), decodeFixedDecimal(record, "decimal128_1"));
          assertEquals(
              decimal128Vector2.getObject(row), decodeFixedDecimal(record, "decimal128_2"));
          assertEquals(
              decimal256Vector1.getObject(row), decodeFixedDecimal(record, "decimal256_1"));
          assertEquals(
              decimal256Vector2.getObject(row), decodeFixedDecimal(record, "decimal256_2"));
        }
      }
    }
  }

  private static BigDecimal decodeFixedDecimal(GenericRecord record, String fieldName) {
    GenericData.Fixed fixed = (GenericData.Fixed) record.get(fieldName);
    var logicalType = LogicalTypes.fromSchema(fixed.getSchema());
    return new Conversions.DecimalConversion().fromFixed(fixed, fixed.getSchema(), logicalType);
  }

  @Test
  public void testWriteDates() throws Exception {

    // Field definitions
    FieldType dateDayField = new FieldType(false, new ArrowType.Date(DateUnit.DAY), null);
    FieldType dateMillisField =
        new FieldType(false, new ArrowType.Date(DateUnit.MILLISECOND), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    DateDayVector dateDayVector =
        new DateDayVector(new Field("dateDay", dateDayField, null), allocator);
    DateMilliVector dateMillisVector =
        new DateMilliVector(new Field("dateMillis", dateMillisField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(dateDayVector, dateMillisVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      dateDayVector.setSafe(0, (int) LocalDate.now().toEpochDay());
      dateDayVector.setSafe(1, (int) LocalDate.now().toEpochDay() + 1);
      dateDayVector.setSafe(2, (int) LocalDate.now().toEpochDay() + 2);

      dateMillisVector.setSafe(0, LocalDate.now().toEpochDay() * 86400000L);
      dateMillisVector.setSafe(1, (LocalDate.now().toEpochDay() + 1) * 86400000L);
      dateMillisVector.setSafe(2, (LocalDate.now().toEpochDay() + 2) * 86400000L);

      File dataFile = new File(TMP, "testWriteDates.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(dateDayVector.get(row), record.get("dateDay"));
          assertEquals(
              dateMillisVector.get(row), ((long) (Integer) record.get("dateMillis")) * 86400000L);
        }
      }
    }
  }

  @Test
  public void testWriteNullableDates() throws Exception {

    // Field definitions
    FieldType dateDayField = new FieldType(true, new ArrowType.Date(DateUnit.DAY), null);
    FieldType dateMillisField = new FieldType(true, new ArrowType.Date(DateUnit.MILLISECOND), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    DateDayVector dateDayVector =
        new DateDayVector(new Field("dateDay", dateDayField, null), allocator);
    DateMilliVector dateMillisVector =
        new DateMilliVector(new Field("dateMillis", dateMillisField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(dateDayVector, dateMillisVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      dateDayVector.setNull(0);
      dateDayVector.setSafe(1, 0);
      dateDayVector.setSafe(2, (int) LocalDate.now().toEpochDay());

      dateMillisVector.setNull(0);
      dateMillisVector.setSafe(1, 0);
      dateMillisVector.setSafe(2, LocalDate.now().toEpochDay() * 86400000L);

      File dataFile = new File(TMP, "testWriteNullableDates.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("dateDay"));
        assertNull(record.get("dateMillis"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(dateDayVector.get(row), record.get("dateDay"));
          assertEquals(
              dateMillisVector.get(row), ((long) (Integer) record.get("dateMillis")) * 86400000L);
        }
      }
    }
  }

  @Test
  public void testWriteTimes() throws Exception {

    // Field definitions
    FieldType timeSecField = new FieldType(false, new ArrowType.Time(TimeUnit.SECOND, 32), null);
    FieldType timeMillisField =
        new FieldType(false, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
    FieldType timeMicrosField =
        new FieldType(false, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null);
    FieldType timeNanosField =
        new FieldType(false, new ArrowType.Time(TimeUnit.NANOSECOND, 64), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeSecVector timeSecVector =
        new TimeSecVector(new Field("timeSec", timeSecField, null), allocator);
    TimeMilliVector timeMillisVector =
        new TimeMilliVector(new Field("timeMillis", timeMillisField, null), allocator);
    TimeMicroVector timeMicrosVector =
        new TimeMicroVector(new Field("timeMicros", timeMicrosField, null), allocator);
    TimeNanoVector timeNanosVector =
        new TimeNanoVector(new Field("timeNanos", timeNanosField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(timeSecVector, timeMillisVector, timeMicrosVector, timeNanosVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timeSecVector.setSafe(0, ZonedDateTime.now().toLocalTime().toSecondOfDay());
      timeSecVector.setSafe(1, ZonedDateTime.now().toLocalTime().toSecondOfDay() - 1);
      timeSecVector.setSafe(2, ZonedDateTime.now().toLocalTime().toSecondOfDay() - 2);

      timeMillisVector.setSafe(
          0, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000));
      timeMillisVector.setSafe(
          1, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000) - 1000);
      timeMillisVector.setSafe(
          2, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000) - 2000);

      timeMicrosVector.setSafe(0, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000);
      timeMicrosVector.setSafe(1, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000 - 1000000);
      timeMicrosVector.setSafe(2, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000 - 2000000);

      timeNanosVector.setSafe(0, ZonedDateTime.now().toLocalTime().toNanoOfDay());
      timeNanosVector.setSafe(1, ZonedDateTime.now().toLocalTime().toNanoOfDay() - 1000000000);
      timeNanosVector.setSafe(2, ZonedDateTime.now().toLocalTime().toNanoOfDay() - 2000000000);

      File dataFile = new File(TMP, "testWriteTimes.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(timeSecVector.get(row), (int) (record.get("timeSec")) / 1000);
          assertEquals(timeMillisVector.get(row), record.get("timeMillis"));
          assertEquals(timeMicrosVector.get(row), record.get("timeMicros"));
          // Avro doesn't have time-nanos (mar 2025), so expect column to be saved as micros
          long nanosAsMicros = (timeNanosVector.get(row) / 1000);
          assertEquals(nanosAsMicros, (long) record.get("timeNanos"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableTimes() throws Exception {

    // Field definitions
    FieldType timeSecField = new FieldType(true, new ArrowType.Time(TimeUnit.SECOND, 32), null);
    FieldType timeMillisField =
        new FieldType(true, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
    FieldType timeMicrosField =
        new FieldType(true, new ArrowType.Time(TimeUnit.MICROSECOND, 64), null);
    FieldType timeNanosField =
        new FieldType(true, new ArrowType.Time(TimeUnit.NANOSECOND, 64), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeSecVector timeSecVector =
        new TimeSecVector(new Field("timeSec", timeSecField, null), allocator);
    TimeMilliVector timeMillisVector =
        new TimeMilliVector(new Field("timeMillis", timeMillisField, null), allocator);
    TimeMicroVector timeMicrosVector =
        new TimeMicroVector(new Field("timeMicros", timeMicrosField, null), allocator);
    TimeNanoVector timeNanosVector =
        new TimeNanoVector(new Field("timeNanos", timeNanosField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors =
        Arrays.asList(timeSecVector, timeMillisVector, timeMicrosVector, timeNanosVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timeSecVector.setNull(0);
      timeSecVector.setSafe(1, 0);
      timeSecVector.setSafe(2, ZonedDateTime.now().toLocalTime().toSecondOfDay());

      timeMillisVector.setNull(0);
      timeMillisVector.setSafe(1, 0);
      timeMillisVector.setSafe(
          2, (int) (ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000000));

      timeMicrosVector.setNull(0);
      timeMicrosVector.setSafe(1, 0);
      timeMicrosVector.setSafe(2, ZonedDateTime.now().toLocalTime().toNanoOfDay() / 1000);

      timeNanosVector.setNull(0);
      timeNanosVector.setSafe(1, 0);
      timeNanosVector.setSafe(2, ZonedDateTime.now().toLocalTime().toNanoOfDay());

      File dataFile = new File(TMP, "testWriteNullableTimes.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("timeSec"));
        assertNull(record.get("timeMillis"));
        assertNull(record.get("timeMicros"));
        assertNull(record.get("timeNanos"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(timeSecVector.get(row), ((int) record.get("timeSec") / 1000));
          assertEquals(timeMillisVector.get(row), record.get("timeMillis"));
          assertEquals(timeMicrosVector.get(row), record.get("timeMicros"));
          // Avro doesn't have time-nanos (mar 2025), so expect column to be saved as micros
          long nanosAsMicros = (timeNanosVector.get(row) / 1000);
          assertEquals(nanosAsMicros, (long) record.get("timeNanos"));
        }
      }
    }
  }

  @Test
  public void testWriteZoneAwareTimestamps() throws Exception {

    // Field definitions
    FieldType timestampSecField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), null);
    FieldType timestampMillisField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null);
    FieldType timestampMicrosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null);
    FieldType timestampNanosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampSecTZVector timestampSecVector =
        new TimeStampSecTZVector(new Field("timestampSec", timestampSecField, null), allocator);
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
        Arrays.asList(
            timestampSecVector, timestampMillisVector, timestampMicrosVector, timestampNanosVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampSecVector.setSafe(0, (int) Instant.now().getEpochSecond());
      timestampSecVector.setSafe(1, (int) Instant.now().getEpochSecond() - 1);
      timestampSecVector.setSafe(2, (int) Instant.now().getEpochSecond() - 2);

      timestampMillisVector.setSafe(0, (int) Instant.now().toEpochMilli());
      timestampMillisVector.setSafe(1, (int) Instant.now().toEpochMilli() - 1000);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli() - 2000);

      timestampMicrosVector.setSafe(0, Instant.now().toEpochMilli() * 1000);
      timestampMicrosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000);
      timestampMicrosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000);

      timestampNanosVector.setSafe(0, Instant.now().toEpochMilli() * 1000000);
      timestampNanosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000000);
      timestampNanosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000000);

      File dataFile = new File(TMP, "testWriteZoneAwareTimestamps.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(
              timestampSecVector.get(row), (int) ((long) record.get("timestampSec") / 1000));
          assertEquals(timestampMillisVector.get(row), (int) (long) record.get("timestampMillis"));
          assertEquals(timestampMicrosVector.get(row), record.get("timestampMicros"));
          assertEquals(timestampNanosVector.get(row), record.get("timestampNanos"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableZoneAwareTimestamps() throws Exception {

    // Field definitions
    FieldType timestampSecField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), null);
    FieldType timestampMillisField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null);
    FieldType timestampMicrosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), null);
    FieldType timestampNanosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampSecTZVector timestampSecVector =
        new TimeStampSecTZVector(new Field("timestampSec", timestampSecField, null), allocator);
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
        Arrays.asList(
            timestampSecVector, timestampMillisVector, timestampMicrosVector, timestampNanosVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampSecVector.setNull(0);
      timestampSecVector.setSafe(1, 0);
      timestampSecVector.setSafe(2, (int) Instant.now().getEpochSecond());

      timestampMillisVector.setNull(0);
      timestampMillisVector.setSafe(1, 0);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli());

      timestampMicrosVector.setNull(0);
      timestampMicrosVector.setSafe(1, 0);
      timestampMicrosVector.setSafe(2, Instant.now().toEpochMilli() * 1000);

      timestampNanosVector.setNull(0);
      timestampNanosVector.setSafe(1, 0);
      timestampNanosVector.setSafe(2, Instant.now().toEpochMilli() * 1000000);

      File dataFile = new File(TMP, "testWriteNullableZoneAwareTimestamps.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("timestampSec"));
        assertNull(record.get("timestampMillis"));
        assertNull(record.get("timestampMicros"));
        assertNull(record.get("timestampNanos"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(
              timestampSecVector.get(row), (int) ((long) record.get("timestampSec") / 1000));
          assertEquals(timestampMillisVector.get(row), (int) (long) record.get("timestampMillis"));
          assertEquals(timestampMicrosVector.get(row), record.get("timestampMicros"));
          assertEquals(timestampNanosVector.get(row), record.get("timestampNanos"));
        }
      }
    }
  }

  @Test
  public void testWriteLocalTimestamps() throws Exception {

    // Field definitions
    FieldType timestampSecField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.SECOND, null), null);
    FieldType timestampMillisField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null);
    FieldType timestampMicrosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null);
    FieldType timestampNanosField =
        new FieldType(false, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampSecVector timestampSecVector =
        new TimeStampSecVector(new Field("timestampSec", timestampSecField, null), allocator);
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
        Arrays.asList(
            timestampSecVector, timestampMillisVector, timestampMicrosVector, timestampNanosVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampSecVector.setSafe(0, (int) Instant.now().getEpochSecond());
      timestampSecVector.setSafe(1, (int) Instant.now().getEpochSecond() - 1);
      timestampSecVector.setSafe(2, (int) Instant.now().getEpochSecond() - 2);

      timestampMillisVector.setSafe(0, (int) Instant.now().toEpochMilli());
      timestampMillisVector.setSafe(1, (int) Instant.now().toEpochMilli() - 1000);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli() - 2000);

      timestampMicrosVector.setSafe(0, Instant.now().toEpochMilli() * 1000);
      timestampMicrosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000);
      timestampMicrosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000);

      timestampNanosVector.setSafe(0, Instant.now().toEpochMilli() * 1000000);
      timestampNanosVector.setSafe(1, (Instant.now().toEpochMilli() - 1000) * 1000000);
      timestampNanosVector.setSafe(2, (Instant.now().toEpochMilli() - 2000) * 1000000);

      File dataFile = new File(TMP, "testWriteZoneAwareTimestamps.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(
              timestampSecVector.get(row), (int) ((long) record.get("timestampSec") / 1000));
          assertEquals(timestampMillisVector.get(row), (int) (long) record.get("timestampMillis"));
          assertEquals(timestampMicrosVector.get(row), record.get("timestampMicros"));
          assertEquals(timestampNanosVector.get(row), record.get("timestampNanos"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableLocalTimestamps() throws Exception {

    // Field definitions
    FieldType timestampSecField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.SECOND, null), null);
    FieldType timestampMillisField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null);
    FieldType timestampMicrosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null);
    FieldType timestampNanosField =
        new FieldType(true, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    TimeStampSecVector timestampSecVector =
        new TimeStampSecVector(new Field("timestampSec", timestampSecField, null), allocator);
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
        Arrays.asList(
            timestampSecVector, timestampMillisVector, timestampMicrosVector, timestampNanosVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      timestampSecVector.setNull(0);
      timestampSecVector.setSafe(1, 0);
      timestampSecVector.setSafe(2, (int) Instant.now().getEpochSecond());

      timestampMillisVector.setNull(0);
      timestampMillisVector.setSafe(1, 0);
      timestampMillisVector.setSafe(2, (int) Instant.now().toEpochMilli());

      timestampMicrosVector.setNull(0);
      timestampMicrosVector.setSafe(1, 0);
      timestampMicrosVector.setSafe(2, Instant.now().toEpochMilli() * 1000);

      timestampNanosVector.setNull(0);
      timestampNanosVector.setSafe(1, 0);
      timestampNanosVector.setSafe(2, Instant.now().toEpochMilli() * 1000000);

      File dataFile = new File(TMP, "testWriteNullableZoneAwareTimestamps.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        // Read and check values
        GenericRecord record = datumReader.read(null, decoder);
        assertNull(record.get("timestampSec"));
        assertNull(record.get("timestampMillis"));
        assertNull(record.get("timestampMicros"));
        assertNull(record.get("timestampNanos"));

        for (int row = 1; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(
              timestampSecVector.get(row), (int) ((long) record.get("timestampSec") / 1000));
          assertEquals(timestampMillisVector.get(row), (int) (long) record.get("timestampMillis"));
          assertEquals(timestampMicrosVector.get(row), record.get("timestampMicros"));
          assertEquals(timestampNanosVector.get(row), record.get("timestampNanos"));
        }
      }
    }
  }

  // Data production for containers of primitive and logical types, nullable and non-nullable

  @Test
  public void testWriteLists() throws Exception {

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

      File dataFile = new File(TMP, "testWriteLists.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(intListVector.getObject(row), record.get("intList"));
          assertEquals(dateListVector.getObject(row), record.get("dateList"));
          // Handle conversion from Arrow Text type
          List<?> vectorList = stringListVector.getObject(row);
          List<?> recordList = (List<?>) record.get("stringList");
          assertEquals(vectorList.size(), recordList.size());
          for (int i = 0; i < vectorList.size(); i++) {
            assertEquals(vectorList.get(i).toString(), recordList.get(i).toString());
          }
        }
      }
    }
  }

  @Test
  public void testWriteNullableLists() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableLists.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          for (String list :
              Arrays.asList("nullEntriesVector", "nullListVector", "nullBothVector")) {
            ListVector vector = (ListVector) root.getVector(list);
            Object recordField = record.get(list);
            if (vector.isNull(row)) {
              assertNull(recordField);
            } else {
              assertEquals(vector.getObject(row), recordField);
            }
          }
        }
      }
    }
  }

  @Test
  public void testWriteFixedLists() throws Exception {

    // Field definitions
    FieldType intListField = new FieldType(false, new ArrowType.FixedSizeList(5), null);
    FieldType stringListField = new FieldType(false, new ArrowType.FixedSizeList(5), null);
    FieldType dateListField = new FieldType(false, new ArrowType.FixedSizeList(5), null);

    Field intField = new Field("item", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field stringField = new Field("item", FieldType.notNullable(new ArrowType.Utf8()), null);
    Field dateField =
        new Field("item", FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    FixedSizeListVector intListVector =
        new FixedSizeListVector("intList", allocator, intListField, null);
    FixedSizeListVector stringListVector =
        new FixedSizeListVector("stringList", allocator, stringListField, null);
    FixedSizeListVector dateListVector =
        new FixedSizeListVector("dateList", allocator, dateListField, null);

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
        for (int j = 0; j < 5; j++) {
          intListWriter.writeInt(j);
        }
        intListWriter.endList();
      }

      // Set test data for stringList
      for (int i = 0; i < rowCount; i++) {
        stringListWriter.startList();
        for (int j = 0; j < 5; j++) {
          stringListWriter.writeVarChar("string" + j);
        }
        stringListWriter.endList();
      }

      // Set test data for dateList
      for (int i = 0; i < rowCount; i++) {
        dateListWriter.startList();
        for (int j = 0; j < 5; j++) {
          dateListWriter.writeDateDay((int) LocalDate.now().plusDays(j).toEpochDay());
        }
        dateListWriter.endList();
      }
      File dataFile = new File(TMP, "testWriteFixedLists.avro");

      // Update count for the vectors
      intListVector.setValueCount(rowCount);
      stringListVector.setValueCount(rowCount);
      dateListVector.setValueCount(rowCount);

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertEquals(intListVector.getObject(row), record.get("intList"));
          assertEquals(dateListVector.getObject(row), record.get("dateList"));
          // Handle conversion from Arrow Text type
          List<?> vectorList = stringListVector.getObject(row);
          List<?> recordList = (List<?>) record.get("stringList");
          assertEquals(vectorList.size(), recordList.size());
          for (int i = 0; i < vectorList.size(); i++) {
            assertEquals(vectorList.get(i).toString(), recordList.get(i).toString());
          }
        }
      }
    }
  }

  @Test
  public void testWriteNullableFixedLists() throws Exception {

    // Field definitions
    FieldType nullListType = new FieldType(true, new ArrowType.FixedSizeList(2), null);
    FieldType nonNullListType = new FieldType(false, new ArrowType.FixedSizeList(2), null);

    Field nullFieldType = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field nonNullFieldType =
        new Field("item", FieldType.notNullable(new ArrowType.Int(32, true)), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    FixedSizeListVector nullEntriesVector =
        new FixedSizeListVector("nullEntriesVector", allocator, nonNullListType, null);
    FixedSizeListVector nullListVector =
        new FixedSizeListVector("nullListVector", allocator, nullListType, null);
    FixedSizeListVector nullBothVector =
        new FixedSizeListVector("nullBothVector", allocator, nullListType, null);

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
      nullListWriter.integer().writeInt(123);
      nullListWriter.integer().writeInt(456);
      nullListWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(789);
      nullEntriesWriter.integer().writeInt(456);
      nullEntriesWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(12345);
      nullEntriesWriter.integer().writeInt(67891);
      nullEntriesWriter.endList();

      // Set test data for nullBothVector
      FieldWriter nullBothWriter = nullBothVector.getWriter();
      nullBothWriter.writeNull();
      nullBothWriter.setPosition(1);
      nullBothWriter.startList();
      nullListWriter.integer().writeNull();
      nullListWriter.integer().writeNull();
      nullBothWriter.endList();
      nullListWriter.startList();
      nullListWriter.integer().writeInt(123);
      nullListWriter.integer().writeInt(456);
      nullListWriter.endList();
      nullEntriesWriter.startList();
      nullEntriesWriter.integer().writeInt(789);
      nullEntriesWriter.integer().writeInt(456);
      nullEntriesWriter.endList();

      // Update count for the vectors
      nullListVector.setValueCount(4);
      nullEntriesVector.setValueCount(4);
      nullBothVector.setValueCount(4);

      File dataFile = new File(TMP, "testWriteNullableFixedLists.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          for (String list :
              Arrays.asList("nullEntriesVector", "nullListVector", "nullBothVector")) {
            FixedSizeListVector vector = (FixedSizeListVector) root.getVector(list);
            Object recordField = record.get(list);
            if (vector.isNull(row)) {
              assertNull(recordField);
            } else {
              assertEquals(vector.getObject(row), recordField);
            }
          }
        }
      }
    }
  }

  @Test
  public void testWriteMap() throws Exception {

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

      File dataFile = new File(TMP, "testWriteMap.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          Map<String, Object> intMap = convertMap(intMapVector.getObject(row));
          Map<String, Object> stringMap = convertMap(stringMapVector.getObject(row));
          Map<String, Object> dateMap = convertMap(dateMapVector.getObject(row));
          compareMaps(intMap, (Map) record.get("intMap"));
          compareMaps(stringMap, (Map) record.get("stringMap"));
          compareMaps(dateMap, (Map) record.get("dateMap"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableMap() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableMap.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          Map<String, Object> intMap = convertMap(nullEntriesVector.getObject(row));
          Map<String, Object> stringMap = convertMap(nullMapVector.getObject(row));
          Map<String, Object> dateMap = convertMap(nullBothVector.getObject(row));
          compareMaps(intMap, (Map) record.get("nullEntriesVector"));
          compareMaps(stringMap, (Map) record.get("nullMapVector"));
          compareMaps(dateMap, (Map) record.get("nullBothVector"));
        }
      }
    }
  }

  private Map<String, Object> convertMap(List<?> entryList) {

    if (entryList == null) {
      return null;
    }

    Map<String, Object> map = new HashMap<>();
    JsonStringArrayList<?> structList = (JsonStringArrayList<?>) entryList;
    for (Object entry : structList) {
      JsonStringHashMap<String, ?> structEntry = (JsonStringHashMap<String, ?>) entry;
      String key = structEntry.get(MapVector.KEY_NAME).toString();
      Object value = structEntry.get(MapVector.VALUE_NAME);
      map.put(key, value);
    }
    return map;
  }

  private void compareMaps(Map<String, ?> expected, Map<?, ?> actual) {
    if (expected == null) {
      assertNull(actual);
    } else {
      assertEquals(expected.size(), actual.size());
      for (Object key : actual.keySet()) {
        assertTrue(expected.containsKey(key.toString()));
        Object actualValue = actual.get(key);
        if (actualValue instanceof Utf8) {
          assertEquals(expected.get(key.toString()).toString(), actualValue.toString());
        } else {
          assertEquals(expected.get(key.toString()), actual.get(key));
        }
      }
    }
  }

  @Test
  public void testWriteStruct() throws Exception {

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

      File dataFile = new File(TMP, "testWriteStruct.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Update count for the vector
      structVector.setValueCount(rowCount);

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          assertNotNull(record.get("struct"));
          GenericRecord structRecord = (GenericRecord) record.get("struct");
          assertEquals(row, structRecord.get("intField"));
          assertEquals("string" + row, structRecord.get("stringField").toString());
          assertEquals((int) LocalDate.now().toEpochDay() + row, structRecord.get("dateField"));
        }
      }
    }
  }

  @Test
  public void testWriteNullableStructs() throws Exception {

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

      File dataFile = new File(TMP, "testWriteNullableStructs.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
        for (int row = 0; row < rowCount; row++) {
          producer.produce(encoder);
        }
        encoder.flush();
      }

      // Set up reading the AVRO block as a GenericRecord
      Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

      try (InputStream inputStream = new FileInputStream(dataFile)) {

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = null;

        // Read and check values
        for (int row = 0; row < rowCount; row++) {
          record = datumReader.read(record, decoder);
          if (row % 2 == 0) {
            assertNotNull(record.get("struct"));
            GenericRecord structRecord = (GenericRecord) record.get("struct");
            assertEquals(row, structRecord.get("intField"));
            assertEquals(row * 10, structRecord.get("nullableIntField"));
          } else {
            assertNotNull(record.get("struct"));
            GenericRecord structRecord = (GenericRecord) record.get("struct");
            assertEquals(row, structRecord.get("intField"));
            assertNull(structRecord.get("nullableIntField"));
          }
          if (row >= 2) {
            assertNotNull(record.get("nullableStruct"));
            GenericRecord nullableStructRecord = (GenericRecord) record.get("nullableStruct");
            assertEquals(row, nullableStructRecord.get("intField"));
            if (row % 2 == 0) {
              assertEquals(row * 10, nullableStructRecord.get("nullableIntField"));
            } else {
              assertNull(nullableStructRecord.get("nullableIntField"));
            }
          } else {
            assertNull(record.get("nullableStruct"));
          }
        }
      }
    }
  }
}
