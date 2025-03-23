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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

public class ArrowToAvroDataTest {

  @TempDir
  public static File TMP;

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
    SmallIntVector int16Vector = new SmallIntVector(new Field("int16", int16Field, null), allocator);
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);
    UInt1Vector uint8Vector = new UInt1Vector(new Field("uint8", uint8Field, null), allocator);
    UInt2Vector uint16Vector = new UInt2Vector(new Field("uint16", uint16Field, null), allocator);
    UInt4Vector uint32Vector = new UInt4Vector(new Field("uint32", uint32Field, null), allocator);
    UInt8Vector uint64Vector = new UInt8Vector(new Field("uint64", uint64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(
        int8Vector, int16Vector, int32Vector, int64Vector,
        uint8Vector, uint16Vector, uint32Vector, uint64Vector);

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
    SmallIntVector int16Vector = new SmallIntVector(new Field("int16", int16Field, null), allocator);
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);
    UInt1Vector uint8Vector = new UInt1Vector(new Field("uint8", uint8Field, null), allocator);
    UInt2Vector uint16Vector = new UInt2Vector(new Field("uint16", uint16Field, null), allocator);
    UInt4Vector uint32Vector = new UInt4Vector(new Field("uint32", uint32Field, null), allocator);
    UInt8Vector uint64Vector = new UInt8Vector(new Field("uint64", uint64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(
        int8Vector, int16Vector, int32Vector, int64Vector,
        uint8Vector, uint16Vector, uint32Vector, uint64Vector);

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
    FieldType float16Field = new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), null);
    FieldType float32Field = new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field = new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float2Vector float16Vector = new Float2Vector(new Field("float16", float16Field, null), allocator);
    Float4Vector float32Vector = new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector = new Float8Vector(new Field("float64", float64Field, null), allocator);

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
    FieldType float16Field = new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), null);
    FieldType float32Field = new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field = new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float2Vector float16Vector = new Float2Vector(new Field("float16", float16Field, null), allocator);
    Float4Vector float32Vector = new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector = new Float8Vector(new Field("float64", float64Field, null), allocator);

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
}
