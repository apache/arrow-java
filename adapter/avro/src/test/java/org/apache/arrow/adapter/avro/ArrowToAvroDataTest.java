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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
      for (int i = 0; i < 10; i++) {
        int8Vector.set(i, 11 * i * (i % 2 == 0 ? 1 : -1));
        int16Vector.set(i, 63 * i * (i % 2 == 0 ? 1 : -1));
        int32Vector.set(i, 513 * i * (i % 2 == 0 ? 1 : -1));
        int64Vector.set(i, 3791L * i * (i % 2 == 0 ? 1 : -1));
        uint8Vector.set(i, 11 * i);
        uint16Vector.set(i, 63 * i);
        uint32Vector.set(i, 513 * i);
        uint64Vector.set(i, 3791L * i);
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
}
