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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adapter.avro.consumers.CompositeAvroConsumer;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestWriteReadAvroRecord {

  @TempDir public static File TMP;

  @Test
  public void testWriteAndRead() throws Exception {
    File dataFile = new File(TMP, "test.avro");
    Schema schema = AvroTestBase.getSchema("test.avsc");

    // write data to disk
    GenericRecord user1 = new GenericData.Record(schema);
    user1.put("name", "Alyssa");
    user1.put("favorite_number", 256);

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<GenericRecord>(datumWriter)) {
      dataFileWriter.create(schema, dataFile);
      dataFileWriter.append(user1);
      dataFileWriter.append(user2);
    }

    // read data from disk
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    List<GenericRecord> result = new ArrayList<>();
    try (DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<GenericRecord>(dataFile, datumReader)) {
      while (dataFileReader.hasNext()) {
        GenericRecord user = dataFileReader.next();
        result.add(user);
      }
    }

    assertEquals(2, result.size());
    GenericRecord deUser1 = result.get(0);
    assertEquals("Alyssa", deUser1.get("name").toString());
    assertEquals(256, deUser1.get("favorite_number"));
    assertEquals(null, deUser1.get("favorite_color"));

    GenericRecord deUser2 = result.get(1);
    assertEquals("Ben", deUser2.get("name").toString());
    assertEquals(7, deUser2.get("favorite_number"));
    assertEquals("red", deUser2.get("favorite_color").toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWriteAndReadVSR(boolean useSchemaFile) throws Exception {

    BufferAllocator allocator = new RootAllocator();
    FieldType stringNotNull = new FieldType(false, ArrowType.Utf8.INSTANCE, null);
    FieldType stringNull = new FieldType(true, ArrowType.Utf8.INSTANCE, null);
    FieldType intN32Null = new FieldType(true, new ArrowType.Int(32, true), null);

    List<Field> fields = new ArrayList<>();
    fields.add(new Field("name", stringNotNull, null));
    fields.add(new Field("favorite_number", intN32Null, null));
    fields.add(new Field("favorite_color", stringNull, null));

    VarCharVector nameVector = new VarCharVector(fields.get(0), allocator);
    nameVector.allocateNew(2);
    nameVector.set(0, "Alyssa".getBytes(StandardCharsets.UTF_8));
    nameVector.set(1, "Ben".getBytes(StandardCharsets.UTF_8));

    IntVector favNumberVector = new IntVector(fields.get(1), allocator);
    favNumberVector.allocateNew(2);
    favNumberVector.set(0, 256);
    favNumberVector.set(1, 7);

    VarCharVector favColorVector = new VarCharVector(fields.get(2), allocator);
    favColorVector.allocateNew(2);
    favColorVector.setNull(0);
    favColorVector.set(1, "red".getBytes(StandardCharsets.UTF_8));

    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(nameVector);
    vectors.add(favNumberVector);
    vectors.add(favColorVector);

    Schema schema =
        useSchemaFile
            ? AvroTestBase.getSchema("test.avsc")
            : ArrowToAvroUtils.createAvroSchema(fields);

    File dataFile = new File(TMP, "test_vsr.avro");
    AvroToArrowConfig config = new AvroToArrowConfigBuilder(allocator).build();

    try (FileOutputStream fos = new FileOutputStream(dataFile)) {

      BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
      CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);

      producer.produce(encoder);
      producer.produce(encoder);

      encoder.flush();
    }

    List<Field> roundTripFields = new ArrayList<>();
    List<FieldVector> roundTripVectors = new ArrayList<>();

    try (FileInputStream fis = new FileInputStream(dataFile)) {

      BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(fis, null);
      CompositeAvroConsumer consumer = AvroToArrowUtils.createCompositeConsumer(schema, config);

      consumer.getConsumers().forEach(c -> roundTripFields.add(c.getVector().getField()));
      consumer.getConsumers().forEach(c -> roundTripVectors.add(c.getVector()));
      consumer.consume(decoder);
      consumer.consume(decoder);
    }

    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 2);
    VectorSchemaRoot roundTripRoot = new VectorSchemaRoot(roundTripFields, roundTripVectors, 2);

    assertEquals(root.getRowCount(), roundTripRoot.getRowCount());

    for (int row = 0; row < 2; row++) {
      for (int col = 0; col < 3; col++) {
        FieldVector vector = root.getVector(col);
        FieldVector roundTripVector = roundTripRoot.getVector(col);
        assertEquals(vector.getObject(row), roundTripVector.getObject(row));
      }
    }
  }
}
