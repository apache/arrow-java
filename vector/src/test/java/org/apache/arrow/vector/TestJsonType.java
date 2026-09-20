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

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.extension.InvalidExtensionMetadataException;
import org.apache.arrow.vector.extension.JsonType;
import org.apache.arrow.vector.extension.JsonVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestJsonType {
  BufferAllocator allocator;
  ArrowType.ExtensionType previousType;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
    previousType = ExtensionTypeRegistry.lookup(JsonType.EXTENSION_NAME);
    JsonType.ensureRegistered();
  }

  @AfterEach
  void afterEach() {
    ExtensionTypeRegistry.unregister(new JsonType(ArrowType.Utf8.INSTANCE));
    if (previousType != null) {
      ExtensionTypeRegistry.register(previousType);
    }
    allocator.close();
  }

  static Stream<ArrowType> storageTypes() {
    return Stream.of(
        ArrowType.Utf8.INSTANCE, ArrowType.LargeUtf8.INSTANCE, ArrowType.Utf8View.INSTANCE);
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testRoundTrip(ArrowType storage) {
    JsonType type = new JsonType(storage);
    assertEquals("arrow.json", type.extensionName());
    assertEquals(storage, type.storageType());
    assertFalse(type.isComplex());
    assertEquals("", type.serialize());
    assertEquals(type, type.deserialize(storage, type.serialize()));
    assertNotEquals(
        type,
        new JsonType(
            storage instanceof ArrowType.Utf8
                ? ArrowType.LargeUtf8.INSTANCE
                : ArrowType.Utf8.INSTANCE));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "{}", " { } ", "{\"future\": 1}"})
  void testDeserializeValid(String metadata) {
    JsonType type = new JsonType(ArrowType.Utf8.INSTANCE);
    assertEquals(type, type.deserialize(type.storageType(), metadata));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {" ", "null", "[]", "1", "true", "\"json\"", "{", "{} {}", "{} trailing"})
  void testInvalidMetadata(String metadata) {
    assertThrows(
        InvalidExtensionMetadataException.class,
        () -> new JsonType(ArrowType.Utf8.INSTANCE).deserialize(ArrowType.Utf8.INSTANCE, metadata));
  }

  @Test
  void testInvalidStorage() {
    for (ArrowType storage :
        new ArrowType[] {
          ArrowType.Binary.INSTANCE,
          ArrowType.LargeBinary.INSTANCE,
          ArrowType.BinaryView.INSTANCE,
          ArrowType.Null.INSTANCE,
          new ArrowType.Int(32, true)
        }) {
      assertThrows(IllegalArgumentException.class, () -> new JsonType(storage));
      assertThrows(
          IllegalArgumentException.class,
          () -> new JsonType(ArrowType.Utf8.INSTANCE).deserialize(storage, ""));
    }
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testSchemaRoundTrip(ArrowType storage) {
    for (boolean nullable : new boolean[] {false, true}) {
      Field field = field(storage, nullable);
      Schema schema = new Schema(Collections.singletonList(field));
      assertEquals(schema, Schema.deserializeMessage(ByteBuffer.wrap(schema.serializeAsMessage())));
    }
  }

  // Generated with PyArrow 24.0.0 using pa.schema([pa.field("json", pa.json_(t),
  // nullable=False, metadata={"custom": "preserved"}) for t in
  // [pa.string(), pa.large_string(), pa.string_view()]]).serialize().
  @Test
  void testPyArrowSchema() throws IOException {
    Schema schema;
    try (InputStream input = getClass().getResourceAsStream("/pyarrow_json_schema.arrow")) {
      schema = Schema.deserializeMessage(ByteBuffer.wrap(input.readAllBytes()));
    }
    ArrowType[] storage = storageTypes().toArray(ArrowType[]::new);
    for (int i = 0; i < storage.length; i++) {
      assertEquals(field(storage[i], false), schema.getFields().get(i));
    }
  }

  private static Field field(ArrowType storage, boolean nullable) {
    return new Field(
        "json",
        new FieldType(
            nullable, new JsonType(storage), null, Collections.singletonMap("custom", "preserved")),
        Collections.emptyList());
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testTransfer(ArrowType storage) {
    Field field = field(storage, true);
    try (JsonVector source = (JsonVector) field.createVector(allocator)) {
      byte[] bytes =
          "{\"key\":\"value longer than twelve bytes\"}".getBytes(StandardCharsets.UTF_8);
      setVector((VariableWidthFieldVector) source.getUnderlyingVector(), null, bytes, null);
      TransferPair split = source.getTransferPair("copy", allocator);
      try (JsonVector target = assertInstanceOf(JsonVector.class, split.getTo())) {
        split.splitAndTransfer(1, 2);
        assertEquals("copy", target.getName());
        assertEquals(field.getFieldType(), target.getField().getFieldType());
        assertEquals(new Text(bytes), target.getObject(0));
        assertTrue(target.isNull(1));
      }
      try (JsonVector target = (JsonVector) field.createVector(allocator)) {
        TransferPair copy = source.makeTransferPair(target);
        copy.copyValueSafe(1, 0);
        copy.copyValueSafe(2, 1);
        target.setValueCount(2);
        assertEquals(new Text(bytes), target.getObject(0));
        assertTrue(target.isNull(1));
      }
      TransferPair transfer = source.getTransferPair(allocator);
      try (JsonVector target = assertInstanceOf(JsonVector.class, transfer.getTo())) {
        transfer.transfer();
        assertEquals(field, target.getField());
        assertTrue(target.isNull(0));
        assertEquals(new Text(bytes), target.getObject(1));
        assertTrue(target.isNull(2));
        assertEquals(3, target.getValueCount());
        assertEquals(0, source.getValueCount());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testVectorIpcRoundTrip(ArrowType storage) throws IOException {
    Field field = field(storage, true);
    byte[] serialized = writeStream(field);
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(serialized), allocator)) {
      assertTrue(reader.loadNextBatch());
      JsonVector vector =
          assertInstanceOf(JsonVector.class, reader.getVectorSchemaRoot().getVector(0));
      assertEquals(field, vector.getField());
      assertValues(vector);
    }
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testReadUnderlyingType(ArrowType storage) throws IOException {
    Field field = field(storage, true);
    byte[] serialized = writeStream(field);
    ExtensionTypeRegistry.unregister((JsonType) field.getType());
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(serialized), allocator)) {
      assertTrue(reader.loadNextBatch());
      FieldVector vector = reader.getVectorSchemaRoot().getVector(0);
      assertEquals(storage, vector.getField().getType());
      assertEquals(field.getMetadata(), vector.getField().getMetadata());
      assertValues(vector);
    }
  }

  private static final String JSON = "{\"message\":\"你好, a JSON value longer than twelve bytes\"}";

  private byte[] writeStream(Field field) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root =
            VectorSchemaRoot.create(new Schema(Collections.singletonList(field)), allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      JsonVector vector = (JsonVector) root.getVector(0);
      setVector(
          (VariableWidthFieldVector) vector.getUnderlyingVector(),
          JSON.getBytes(StandardCharsets.UTF_8),
          null,
          "null".getBytes(StandardCharsets.UTF_8));
      root.setRowCount(3);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }

  private static void assertValues(FieldVector vector) {
    assertEquals(3, vector.getValueCount());
    assertEquals(new Text(JSON), vector.getObject(0));
    assertTrue(vector.isNull(1));
    assertEquals(new Text("null"), vector.getObject(2));
  }
}
