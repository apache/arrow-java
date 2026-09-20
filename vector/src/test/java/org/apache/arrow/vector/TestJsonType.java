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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestJsonType {
  static Stream<ArrowType> storageTypes() {
    return Stream.of(
        ArrowType.Utf8.INSTANCE, ArrowType.LargeUtf8.INSTANCE, ArrowType.Utf8View.INSTANCE);
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testType(ArrowType storage) {
    JsonType type = new JsonType(storage);
    assertEquals("arrow.json", type.extensionName());
    assertEquals(storage, type.storageType());
    assertFalse(type.isComplex());
    assertEquals("", type.serialize());
    for (String metadata : new String[] {"", "{}", " { } ", "{\"future\": {\"value\": 1}}"}) {
      ArrowType restored = type.deserialize(storage, metadata);
      assertEquals(type, restored);
      assertEquals(type.hashCode(), restored.hashCode());
    }
    storageTypes()
        .filter(other -> !storage.equals(other))
        .forEach(other -> assertNotEquals(type, new JsonType(other)));
    assertNotEquals(type, storage);
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
    JsonType.ensureRegistered();
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
  void testPyArrowSchema() {
    JsonType.ensureRegistered();
    String encoded =
        "/////9ACAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAMAAADEAQAA2AAAAAQAAABa/v//"
            + "AAAAGBQAAADAAAAACAAAABQAAAAAAAAABAAAAGpzb24AAAAAAwAAAHgAAABAAAAABAAAANz9//8YAAAABAAAAAoAAABhcnJv"
            + "dy5qc29uAAAUAAAAQVJST1c6ZXh0ZW5zaW9uOm5hbWUAAAAAFP7//xAAAAAEAAAAAAAAAAAAAAAYAAAAQVJST1c6ZXh0ZW5z"
            + "aW9uOm1ldGFkYXRhAAAAAEj+//8YAAAABAAAAAkAAABwcmVzZXJ2ZWQAAAAGAAAAY3VzdG9tAABA/v//Kv///wAAABQUAAAA"
            + "xAAAAAgAAAAUAAAAAAAAAAQAAABqc29uAAAAAAMAAAB4AAAAQAAAAAQAAACs/v//GAAAAAQAAAAKAAAAYXJyb3cuanNvbgAA"
            + "FAAAAEFSUk9XOmV4dGVuc2lvbjpuYW1lAAAAAOT+//8QAAAABAAAAAAAAAAAAAAAGAAAAEFSUk9XOmV4dGVuc2lvbjptZXRh"
            + "ZGF0YQAAAAAY////GAAAAAQAAAAJAAAAcHJlc2VydmVkAAAABgAAAGN1c3RvbQAABAAGAAQAAAAAABIAGAAIAAAABwAMAAAA"
            + "EAAUABIAAAAAAAAFFAAAAMwAAAAIAAAAFAAAAAAAAAAEAAAAanNvbgAAAAADAAAAgAAAAEAAAAAEAAAAlP///xgAAAAEAAAA"
            + "CgAAAGFycm93Lmpzb24AABQAAABBUlJPVzpleHRlbnNpb246bmFtZQAAAADM////EAAAAAQAAAAAAAAAAAAAABgAAABBUlJP"
            + "VzpleHRlbnNpb246bWV0YWRhdGEAAAAACAAMAAQACAAIAAAAGAAAAAQAAAAJAAAAcHJlc2VydmVkAAAABgAAAGN1c3RvbQAA"
            + "BAAEAAQAAAA=";
    Schema schema = Schema.deserializeMessage(ByteBuffer.wrap(Base64.getDecoder().decode(encoded)));
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
    try (RootAllocator allocator = new RootAllocator();
        JsonVector source = (JsonVector) field.createVector(allocator)) {
      byte[] bytes =
          "{\"key\":\"value longer than twelve bytes\"}".getBytes(StandardCharsets.UTF_8);
      FieldVector underlying = source.getUnderlyingVector();
      if (underlying instanceof VarCharVector) {
        ((VarCharVector) underlying).setSafe(0, bytes);
      } else if (underlying instanceof LargeVarCharVector) {
        ((LargeVarCharVector) underlying).setSafe(0, bytes);
      } else {
        ((ViewVarCharVector) underlying).setSafe(0, bytes);
      }
      source.setNull(1);
      source.setValueCount(2);
      TransferPair split = source.getTransferPair("copy", allocator);
      try (JsonVector target = assertInstanceOf(JsonVector.class, split.getTo())) {
        split.splitAndTransfer(0, 2);
        assertEquals("copy", target.getName());
        assertEquals(field.getFieldType(), target.getField().getFieldType());
        assertEquals(source.getObject(0), target.getObject(0));
        assertTrue(target.isNull(1));
      }
      try (JsonVector target = (JsonVector) field.createVector(allocator)) {
        TransferPair copy = source.makeTransferPair(target);
        copy.copyValueSafe(0, 0);
        copy.copyValueSafe(1, 1);
        target.setValueCount(2);
        assertEquals(source.getObject(0), target.getObject(0));
        assertTrue(target.isNull(1));
      }
      TransferPair transfer = source.getTransferPair(allocator);
      try (JsonVector target = assertInstanceOf(JsonVector.class, transfer.getTo())) {
        transfer.transfer();
        assertEquals(field, target.getField());
        assertEquals(new Text(bytes), target.getObject(0));
        assertTrue(target.isNull(1));
        assertEquals(2, target.getValueCount());
        assertEquals(0, source.getValueCount());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  void testIpcAndUnregisteredFallback(ArrowType storage) throws Exception {
    ArrowType.ExtensionType previous = ExtensionTypeRegistry.lookup(JsonType.EXTENSION_NAME);
    JsonType type = new JsonType(storage);
    JsonType.ensureRegistered();
    try (RootAllocator allocator = new RootAllocator()) {
      Field field = field(storage, true);
      Schema schema = new Schema(Collections.singletonList(field));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      String[] values = {
        "{\"message\":\"你好, a JSON value longer than twelve bytes\"}", null, "null", "42", "[]"
      };
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        JsonVector vector = assertInstanceOf(JsonVector.class, root.getVector(0));
        FieldVector underlying = vector.getUnderlyingVector();
        for (int i = 0; i < values.length; i++) {
          if (values[i] == null) {
            vector.setNull(i);
          } else {
            byte[] bytes = values[i].getBytes(StandardCharsets.UTF_8);
            if (storage instanceof ArrowType.Utf8) {
              assertInstanceOf(VarCharVector.class, underlying).setSafe(i, bytes);
            } else if (storage instanceof ArrowType.LargeUtf8) {
              assertInstanceOf(LargeVarCharVector.class, underlying).setSafe(i, bytes);
            } else {
              assertInstanceOf(ViewVarCharVector.class, underlying).setSafe(i, bytes);
            }
          }
        }
        root.setRowCount(values.length);
        assertEquals(underlying.hashCode(0), vector.hashCode(0));
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      for (boolean registered : new boolean[] {true, false}) {
        if (!registered) {
          ExtensionTypeRegistry.unregister(type);
        }
        try (ArrowStreamReader reader =
            new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
          assertTrue(reader.loadNextBatch());
          FieldVector vector = reader.getVectorSchemaRoot().getVector(0);
          assertEquals(registered ? type : storage, vector.getField().getType());
          assertEquals(field.getMetadata(), vector.getField().getMetadata());
          assertTrue(vector.getField().isNullable());
          assertEquals(values.length, vector.getValueCount());
          for (int i = 0; i < values.length; i++) {
            assertEquals(values[i] == null ? null : new Text(values[i]), vector.getObject(i));
          }
          assertFalse(reader.loadNextBatch());
        }
      }
    } finally {
      ExtensionTypeRegistry.unregister(type);
      if (previous != null) {
        ExtensionTypeRegistry.register(previous);
      }
    }
  }
}
