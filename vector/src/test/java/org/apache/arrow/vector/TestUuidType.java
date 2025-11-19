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

import static org.apache.arrow.vector.TestUtils.ensureRegistered;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.UuidUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestUuidType {
  BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  @Test
  void testConstants() {
    assertEquals("arrow.uuid", UuidType.EXTENSION_NAME);
    assertNotNull(UuidType.INSTANCE);
    assertNotNull(UuidType.STORAGE_TYPE);
    assertInstanceOf(ArrowType.FixedSizeBinary.class, UuidType.STORAGE_TYPE);
    assertEquals(
        UuidType.UUID_BYTE_WIDTH,
        ((ArrowType.FixedSizeBinary) UuidType.STORAGE_TYPE).getByteWidth());
  }

  @Test
  void testStorageType() {
    UuidType type = new UuidType();
    assertEquals(UuidType.STORAGE_TYPE, type.storageType());
    assertInstanceOf(ArrowType.FixedSizeBinary.class, type.storageType());
  }

  @Test
  void testExtensionName() {
    UuidType type = new UuidType();
    assertEquals("arrow.uuid", type.extensionName());
  }

  @Test
  void testExtensionEquals() {
    UuidType type1 = new UuidType();
    UuidType type2 = new UuidType();
    UuidType type3 = UuidType.INSTANCE;

    assertTrue(type1.extensionEquals(type2));
    assertTrue(type1.extensionEquals(type3));
    assertTrue(type2.extensionEquals(type3));
  }

  @Test
  void testIsComplex() {
    UuidType type = new UuidType();
    assertFalse(type.isComplex());
  }

  @Test
  void testSerialize() {
    UuidType type = new UuidType();
    String serialized = type.serialize();
    assertEquals("", serialized);
  }

  @Test
  void testDeserializeValid() {
    UuidType type = new UuidType();
    ArrowType storageType = new ArrowType.FixedSizeBinary(UuidType.UUID_BYTE_WIDTH);

    ArrowType deserialized = assertDoesNotThrow(() -> type.deserialize(storageType, ""));
    assertInstanceOf(UuidType.class, deserialized);
    assertEquals(UuidType.INSTANCE, deserialized);
  }

  @Test
  void testDeserializeInvalidStorageType() {
    UuidType type = new UuidType();
    ArrowType wrongStorageType = new ArrowType.FixedSizeBinary(32);

    assertThrows(UnsupportedOperationException.class, () -> type.deserialize(wrongStorageType, ""));
  }

  @Test
  void testGetNewVector() {
    UuidType type = new UuidType();
    try (FieldVector vector =
        type.getNewVector("uuid_field", FieldType.nullable(type), allocator)) {
      assertInstanceOf(UuidVector.class, vector);
      assertEquals("uuid_field", vector.getField().getName());
      assertEquals(type, vector.getField().getType());
    }
  }

  @Test
  void testVectorOperations() {
    UuidType type = new UuidType();
    try (FieldVector vector =
        type.getNewVector("uuid_field", FieldType.nullable(type), allocator)) {
      UuidVector uuidVector = (UuidVector) vector;

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      uuidVector.setSafe(0, uuid1);
      uuidVector.setSafe(1, uuid2);
      uuidVector.setNull(2);
      uuidVector.setValueCount(3);

      assertEquals(uuid1, uuidVector.getObject(0));
      assertEquals(uuid2, uuidVector.getObject(1));
      assertNull(uuidVector.getObject(2));
      assertFalse(uuidVector.isNull(0));
      assertFalse(uuidVector.isNull(1));
      assertTrue(uuidVector.isNull(2));
    }
  }

  @Test
  void testIpcRoundTrip() {
    UuidType type = UuidType.INSTANCE;
    ensureRegistered(type);

    Schema schema = new Schema(Collections.singletonList(Field.nullable("uuid", type)));
    byte[] serialized = schema.serializeAsMessage();
    Schema deserialized = Schema.deserializeMessage(ByteBuffer.wrap(serialized));
    assertEquals(schema, deserialized);
  }

  @Test
  void testVectorIpcRoundTrip() throws IOException {
    UuidType type = UuidType.INSTANCE;
    ensureRegistered(type);

    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    try (FieldVector vector = type.getNewVector("field", FieldType.nullable(type), allocator)) {
      UuidVector uuidVector = (UuidVector) vector;
      uuidVector.setSafe(0, uuid1);
      uuidVector.setNull(1);
      uuidVector.setSafe(2, uuid2);
      uuidVector.setValueCount(3);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(uuidVector));
          ArrowStreamWriter writer =
              new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), baos)) {
        writer.start();
        writer.writeBatch();
      }

      try (ArrowStreamReader reader =
          new ArrowStreamReader(new ByteArrayInputStream(baos.toByteArray()), allocator)) {
        assertTrue(reader.loadNextBatch());
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());
        assertEquals(
            new Schema(Collections.singletonList(uuidVector.getField())), root.getSchema());

        UuidVector actual = assertInstanceOf(UuidVector.class, root.getVector("field"));
        assertFalse(actual.isNull(0));
        assertTrue(actual.isNull(1));
        assertFalse(actual.isNull(2));
        assertEquals(uuid1, actual.getObject(0));
        assertNull(actual.getObject(1));
        assertEquals(uuid2, actual.getObject(2));
      }
    }
  }

  @Test
  void testVectorByteArrayOperations() {
    UuidType type = new UuidType();
    try (FieldVector vector =
        type.getNewVector("uuid_field", FieldType.nullable(type), allocator)) {
      UuidVector uuidVector = (UuidVector) vector;

      UUID uuid = UUID.randomUUID();
      byte[] uuidBytes = UuidUtility.getBytesFromUUID(uuid);

      uuidVector.setSafe(0, uuidBytes);
      uuidVector.setValueCount(1);

      assertEquals(uuid, uuidVector.getObject(0));

      // Verify the bytes match
      byte[] actualBytes = new byte[UuidType.UUID_BYTE_WIDTH];
      uuidVector.get(0).getBytes(0, actualBytes);
      assertArrayEquals(uuidBytes, actualBytes);
    }
  }

  @Test
  void testGetNewVectorWithCustomFieldType() {
    UuidType type = new UuidType();
    FieldType fieldType = new FieldType(false, type, null);

    try (FieldVector vector = type.getNewVector("non_nullable_uuid", fieldType, allocator)) {
      assertInstanceOf(UuidVector.class, vector);
      assertEquals("non_nullable_uuid", vector.getField().getName());
      assertFalse(vector.getField().isNullable());
    }
  }

  @Test
  void testSingleton() {
    UuidType type1 = UuidType.INSTANCE;
    UuidType type2 = UuidType.INSTANCE;

    // Same instance
    assertSame(type1, type2);
    assertTrue(type1.extensionEquals(type2));
  }

  @Test
  void testUnderlyingVector() {
    UuidType type = new UuidType();
    try (FieldVector vector =
        type.getNewVector("uuid_field", FieldType.nullable(type), allocator)) {
      UuidVector uuidVector = (UuidVector) vector;
      FixedSizeBinaryVector underlying = uuidVector.getUnderlyingVector();

      assertInstanceOf(FixedSizeBinaryVector.class, underlying);
      assertEquals(UuidType.UUID_BYTE_WIDTH, underlying.getByteWidth());
    }
  }
}
