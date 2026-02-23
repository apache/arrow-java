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
package org.apache.arrow.variant.extension;

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
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.variant.holders.NullableVariantHolder;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestVariantType {
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
    assertNotNull(VariantType.INSTANCE);
  }

  @Test
  void testStorageType() {
    VariantType type = VariantType.INSTANCE;
    assertEquals(ArrowType.Struct.INSTANCE, type.storageType());
    assertInstanceOf(ArrowType.Struct.class, type.storageType());
  }

  @Test
  void testExtensionName() {
    VariantType type = VariantType.INSTANCE;
    assertEquals("parquet.variant", type.extensionName());
  }

  @Test
  void testExtensionEquals() {
    VariantType type1 = VariantType.INSTANCE;
    VariantType type2 = VariantType.INSTANCE;

    assertTrue(type1.extensionEquals(type2));
  }

  @Test
  void testIsComplex() {
    VariantType type = VariantType.INSTANCE;
    assertFalse(type.isComplex());
  }

  @Test
  void testSerialize() {
    VariantType type = VariantType.INSTANCE;
    String serialized = type.serialize();
    assertEquals("", serialized);
  }

  @Test
  void testDeserializeValid() {
    VariantType type = VariantType.INSTANCE;
    ArrowType storageType = ArrowType.Struct.INSTANCE;

    ArrowType deserialized = assertDoesNotThrow(() -> type.deserialize(storageType, ""));
    assertInstanceOf(VariantType.class, deserialized);
    assertEquals(VariantType.INSTANCE, deserialized);
  }

  @Test
  void testDeserializeInvalidStorageType() {
    VariantType type = VariantType.INSTANCE;
    ArrowType wrongStorageType = ArrowType.Utf8.INSTANCE;

    assertThrows(UnsupportedOperationException.class, () -> type.deserialize(wrongStorageType, ""));
  }

  @Test
  void testGetNewVector() {
    VariantType type = VariantType.INSTANCE;
    try (FieldVector vector =
        type.getNewVector("variant_field", FieldType.nullable(type), allocator)) {
      assertInstanceOf(VariantVector.class, vector);
      assertEquals("variant_field", vector.getField().getName());
      assertEquals(type, vector.getField().getType());
    }
  }

  @Test
  void testGetNewVectorWithNullableFieldType() {
    VariantType type = VariantType.INSTANCE;
    FieldType nullableFieldType = FieldType.nullable(type);

    try (FieldVector vector = type.getNewVector("nullable_variant", nullableFieldType, allocator)) {
      assertInstanceOf(VariantVector.class, vector);
      assertEquals("nullable_variant", vector.getField().getName());
      assertTrue(vector.getField().isNullable());
    }
  }

  @Test
  void testGetNewVectorWithNonNullableFieldType() {
    VariantType type = VariantType.INSTANCE;
    FieldType nonNullableFieldType = FieldType.notNullable(type);

    try (FieldVector vector =
        type.getNewVector("non_nullable_variant", nonNullableFieldType, allocator)) {
      assertInstanceOf(VariantVector.class, vector);
      assertEquals("non_nullable_variant", vector.getField().getName());
    }
  }

  @Test
  void testIpcRoundTrip() {
    VariantType type = VariantType.INSTANCE;

    Schema schema = new Schema(Collections.singletonList(Field.nullable("variant", type)));
    byte[] serialized = schema.serializeAsMessage();
    Schema deserialized = Schema.deserializeMessage(ByteBuffer.wrap(serialized));
    assertEquals(schema, deserialized);
  }

  @Test
  void testVectorIpcRoundTrip() throws IOException {
    VariantType type = VariantType.INSTANCE;

    try (FieldVector vector = type.getNewVector("field", FieldType.nullable(type), allocator);
        ArrowBuf metadataBuf1 = allocator.buffer(10);
        ArrowBuf valueBuf1 = allocator.buffer(10);
        ArrowBuf metadataBuf2 = allocator.buffer(10);
        ArrowBuf valueBuf2 = allocator.buffer(10)) {
      VariantVector variantVector = (VariantVector) vector;

      byte[] metadata1 = new byte[] {1, 2, 3};
      byte[] value1 = new byte[] {4, 5, 6, 7};
      metadataBuf1.setBytes(0, metadata1);
      valueBuf1.setBytes(0, value1);

      byte[] metadata2 = new byte[] {8, 9};
      byte[] value2 = new byte[] {10, 11, 12};
      metadataBuf2.setBytes(0, metadata2);
      valueBuf2.setBytes(0, value2);

      NullableVariantHolder holder1 = new NullableVariantHolder();
      holder1.isSet = 1;
      holder1.metadataStart = 0;
      holder1.metadataEnd = metadata1.length;
      holder1.metadataBuffer = metadataBuf1;
      holder1.valueStart = 0;
      holder1.valueEnd = value1.length;
      holder1.valueBuffer = valueBuf1;

      NullableVariantHolder holder2 = new NullableVariantHolder();
      holder2.isSet = 1;
      holder2.metadataStart = 0;
      holder2.metadataEnd = metadata2.length;
      holder2.metadataBuffer = metadataBuf2;
      holder2.valueStart = 0;
      holder2.valueEnd = value2.length;
      holder2.valueBuffer = valueBuf2;

      variantVector.setSafe(0, holder1);
      variantVector.setNull(1);
      variantVector.setSafe(2, holder2);
      variantVector.setValueCount(3);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(variantVector));
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
            new Schema(Collections.singletonList(variantVector.getField())), root.getSchema());

        VariantVector actual = assertInstanceOf(VariantVector.class, root.getVector("field"));
        assertFalse(actual.isNull(0));
        assertTrue(actual.isNull(1));
        assertFalse(actual.isNull(2));

        NullableVariantHolder result1 = new NullableVariantHolder();
        actual.get(0, result1);
        assertEquals(1, result1.isSet);
        assertEquals(metadata1.length, result1.metadataEnd - result1.metadataStart);
        assertEquals(value1.length, result1.valueEnd - result1.valueStart);

        assertNull(actual.getObject(1));

        NullableVariantHolder result2 = new NullableVariantHolder();
        actual.get(2, result2);
        assertEquals(1, result2.isSet);
        assertEquals(metadata2.length, result2.metadataEnd - result2.metadataStart);
        assertEquals(value2.length, result2.valueEnd - result2.valueStart);
      }
    }
  }

  @Test
  void testSingleton() {
    VariantType type1 = VariantType.INSTANCE;
    VariantType type2 = VariantType.INSTANCE;

    // Same instance
    assertSame(type1, type2);
    assertTrue(type1.extensionEquals(type2));
  }

  @Test
  void testExtensionTypeRegistry() {
    // VariantType should be automatically registered via static initializer
    ArrowType.ExtensionType registeredType =
        ExtensionTypeRegistry.lookup(VariantType.EXTENSION_NAME);
    assertNotNull(registeredType);
    assertInstanceOf(VariantType.class, registeredType);
    assertEquals(VariantType.INSTANCE, registeredType);
  }

  @Test
  void testFieldMetadata() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    metadata.put("key2", "value2");

    FieldType fieldType = new FieldType(true, VariantType.INSTANCE, null, metadata);
    try (VariantVector vector = new VariantVector("test", allocator)) {
      Field field = new Field("test", fieldType, VariantVector.createVariantChildFields());

      // Field metadata includes both custom metadata and extension type metadata
      Map<String, String> fieldMetadata = field.getMetadata();
      assertEquals("value1", fieldMetadata.get("key1"));
      assertEquals("value2", fieldMetadata.get("key2"));
      // Extension type metadata is also present
      assertTrue(fieldMetadata.containsKey("ARROW:extension:name"));
      assertTrue(fieldMetadata.containsKey("ARROW:extension:metadata"));
    }
  }

  @Test
  void testFieldChildren() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      Field field = vector.getField();

      assertNotNull(field.getChildren());
      assertEquals(2, field.getChildren().size());

      Field metadataField = field.getChildren().get(0);
      assertEquals(VariantVector.METADATA_VECTOR_NAME, metadataField.getName());
      assertEquals(ArrowType.Binary.INSTANCE, metadataField.getType());

      Field valueField = field.getChildren().get(1);
      assertEquals(VariantVector.VALUE_VECTOR_NAME, valueField.getName());
      assertEquals(ArrowType.Binary.INSTANCE, valueField.getType());
    }
  }
}
