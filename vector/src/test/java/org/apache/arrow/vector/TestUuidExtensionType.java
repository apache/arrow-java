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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.extension.NullableUuidHolder;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.extension.UuidVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.validate.ValidateVectorVisitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestUuidExtensionType {
  BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  /** Test that a custom UUID type can be round-tripped through a temporary file. */
  @Test
  public void roundtripUuid() throws IOException {
    UuidType.ensureRegistered();
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", new UuidType())));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();
      UuidVector vector = (UuidVector) root.getVector("a");
      vector.setValueCount(2);
      vector.set(0, u1);
      vector.set(1, u2);
      root.setRowCount(2);

      final File file = File.createTempFile("uuidtest", ".arrow");
      try (final WritableByteChannel channel =
              FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
          final ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      try (final SeekableByteChannel channel =
              Files.newByteChannel(Paths.get(file.getAbsolutePath()));
          final ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        reader.loadNextBatch();
        final VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        assertEquals(root.getSchema(), readerRoot.getSchema());

        final Field field = readerRoot.getSchema().getFields().get(0);
        final UuidType expectedType = new UuidType();
        assertEquals(
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
            expectedType.extensionName());
        assertEquals(
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
            expectedType.serialize());

        final ExtensionTypeVector deserialized =
            (ExtensionTypeVector) readerRoot.getFieldVectors().get(0);
        assertEquals(vector.getValueCount(), deserialized.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
          assertEquals(vector.isNull(i), deserialized.isNull(i));
          if (!vector.isNull(i)) {
            assertEquals(vector.getObject(i), deserialized.getObject(i));
          }
        }
      }
    }
  }

  /** Test that a custom UUID type can be read as its underlying type. */
  @Test
  public void readUnderlyingType() throws IOException {
    UuidType.ensureRegistered();
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", new UuidType())));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();
      UuidVector vector = (UuidVector) root.getVector("a");
      vector.setValueCount(2);
      vector.set(0, u1);
      vector.set(1, u2);
      root.setRowCount(2);

      final File file = File.createTempFile("uuidtest", ".arrow");
      try (final WritableByteChannel channel =
              FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
          final ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      ExtensionTypeRegistry.unregister(new UuidType());

      try (final SeekableByteChannel channel =
              Files.newByteChannel(Paths.get(file.getAbsolutePath()));
          final ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        reader.loadNextBatch();
        final VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        assertEquals(1, readerRoot.getSchema().getFields().size());
        assertEquals("a", readerRoot.getSchema().getFields().get(0).getName());
        assertTrue(
            readerRoot.getSchema().getFields().get(0).getType()
                instanceof ArrowType.FixedSizeBinary);
        assertEquals(
            16,
            ((ArrowType.FixedSizeBinary) readerRoot.getSchema().getFields().get(0).getType())
                .getByteWidth());

        final Field field = readerRoot.getSchema().getFields().get(0);
        final UuidType expectedType = new UuidType();
        assertEquals(
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
            expectedType.extensionName());
        assertEquals(
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
            expectedType.serialize());

        final FixedSizeBinaryVector deserialized =
            (FixedSizeBinaryVector) readerRoot.getFieldVectors().get(0);
        assertEquals(vector.getValueCount(), deserialized.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
          assertEquals(vector.isNull(i), deserialized.isNull(i));
          if (!vector.isNull(i)) {
            final UUID uuid = vector.getObject(i);
            final ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(uuid.getMostSignificantBits());
            bb.putLong(uuid.getLeastSignificantBits());
            assertArrayEquals(bb.array(), deserialized.get(i));
          }
        }
      }
    }
  }

  @Test
  public void testNullCheck() {
    NullPointerException e =
        assertThrows(
            NullPointerException.class,
            () -> {
              try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                  final ExtensionTypeVector vector = new UuidVector("uuid", allocator, null)) {
                vector.getField();
                vector.allocateNewSafe();
              }
            });
    assertTrue(e.getMessage().contains("underlyingVector cannot be null."));
  }

  @Test
  public void testVectorCompare() {
    UuidType.ensureRegistered();
    UuidType uuidType = new UuidType();
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        UuidVector a1 =
            (UuidVector) uuidType.getNewVector("a", FieldType.nullable(uuidType), allocator);
        UuidVector a2 =
            (UuidVector) uuidType.getNewVector("a", FieldType.nullable(uuidType), allocator);
        UuidVector bb =
            (UuidVector) uuidType.getNewVector("a", FieldType.nullable(uuidType), allocator)) {
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();

      // Test out type and vector validation visitors for an ExtensionTypeVector
      ValidateVectorVisitor validateVisitor = new ValidateVectorVisitor();
      validateVisitor.visit(a1, null);

      a1.setValueCount(2);
      a1.set(0, u1);
      a1.set(1, u2);

      a2.setValueCount(2);
      a2.set(0, u1);
      a2.set(1, u2);

      bb.setValueCount(2);
      bb.set(0, u2);
      bb.set(1, u1);

      Range range = new Range(0, 0, a1.getValueCount());
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(a1, a2);
      assertTrue(visitor.rangeEquals(range));

      visitor = new RangeEqualsVisitor(a1, bb);
      assertFalse(visitor.rangeEquals(range));

      // Test out vector appender
      VectorBatchAppender.batchAppend(a1, a2, bb);
      assertEquals(6, a1.getValueCount());
      validateVisitor.visit(a1, null);
    }
  }

  @Test
  public void testUuidTypeProperties() {
    UuidType uuidType = new UuidType();

    // Test basic properties
    assertEquals("uuid", uuidType.extensionName());
    assertEquals(new ArrowType.FixedSizeBinary(16), uuidType.storageType());
    assertEquals("", uuidType.serialize());
    assertFalse(uuidType.isComplex());

    // Test equality
    UuidType anotherUuidType = new UuidType();
    assertTrue(uuidType.extensionEquals(anotherUuidType));

    // Test deserialization
    ArrowType deserializedType = uuidType.deserialize(uuidType.storageType(), "");
    assertTrue(deserializedType instanceof UuidType);
  }

  @Test
  public void testUuidVectorBasicOperations() {
    UuidType.ensureRegistered();
    try (UuidVector vector = new UuidVector("test", allocator)) {
      vector.allocateNew();

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      // Test setting values
      vector.set(0, uuid1);
      vector.set(1, uuid2);
      vector.setNull(2);
      vector.setValueCount(3);

      // Test getting values
      assertEquals(uuid1, vector.getObject(0));
      assertEquals(uuid2, vector.getObject(1));
      assertTrue(vector.isNull(2));

      // Test null checks
      assertFalse(vector.isNull(0));
      assertFalse(vector.isNull(1));
      assertTrue(vector.isNull(2));

      // Test value count
      assertEquals(3, vector.getValueCount());
    }
  }

  @Test
  public void testUuidVectorWithNullValues() {
    UuidType.ensureRegistered();
    try (UuidVector vector = new UuidVector("test", allocator)) {
      vector.allocateNew();

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      // Set some values and nulls
      vector.set(0, uuid1);
      vector.setNull(1);
      vector.set(2, uuid2);
      vector.setNull(3);
      vector.setNull(4);
      vector.setValueCount(5);

      // Verify values and nulls
      assertEquals(uuid1, vector.getObject(0));
      assertTrue(vector.isNull(1));
      assertEquals(uuid2, vector.getObject(2));
      assertTrue(vector.isNull(3));
      assertTrue(vector.isNull(4));

      assertEquals(5, vector.getValueCount());
    }
  }

  @Test
  public void testUuidVectorHashCode() {
    UuidType.ensureRegistered();
    try (UuidVector vector1 = new UuidVector("test1", allocator);
        UuidVector vector2 = new UuidVector("test2", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      UUID uuid = UUID.randomUUID();

      // Set same UUID in both vectors
      vector1.set(0, uuid);
      vector2.set(0, uuid);
      vector1.setValueCount(2);
      vector2.setValueCount(2);

      // Hash codes should be equal for same UUID values
      assertEquals(vector1.hashCode(0), vector2.hashCode(0));
    }
  }

  @Test
  public void testUuidVectorCopyFromSafe() {
    UuidType.ensureRegistered();
    try (UuidVector source = new UuidVector("source", allocator);
        UuidVector target = new UuidVector("target", allocator)) {

      source.allocateNew();
      target.allocateNew();

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      // Set values in source
      source.set(0, uuid1);
      source.set(1, uuid2);
      source.setNull(2);
      source.setValueCount(3);

      // Copy values to target
      target.copyFromSafe(0, 0, source);
      target.copyFromSafe(1, 1, source);
      target.copyFromSafe(2, 2, source);
      target.setValueCount(3);

      // Verify copied values
      assertEquals(uuid1, target.getObject(0));
      assertEquals(uuid2, target.getObject(1));
      assertTrue(target.isNull(2));
    }
  }

  @Test
  public void testNullableUuidHolder() {
    UuidType.ensureRegistered();
    try (UuidVector vector = new UuidVector("test", allocator)) {
      vector.allocateNew();

      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      // Set some values and nulls
      vector.set(0, uuid1);
      vector.setNull(1);
      vector.set(2, uuid2);
      vector.setValueCount(3);

      NullableUuidHolder holder = new NullableUuidHolder();

      // Test getting non-null value
      vector.get(0, holder);
      assertEquals(1, holder.isSet);
      assertNotNull(holder.buffer);
      assertEquals(16, holder.buffer.capacity());

      // Test getting null value
      vector.get(1, holder);
      assertEquals(0, holder.isSet);

      // Test getting another non-null value
      vector.get(2, holder);
      assertEquals(1, holder.isSet);
      assertNotNull(holder.buffer);
      assertEquals(16, holder.buffer.capacity());
    }
  }
}
