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
package org.apache.arrow.vector.types.pojo;

import static org.apache.arrow.vector.TestUtils.ensureRegistered;
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
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.extension.VariantType;
import org.apache.arrow.vector.extension.VariantVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.validate.ValidateVectorVisitor;
import org.apache.arrow.vector.variant.TestVariant;
import org.apache.arrow.vector.variant.Variant;
import org.junit.jupiter.api.Test;

public class TestExtensionType {
  /** Test that a custom UUID type can be round-tripped through a temporary file. */
  @Test
  public void roundtripUuid() throws IOException {
    ensureRegistered(UuidType.INSTANCE);
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", UuidType.INSTANCE)));
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
        final UuidType expectedType = UuidType.INSTANCE;
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
    ensureRegistered(UuidType.INSTANCE);
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", UuidType.INSTANCE)));
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

      ExtensionTypeRegistry.unregister(UuidType.INSTANCE);

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
        final UuidType expectedType = UuidType.INSTANCE;
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

  /** Test that a custom Location type can be round-tripped through a temporary file. */
  @Test
  public void roundtripLocation() throws IOException {
    ExtensionTypeRegistry.register(new LocationType());
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("location", new LocationType())));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      LocationVector vector = (LocationVector) root.getVector("location");
      vector.allocateNew();
      vector.set(0, 34.073814f, -118.240784f);
      vector.set(2, 37.768056f, -122.3875f);
      vector.set(3, 40.739716f, -73.840782f);
      vector.setValueCount(4);
      root.setRowCount(4);

      final File file = File.createTempFile("locationtest", ".arrow");
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
        final LocationType expectedType = new LocationType();
        assertEquals(
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
            expectedType.extensionName());
        assertEquals(
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
            expectedType.serialize());

        final ExtensionTypeVector deserialized =
            (ExtensionTypeVector) readerRoot.getFieldVectors().get(0);
        assertTrue(deserialized instanceof LocationVector);
        assertEquals("location", deserialized.getName());
        StructVector deserStruct = (StructVector) deserialized.getUnderlyingVector();
        assertNotNull(deserStruct.getChild("Latitude"));
        assertNotNull(deserStruct.getChild("Longitude"));
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

  @Test
  public void testVectorCompare() {
    UuidType uuidType = UuidType.INSTANCE;
    ExtensionTypeRegistry.register(uuidType);
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
  public void roundtripVariant() throws IOException {
    ensureRegistered(VariantType.INSTANCE);
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", VariantType.INSTANCE)));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      VariantVector vector = (VariantVector) root.getVector("a");
      vector.allocateNew();

      setVariant(vector, 0, TestVariant.variantString("hello"));
      setVariant(vector, 1, TestVariant.variantString("world"));
      vector.setValueCount(2);
      root.setRowCount(2);

      final File file = File.createTempFile("varianttest", ".arrow");
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
        final VariantType expectedType = VariantType.INSTANCE;
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

  @Test
  public void readVariantAsUnderlyingType() throws IOException {
    ensureRegistered(VariantType.INSTANCE);
    final Schema schema =
        new Schema(Collections.singletonList(VariantVector.createVariantField("a")));
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      VariantVector vector = (VariantVector) root.getVector("a");
      vector.allocateNew();

      setVariant(vector, 0, TestVariant.variantString("hello"));
      vector.setValueCount(1);
      root.setRowCount(1);

      final File file = File.createTempFile("varianttest", ".arrow");
      try (final WritableByteChannel channel =
              FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE);
          final ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }

      ExtensionTypeRegistry.unregister(VariantType.INSTANCE);

      try (final SeekableByteChannel channel =
              Files.newByteChannel(Paths.get(file.getAbsolutePath()));
          final ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        reader.loadNextBatch();
        VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();

        // Verify schema properties
        assertEquals(1, readRoot.getSchema().getFields().size());
        assertEquals("a", readRoot.getSchema().getFields().get(0).getName());
        assertTrue(readRoot.getSchema().getFields().get(0).getType() instanceof ArrowType.Struct);

        // Verify extension metadata is preserved
        final Field field = readRoot.getSchema().getFields().get(0);
        assertEquals(
            VariantType.EXTENSION_NAME,
            field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME));
        assertEquals("", field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA));

        // Verify vector type and row count
        assertEquals(1, readRoot.getRowCount());
        FieldVector readVector = readRoot.getVector("a");
        assertEquals(StructVector.class, readVector.getClass());

        // Verify value count matches
        StructVector structVector = (StructVector) readVector;
        assertEquals(vector.getValueCount(), structVector.getValueCount());

        // Verify the underlying data can be accessed from child vectors
        VarBinaryVector metadataVector =
            structVector.getChild(VariantVector.METADATA_VECTOR_NAME, VarBinaryVector.class);
        VarBinaryVector valueVector =
            structVector.getChild(VariantVector.VALUE_VECTOR_NAME, VarBinaryVector.class);
        assertNotNull(metadataVector);
        assertNotNull(valueVector);
        assertEquals(1, metadataVector.getValueCount());
        assertEquals(1, valueVector.getValueCount());
      }
    }
  }

  @Test
  public void testVariantVectorCompare() {
    VariantType variantType = VariantType.INSTANCE;
    ExtensionTypeRegistry.register(variantType);
    Variant hello = TestVariant.variantString("hello");
    Variant world = TestVariant.variantString("world");
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        VariantVector a1 =
            (VariantVector)
                variantType.getNewVector("a", FieldType.nullable(variantType), allocator);
        VariantVector a2 =
            (VariantVector)
                variantType.getNewVector("a", FieldType.nullable(variantType), allocator);
        VariantVector bb =
            (VariantVector)
                variantType.getNewVector("a", FieldType.nullable(variantType), allocator)) {

      ValidateVectorVisitor validateVisitor = new ValidateVectorVisitor();
      validateVisitor.visit(a1, null);

      a1.allocateNew();
      a2.allocateNew();
      bb.allocateNew();

      setVariant(a1, 0, hello);
      setVariant(a1, 1, world);
      a1.setValueCount(2);

      setVariant(a2, 0, hello);
      setVariant(a2, 1, world);
      a2.setValueCount(2);

      setVariant(bb, 0, world);
      setVariant(bb, 1, hello);
      bb.setValueCount(2);

      Range range = new Range(0, 0, a1.getValueCount());
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(a1, a2);
      assertTrue(visitor.rangeEquals(range));

      visitor = new RangeEqualsVisitor(a1, bb);
      assertFalse(visitor.rangeEquals(range));

      VectorBatchAppender.batchAppend(a1, a2, bb);
      assertEquals(6, a1.getValueCount());
      validateVisitor.visit(a1, null);
    }
  }

  @Test
  public void testVariantCopyAsValueThrowsException() {
    ensureRegistered(VariantType.INSTANCE);
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        VariantVector vector = new VariantVector("variant", allocator)) {
      vector.allocateNew();
      setVariant(vector, 0, TestVariant.variantString("hello"));
      vector.setValueCount(1);

      var reader = vector.getReader();
      reader.setPosition(0);

      assertThrows(
          IllegalArgumentException.class, () -> reader.copyAsValue((BaseWriter.StructWriter) null));
    }
  }

  private static void setVariant(VariantVector vector, int index, Variant variant) {
    vector.setSafe(index, variant);
  }

  static class LocationType extends ExtensionType {

    @Override
    public ArrowType storageType() {
      return Struct.INSTANCE;
    }

    @Override
    public String extensionName() {
      return "location";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
      return other instanceof LocationType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
      if (!storageType.equals(storageType())) {
        throw new UnsupportedOperationException(
            "Cannot construct LocationType from underlying type " + storageType);
      }
      return new LocationType();
    }

    @Override
    public String serialize() {
      return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
      return new LocationVector(name, allocator);
    }

    @Override
    public FieldWriter getNewFieldWriter(ValueVector vector) {
      throw new UnsupportedOperationException("Not yet implemented.");
    }
  }

  public static class LocationVector extends ExtensionTypeVector<StructVector>
      implements ValueIterableVector<java.util.Map<String, ?>> {

    private static StructVector buildUnderlyingVector(String name, BufferAllocator allocator) {
      final StructVector underlyingVector =
          new StructVector(name, allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), null);
      underlyingVector.addOrGet(
          "Latitude",
          FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
          Float4Vector.class);
      underlyingVector.addOrGet(
          "Longitude",
          FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
          Float4Vector.class);
      return underlyingVector;
    }

    public LocationVector(String name, BufferAllocator allocator) {
      super(name, allocator, buildUnderlyingVector(name, allocator));
    }

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      return getUnderlyingVector().hashCode(index, hasher);
    }

    @Override
    public java.util.Map<String, ?> getObject(int index) {
      return getUnderlyingVector().getObject(index);
    }

    public void set(int index, float latitude, float longitude) {
      getUnderlyingVector().getChild("Latitude", Float4Vector.class).set(index, latitude);
      getUnderlyingVector().getChild("Longitude", Float4Vector.class).set(index, longitude);
      getUnderlyingVector().setIndexDefined(index);
    }
  }
}
