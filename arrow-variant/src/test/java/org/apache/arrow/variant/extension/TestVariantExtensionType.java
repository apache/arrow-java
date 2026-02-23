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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.variant.TestVariant;
import org.apache.arrow.variant.Variant;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter;
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
import org.junit.jupiter.api.Test;

public class TestVariantExtensionType {

  private static void ensureRegistered(ArrowType.ExtensionType type) {
    if (ExtensionTypeRegistry.lookup(type.extensionName()) == null) {
      ExtensionTypeRegistry.register(type);
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

      vector.setSafe(0, TestVariant.variantString("hello"));
      vector.setSafe(1, TestVariant.variantString("world"));
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

      vector.setSafe(0, TestVariant.variantString("hello"));
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

      a1.setSafe(0, hello);
      a1.setSafe(1, world);
      a1.setValueCount(2);

      a2.setSafe(0, hello);
      a2.setSafe(1, world);
      a2.setValueCount(2);

      bb.setSafe(0, world);
      bb.setSafe(1, hello);
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
      vector.setSafe(0, TestVariant.variantString("hello"));
      vector.setValueCount(1);

      var reader = vector.getReader();
      reader.setPosition(0);

      assertThrows(
          IllegalArgumentException.class, () -> reader.copyAsValue((BaseWriter.StructWriter) null));
    }
  }
}
