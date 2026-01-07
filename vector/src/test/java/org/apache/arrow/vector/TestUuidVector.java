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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.impl.NullableUuidHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.UuidReaderImpl;
import org.apache.arrow.vector.complex.impl.UuidWriterImpl;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.holders.NullableUuidHolder;
import org.apache.arrow.vector.holders.UuidHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.UuidUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for UuidVector, UuidWriterImpl, and UuidReaderImpl. */
class TestUuidVector {

  private BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  // ========== Writer Tests ==========

  @Test
  void testWriteToExtensionVector() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      UUID uuid = UUID.randomUUID();
      ByteBuffer bb = ByteBuffer.allocate(UuidType.UUID_BYTE_WIDTH);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());

      // Allocate ArrowBuf for the holder
      try (ArrowBuf buf = allocator.buffer(UuidType.UUID_BYTE_WIDTH)) {
        buf.setBytes(0, bb.array());

        UuidHolder holder = new UuidHolder();
        holder.buffer = buf;

        writer.write(holder);
        UUID result = vector.getObject(0);
        assertEquals(uuid, result);
      }
    }
  }

  @Test
  void testWriteExtensionWithUUID() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      UUID uuid = UUID.randomUUID();
      writer.setPosition(0);
      writer.writeExtension(uuid);

      UUID result = vector.getObject(0);
      assertEquals(uuid, result);
      assertEquals(1, vector.getValueCount());
    }
  }

  @Test
  void testWriteExtensionWithByteArray() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      UUID uuid = UUID.randomUUID();
      byte[] uuidBytes = UuidUtility.getBytesFromUUID(uuid);

      writer.setPosition(0);
      writer.writeExtension(uuidBytes);

      UUID result = vector.getObject(0);
      assertEquals(uuid, result);
      assertEquals(1, vector.getValueCount());
    }
  }

  @Test
  void testWriteExtensionWithArrowBuf() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector);
        ArrowBuf buf = allocator.buffer(UuidType.UUID_BYTE_WIDTH)) {
      UUID uuid = UUID.randomUUID();
      byte[] uuidBytes = UuidUtility.getBytesFromUUID(uuid);
      buf.setBytes(0, uuidBytes);

      writer.setPosition(0);
      writer.writeExtension(buf);

      UUID result = vector.getObject(0);
      assertEquals(uuid, result);
      assertEquals(1, vector.getValueCount());
    }
  }

  @Test
  void testWriteExtensionWithUnsupportedType() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      writer.setPosition(0);

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> writer.writeExtension("invalid-type"));

      assertTrue(
          exception.getMessage().contains("Unsupported value type for UUID: java.lang.String"));
    }
  }

  @Test
  void testWriteExtensionMultipleValues() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      UUID uuid3 = UUID.randomUUID();

      writer.setPosition(0);
      writer.writeExtension(uuid1);
      writer.setPosition(1);
      writer.writeExtension(uuid2);
      writer.setPosition(2);
      writer.writeExtension(uuid3);

      assertEquals(uuid1, vector.getObject(0));
      assertEquals(uuid2, vector.getObject(1));
      assertEquals(uuid3, vector.getObject(2));
      assertEquals(3, vector.getValueCount());
    }
  }

  @Test
  void testWriteWithUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector);
        ArrowBuf buf = allocator.buffer(UuidType.UUID_BYTE_WIDTH)) {
      UUID uuid = UUID.randomUUID();
      byte[] uuidBytes = UuidUtility.getBytesFromUUID(uuid);
      buf.setBytes(0, uuidBytes);

      UuidHolder holder = new UuidHolder();
      holder.buffer = buf;
      holder.isSet = 1;

      writer.setPosition(0);
      writer.write(holder);

      UUID result = vector.getObject(0);
      assertEquals(uuid, result);
      assertEquals(1, vector.getValueCount());
    }
  }

  @Test
  void testWriteWithNullableUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector);
        ArrowBuf buf = allocator.buffer(UuidType.UUID_BYTE_WIDTH)) {
      UUID uuid = UUID.randomUUID();
      byte[] uuidBytes = UuidUtility.getBytesFromUUID(uuid);
      buf.setBytes(0, uuidBytes);

      NullableUuidHolder holder = new NullableUuidHolder();
      holder.buffer = buf;
      holder.isSet = 1;

      writer.setPosition(0);
      writer.write(holder);

      UUID result = vector.getObject(0);
      assertEquals(uuid, result);
      assertEquals(1, vector.getValueCount());
    }
  }

  @Test
  void testWriteWithNullableUuidHolderNull() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      NullableUuidHolder holder = new NullableUuidHolder();
      holder.isSet = 0;

      writer.setPosition(0);
      writer.write(holder);

      assertTrue(vector.isNull(0));
      assertEquals(1, vector.getValueCount());
    }
  }

  // ========== Reader Tests ==========

  @Test
  void testReaderCopyAsValueExtensionVector() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator);
        UuidVector vectorForRead = new UuidVector("test2", allocator);
        UuidWriterImpl writer = new UuidWriterImpl(vector)) {
      UUID uuid = UUID.randomUUID();
      vectorForRead.setValueCount(1);
      vectorForRead.set(0, uuid);
      UuidReaderImpl reader = (UuidReaderImpl) vectorForRead.getReader();
      reader.copyAsValue(writer);
      UuidReaderImpl reader2 = (UuidReaderImpl) vector.getReader();
      UuidHolder holder = new UuidHolder();
      reader2.read(0, holder);
      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start);
      assertEquals(uuid, actualUuid);
    }
  }

  @Test
  void testReaderReadWithUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setValueCount(1);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      reader.setPosition(0);

      UuidHolder holder = new UuidHolder();
      reader.read(holder);

      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start);
      assertEquals(uuid, actualUuid);
      assertEquals(1, holder.isSet);
    }
  }

  @Test
  void testReaderReadWithNullableUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setValueCount(1);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      reader.setPosition(0);

      NullableUuidHolder holder = new NullableUuidHolder();
      reader.read(holder);

      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start);
      assertEquals(uuid, actualUuid);
      assertEquals(1, holder.isSet);
    }
  }

  @Test
  void testReaderReadWithNullableUuidHolderNull() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      vector.setNull(0);
      vector.setValueCount(1);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      reader.setPosition(0);

      NullableUuidHolder holder = new NullableUuidHolder();
      reader.read(holder);

      assertEquals(0, holder.isSet);
    }
  }

  @Test
  void testReaderReadWithArrayIndexUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      UUID uuid3 = UUID.randomUUID();

      vector.setSafe(0, uuid1);
      vector.setSafe(1, uuid2);
      vector.setSafe(2, uuid3);
      vector.setValueCount(3);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();

      UuidHolder holder = new UuidHolder();
      reader.read(1, holder);

      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start);
      assertEquals(uuid2, actualUuid);
      assertEquals(1, holder.isSet);
    }
  }

  @Test
  void testReaderReadWithArrayIndexNullableUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      vector.setSafe(0, uuid1);
      vector.setNull(1);
      vector.setSafe(2, uuid2);
      vector.setValueCount(3);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();

      NullableUuidHolder holder1 = new NullableUuidHolder();
      reader.read(0, holder1);
      assertEquals(uuid1, UuidUtility.uuidFromArrowBuf(holder1.buffer, holder1.start));
      assertEquals(1, holder1.isSet);

      NullableUuidHolder holder2 = new NullableUuidHolder();
      reader.read(1, holder2);
      assertEquals(0, holder2.isSet);

      NullableUuidHolder holder3 = new NullableUuidHolder();
      reader.read(2, holder3);
      assertEquals(uuid2, UuidUtility.uuidFromArrowBuf(holder3.buffer, holder3.start));
      assertEquals(1, holder3.isSet);
    }
  }

  @Test
  void testReaderReadWithUnsupportedHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setValueCount(1);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      reader.setPosition(0);

      // Create a mock unsupported holder
      ExtensionHolder unsupportedHolder =
          new ExtensionHolder() {
            @Override
            public ArrowType type() {
              return null;
            }
          };

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> reader.read(unsupportedHolder));

      assertTrue(exception.getMessage().contains("Unsupported holder type for UuidReader"));
    }
  }

  @Test
  void testReaderReadWithArrayIndexUnsupportedHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setValueCount(1);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();

      // Create a mock unsupported holder
      ExtensionHolder unsupportedHolder =
          new ExtensionHolder() {
            @Override
            public ArrowType type() {
              return null;
            }
          };

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> reader.read(0, unsupportedHolder));

      assertTrue(exception.getMessage().contains("Unsupported holder type for UuidReader"));
    }
  }

  @Test
  void testReaderIsSet() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setNull(1);
      vector.setSafe(2, uuid);
      vector.setValueCount(3);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();

      reader.setPosition(0);
      assertTrue(reader.isSet());

      reader.setPosition(1);
      assertFalse(reader.isSet());

      reader.setPosition(2);
      assertTrue(reader.isSet());
    }
  }

  @Test
  void testReaderReadObject() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      vector.setSafe(0, uuid1);
      vector.setNull(1);
      vector.setSafe(2, uuid2);
      vector.setValueCount(3);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();

      reader.setPosition(0);
      assertEquals(uuid1, reader.readObject());

      reader.setPosition(1);
      assertNull(reader.readObject());

      reader.setPosition(2);
      assertEquals(uuid2, reader.readObject());
    }
  }

  @Test
  void testReaderGetMinorType() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      assertEquals(vector.getMinorType(), reader.getMinorType());
    }
  }

  @Test
  void testReaderGetField() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      assertEquals(vector.getField(), reader.getField());
      assertEquals("test", reader.getField().getName());
    }
  }

  @Test
  void testHolderStartOffsetWithMultipleValues() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      UUID uuid3 = UUID.randomUUID();

      vector.setSafe(0, uuid1);
      vector.setSafe(1, uuid2);
      vector.setSafe(2, uuid3);
      vector.setValueCount(3);

      // Test UuidHolder with different indices
      UuidHolder holder1 = new UuidHolder();
      vector.get(0, holder1);
      assertEquals(0, holder1.start);
      assertEquals(uuid1, UuidUtility.uuidFromArrowBuf(holder1.buffer, holder1.start));

      UuidHolder holder2 = new UuidHolder();
      vector.get(1, holder2);
      assertEquals(16, holder2.start); // UUID_BYTE_WIDTH = 16
      assertEquals(uuid2, UuidUtility.uuidFromArrowBuf(holder2.buffer, holder2.start));

      UuidHolder holder3 = new UuidHolder();
      vector.get(2, holder3);
      assertEquals(32, holder3.start); // 2 * UUID_BYTE_WIDTH = 32
      assertEquals(uuid3, UuidUtility.uuidFromArrowBuf(holder3.buffer, holder3.start));

      // Verify all holders share the same buffer
      assertEquals(holder1.buffer, holder2.buffer);
      assertEquals(holder2.buffer, holder3.buffer);
    }
  }

  @Test
  void testNullableHolderStartOffsetWithMultipleValues() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      vector.setSafe(0, uuid1);
      vector.setNull(1);
      vector.setSafe(2, uuid2);
      vector.setValueCount(3);

      // Test NullableUuidHolder with different indices
      NullableUuidHolder holder1 = new NullableUuidHolder();
      vector.get(0, holder1);
      assertEquals(0, holder1.start);
      assertEquals(1, holder1.isSet);
      assertEquals(uuid1, UuidUtility.uuidFromArrowBuf(holder1.buffer, holder1.start));

      NullableUuidHolder holder2 = new NullableUuidHolder();
      vector.get(1, holder2);
      assertEquals(16, holder2.start); // UUID_BYTE_WIDTH = 16
      assertEquals(0, holder2.isSet);

      NullableUuidHolder holder3 = new NullableUuidHolder();
      vector.get(2, holder3);
      assertEquals(32, holder3.start); // 2 * UUID_BYTE_WIDTH = 32
      assertEquals(1, holder3.isSet);
      assertEquals(uuid2, UuidUtility.uuidFromArrowBuf(holder3.buffer, holder3.start));

      // Verify all holders share the same buffer
      assertEquals(holder1.buffer, holder2.buffer);
      assertEquals(holder2.buffer, holder3.buffer);
    }
  }

  @Test
  void testSetFromHolderWithStartOffset() throws Exception {
    try (UuidVector sourceVector = new UuidVector("source", allocator);
        UuidVector targetVector = new UuidVector("target", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      UUID uuid3 = UUID.randomUUID();

      sourceVector.setSafe(0, uuid1);
      sourceVector.setSafe(1, uuid2);
      sourceVector.setSafe(2, uuid3);
      sourceVector.setValueCount(3);

      // Get holder from index 1 (should have start = 16)
      UuidHolder holder = new UuidHolder();
      sourceVector.get(1, holder);
      assertEquals(16, holder.start);

      // Set target vector using holder with non-zero start offset
      targetVector.setSafe(0, holder);
      targetVector.setValueCount(1);

      // Verify the value was copied correctly
      assertEquals(uuid2, targetVector.getObject(0));
    }
  }

  @Test
  void testSetFromNullableHolderWithStartOffset() throws Exception {
    try (UuidVector sourceVector = new UuidVector("source", allocator);
        UuidVector targetVector = new UuidVector("target", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();

      sourceVector.setSafe(0, uuid1);
      sourceVector.setNull(1);
      sourceVector.setSafe(2, uuid2);
      sourceVector.setValueCount(3);

      // Get holder from index 2 (should have start = 32)
      NullableUuidHolder holder = new NullableUuidHolder();
      sourceVector.get(2, holder);
      assertEquals(32, holder.start);
      assertEquals(1, holder.isSet);

      // Set target vector using holder with non-zero start offset
      targetVector.setSafe(0, holder);
      targetVector.setValueCount(1);

      // Verify the value was copied correctly
      assertEquals(uuid2, targetVector.getObject(0));

      // Test with null holder
      NullableUuidHolder nullHolder = new NullableUuidHolder();
      sourceVector.get(1, nullHolder);
      assertEquals(16, nullHolder.start);
      assertEquals(0, nullHolder.isSet);

      targetVector.setSafe(1, nullHolder);
      targetVector.setValueCount(2);
      assertTrue(targetVector.isNull(1));
    }
  }

  @Test
  void testGetStartOffset() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      vector.allocateNew(10);

      // Test getStartOffset for various indices
      assertEquals(0, vector.getStartOffset(0));
      assertEquals(16, vector.getStartOffset(1));
      assertEquals(32, vector.getStartOffset(2));
      assertEquals(48, vector.getStartOffset(3));
      assertEquals(160, vector.getStartOffset(10));
    }
  }

  @Test
  void testReaderWithStartOffsetMultipleReads() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      UUID uuid3 = UUID.randomUUID();

      vector.setSafe(0, uuid1);
      vector.setSafe(1, uuid2);
      vector.setSafe(2, uuid3);
      vector.setValueCount(3);

      UuidReaderImpl reader = (UuidReaderImpl) vector.getReader();
      UuidHolder holder = new UuidHolder();

      // Read from different positions and verify start offset
      reader.read(0, holder);
      assertEquals(0, holder.start);
      assertEquals(uuid1, UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start));

      reader.read(1, holder);
      assertEquals(16, holder.start);
      assertEquals(uuid2, UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start));

      reader.read(2, holder);
      assertEquals(32, holder.start);
      assertEquals(uuid3, UuidUtility.uuidFromArrowBuf(holder.buffer, holder.start));
    }
  }

  @Test
  void testWriterWithExtensionHolder() throws Exception {
    try (UuidVector sourceVector = new UuidVector("source", allocator);
        UuidVector targetVector = new UuidVector("target", allocator)) {
      UUID uuid = UUID.randomUUID();
      sourceVector.setSafe(0, uuid);
      sourceVector.setValueCount(1);

      // Get holder from source
      UuidHolder holder = new UuidHolder();
      sourceVector.get(0, holder);

      // Write using UuidWriterImpl with ExtensionHolder
      UuidWriterImpl writer = new UuidWriterImpl(targetVector);
      writer.setPosition(0);
      writer.writeExtension(holder);

      assertEquals(uuid, targetVector.getObject(0));
    }
  }

  @Test
  void testNullableUuidHolderReaderImpl() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setValueCount(1);

      // Get holder from vector
      NullableUuidHolder sourceHolder = new NullableUuidHolder();
      vector.get(0, sourceHolder);
      assertEquals(1, sourceHolder.isSet);
      assertEquals(0, sourceHolder.start);

      // Create reader from holder
      NullableUuidHolderReaderImpl reader = new NullableUuidHolderReaderImpl(sourceHolder);
      assertTrue(reader.isSet());
      assertEquals(uuid, reader.readObject());

      // Read into another holder
      NullableUuidHolder targetHolder = new NullableUuidHolder();
      reader.read(targetHolder);
      assertEquals(1, targetHolder.isSet);
      assertEquals(0, targetHolder.start);
      assertEquals(uuid, UuidUtility.uuidFromArrowBuf(targetHolder.buffer, targetHolder.start));
    }
  }

  @Test
  void testNullableUuidHolderReaderImplWithNull() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      vector.setNull(0);
      vector.setValueCount(1);

      // Get null holder from vector
      NullableUuidHolder sourceHolder = new NullableUuidHolder();
      vector.get(0, sourceHolder);
      assertEquals(0, sourceHolder.isSet);

      // Create reader from null holder
      NullableUuidHolderReaderImpl reader = new NullableUuidHolderReaderImpl(sourceHolder);
      assertFalse(reader.isSet());
      assertNull(reader.readObject());

      // Read into another holder
      NullableUuidHolder targetHolder = new NullableUuidHolder();
      reader.read(targetHolder);
      assertEquals(0, targetHolder.isSet);
    }
  }

  @Test
  void testNullableUuidHolderReaderImplReadIntoUuidHolder() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid = UUID.randomUUID();
      vector.setSafe(0, uuid);
      vector.setValueCount(1);

      // Get holder from vector
      NullableUuidHolder sourceHolder = new NullableUuidHolder();
      vector.get(0, sourceHolder);

      // Create reader from holder
      NullableUuidHolderReaderImpl reader = new NullableUuidHolderReaderImpl(sourceHolder);

      // Read into UuidHolder (non-nullable)
      UuidHolder targetHolder = new UuidHolder();
      reader.read(targetHolder);
      assertEquals(0, targetHolder.start);
      assertEquals(uuid, UuidUtility.uuidFromArrowBuf(targetHolder.buffer, targetHolder.start));
    }
  }

  @Test
  void testNullableUuidHolderReaderImplWithNonZeroStart() throws Exception {
    try (UuidVector vector = new UuidVector("test", allocator)) {
      UUID uuid1 = UUID.randomUUID();
      UUID uuid2 = UUID.randomUUID();
      vector.setSafe(0, uuid1);
      vector.setSafe(1, uuid2);
      vector.setValueCount(2);

      // Get holder from index 1 (start = 16)
      NullableUuidHolder sourceHolder = new NullableUuidHolder();
      vector.get(1, sourceHolder);
      assertEquals(1, sourceHolder.isSet);
      assertEquals(16, sourceHolder.start);

      // Create reader from holder
      NullableUuidHolderReaderImpl reader = new NullableUuidHolderReaderImpl(sourceHolder);
      assertEquals(uuid2, reader.readObject());

      // Read into another holder and verify start is preserved
      NullableUuidHolder targetHolder = new NullableUuidHolder();
      reader.read(targetHolder);
      assertEquals(16, targetHolder.start);
      assertEquals(uuid2, UuidUtility.uuidFromArrowBuf(targetHolder.buffer, targetHolder.start));
    }
  }
}
