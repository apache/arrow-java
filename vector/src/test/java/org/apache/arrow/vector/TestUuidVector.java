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

      assertEquals(
          "Unsupported value type for UUID: class java.lang.String", exception.getMessage());
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
      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, 0);
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

      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, 0);
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

      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, 0);
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

      UUID actualUuid = UuidUtility.uuidFromArrowBuf(holder.buffer, 0);
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
      assertEquals(uuid1, UuidUtility.uuidFromArrowBuf(holder1.buffer, 0));
      assertEquals(1, holder1.isSet);

      NullableUuidHolder holder2 = new NullableUuidHolder();
      reader.read(1, holder2);
      assertEquals(0, holder2.isSet);

      NullableUuidHolder holder3 = new NullableUuidHolder();
      reader.read(2, holder3);
      assertEquals(uuid2, UuidUtility.uuidFromArrowBuf(holder3.buffer, 0));
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
}
