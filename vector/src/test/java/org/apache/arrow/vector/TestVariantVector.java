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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.impl.VariantReaderImpl;
import org.apache.arrow.vector.complex.impl.VariantWriterImpl;
import org.apache.arrow.vector.extension.VariantType;
import org.apache.arrow.vector.extension.VariantVector;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.holders.NullableVariantHolder;
import org.apache.arrow.vector.holders.VariantHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for VariantVector, VariantWriterImpl, and VariantReaderImpl. */
class TestVariantVector {

  private BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  private VariantHolder createHolder(
      ArrowBuf metadataBuf, byte[] metadata, ArrowBuf valueBuf, byte[] value) {
    VariantHolder holder = new VariantHolder();
    holder.metadataStart = 0;
    holder.metadataEnd = metadata.length;
    holder.metadataBuffer = metadataBuf;
    holder.valueStart = 0;
    holder.valueEnd = value.length;
    holder.valueBuffer = valueBuf;
    return holder;
  }

  private NullableVariantHolder createNullableHolder(
      ArrowBuf metadataBuf, byte[] metadata, ArrowBuf valueBuf, byte[] value) {
    NullableVariantHolder holder = new NullableVariantHolder();
    holder.isSet = 1;
    holder.metadataStart = 0;
    holder.metadataEnd = metadata.length;
    holder.metadataBuffer = metadataBuf;
    holder.valueStart = 0;
    holder.valueEnd = value.length;
    holder.valueBuffer = valueBuf;
    return holder;
  }

  private NullableVariantHolder createNullHolder() {
    NullableVariantHolder holder = new NullableVariantHolder();
    holder.isSet = 0;
    return holder;
  }

  // ========== Basic Vector Tests ==========

  @Test
  void testVectorCreation() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      assertNotNull(vector);
      assertEquals("test", vector.getField().getName());
      assertNotNull(vector.getMetadataVector());
      assertNotNull(vector.getValueVector());
    }
  }

  @Test
  void testSetAndGet() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5, 6, 7};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      // Retrieve and verify
      NullableVariantHolder result = new NullableVariantHolder();
      vector.get(0, result);

      assertEquals(1, result.isSet);
      assertEquals(metadata.length, result.metadataEnd - result.metadataStart);
      assertEquals(value.length, result.valueEnd - result.valueStart);

      byte[] actualMetadata = new byte[metadata.length];
      byte[] actualValue = new byte[value.length];
      result.metadataBuffer.getBytes(result.metadataStart, actualMetadata);
      result.valueBuffer.getBytes(result.valueStart, actualValue);

      assertArrayEquals(metadata, actualMetadata);
      assertArrayEquals(value, actualValue);
    }
  }

  @Test
  void testSetNull() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      NullableVariantHolder holder = createNullHolder();

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      assertTrue(vector.isNull(0));

      NullableVariantHolder result = new NullableVariantHolder();
      vector.get(0, result);
      assertEquals(0, result.isSet);
    }
  }

  @Test
  void testMultipleValues() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf1 = allocator.buffer(10);
        ArrowBuf valueBuf1 = allocator.buffer(10);
        ArrowBuf metadataBuf2 = allocator.buffer(10);
        ArrowBuf valueBuf2 = allocator.buffer(10)) {

      byte[] metadata1 = new byte[] {1, 2};
      byte[] value1 = new byte[] {3, 4, 5};
      metadataBuf1.setBytes(0, metadata1);
      valueBuf1.setBytes(0, value1);

      NullableVariantHolder holder1 =
          createNullableHolder(metadataBuf1, metadata1, valueBuf1, value1);

      byte[] metadata2 = new byte[] {6, 7, 8};
      byte[] value2 = new byte[] {9, 10};
      metadataBuf2.setBytes(0, metadata2);
      valueBuf2.setBytes(0, value2);

      NullableVariantHolder holder2 =
          createNullableHolder(metadataBuf2, metadata2, valueBuf2, value2);

      vector.setSafe(0, holder1);
      vector.setSafe(1, holder2);
      vector.setValueCount(2);

      // Verify first value
      NullableVariantHolder result1 = new NullableVariantHolder();
      vector.get(0, result1);
      assertEquals(1, result1.isSet);

      byte[] actualMetadata1 = new byte[metadata1.length];
      byte[] actualValue1 = new byte[value1.length];
      result1.metadataBuffer.getBytes(result1.metadataStart, actualMetadata1);
      result1.valueBuffer.getBytes(result1.valueStart, actualValue1);
      assertArrayEquals(metadata1, actualMetadata1);
      assertArrayEquals(value1, actualValue1);

      // Verify second value
      NullableVariantHolder result2 = new NullableVariantHolder();
      vector.get(1, result2);
      assertEquals(1, result2.isSet);

      byte[] actualMetadata2 = new byte[metadata2.length];
      byte[] actualValue2 = new byte[value2.length];
      result2.metadataBuffer.getBytes(result2.metadataStart, actualMetadata2);
      result2.valueBuffer.getBytes(result2.valueStart, actualValue2);
      assertArrayEquals(metadata2, actualMetadata2);
      assertArrayEquals(value2, actualValue2);
    }
  }

  @Test
  void testNonNullableHolder() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5, 6};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      VariantHolder holder = createHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      assertFalse(vector.isNull(0));

      NullableVariantHolder result = new NullableVariantHolder();
      vector.get(0, result);
      assertEquals(1, result.isSet);
    }
  }

  // ========== Writer Tests ==========

  @Test
  void testWriteWithVariantHolder() {
    try (VariantVector vector = new VariantVector("test", allocator);
        VariantWriterImpl writer = new VariantWriterImpl(vector);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2};
      byte[] value = new byte[] {3, 4, 5};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      VariantHolder holder = createHolder(metadataBuf, metadata, valueBuf, value);

      writer.setPosition(0);
      writer.write(holder);

      assertEquals(1, vector.getValueCount());
      assertFalse(vector.isNull(0));
    }
  }

  @Test
  void testWriteWithNullableVariantHolder() {
    try (VariantVector vector = new VariantVector("test", allocator);
        VariantWriterImpl writer = new VariantWriterImpl(vector);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2};
      byte[] value = new byte[] {3, 4, 5};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      writer.setPosition(0);
      writer.write(holder);

      assertEquals(1, vector.getValueCount());
      assertFalse(vector.isNull(0));
    }
  }

  @Test
  void testWriteWithNullableVariantHolderNull() {
    try (VariantVector vector = new VariantVector("test", allocator);
        VariantWriterImpl writer = new VariantWriterImpl(vector)) {

      NullableVariantHolder holder = createNullHolder();

      writer.setPosition(0);
      writer.write(holder);

      assertEquals(1, vector.getValueCount());
      assertTrue(vector.isNull(0));
    }
  }

  @Test
  void testWriteExtensionWithUnsupportedType() {
    try (VariantVector vector = new VariantVector("test", allocator);
        VariantWriterImpl writer = new VariantWriterImpl(vector)) {

      writer.setPosition(0);

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> writer.writeExtension("invalid-type"));

      assertTrue(exception.getMessage().contains("Unsupported type for Variant"));
    }
  }

  @Test
  void testWriteWithUnsupportedHolder() {
    try (VariantVector vector = new VariantVector("test", allocator);
        VariantWriterImpl writer = new VariantWriterImpl(vector)) {

      ExtensionHolder unsupportedHolder =
          new ExtensionHolder() {
            @Override
            public ArrowType type() {
              return VariantType.INSTANCE;
            }
          };

      writer.setPosition(0);

      IllegalArgumentException exception =
          assertThrows(IllegalArgumentException.class, () -> writer.write(unsupportedHolder));

      assertTrue(exception.getMessage().contains("Unsupported type for Variant"));
    }
  }

  // ========== Reader Tests ==========

  @Test
  void testReaderReadWithNullableVariantHolder() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5, 6};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      VariantReaderImpl reader = (VariantReaderImpl) vector.getReader();
      reader.setPosition(0);

      NullableVariantHolder result = new NullableVariantHolder();
      reader.read(result);

      assertEquals(1, result.isSet);
      assertEquals(metadata.length, result.metadataEnd - result.metadataStart);
      assertEquals(value.length, result.valueEnd - result.valueStart);
    }
  }

  @Test
  void testReaderReadWithNullableVariantHolderNull() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      vector.setNull(0);
      vector.setValueCount(1);

      VariantReaderImpl reader = (VariantReaderImpl) vector.getReader();
      reader.setPosition(0);

      NullableVariantHolder holder = new NullableVariantHolder();
      reader.read(holder);

      assertEquals(0, holder.isSet);
    }
  }

  @Test
  void testReaderIsSet() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1};
      byte[] value = new byte[] {2};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setNull(1);
      vector.setValueCount(2);

      VariantReaderImpl reader = (VariantReaderImpl) vector.getReader();

      reader.setPosition(0);
      assertTrue(reader.isSet());

      reader.setPosition(1);
      assertFalse(reader.isSet());
    }
  }

  @Test
  void testReaderGetMinorType() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      VariantReaderImpl reader = (VariantReaderImpl) vector.getReader();
      assertEquals(vector.getMinorType(), reader.getMinorType());
    }
  }

  @Test
  void testReaderGetField() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      VariantReaderImpl reader = (VariantReaderImpl) vector.getReader();
      assertEquals(vector.getField(), reader.getField());
      assertEquals("test", reader.getField().getName());
    }
  }

  @Test
  void testReaderReadWithNonNullableVariantHolder() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5, 6};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      VariantReaderImpl reader = (VariantReaderImpl) vector.getReader();
      reader.setPosition(0);

      VariantHolder result = new VariantHolder();
      reader.read(result);

      // Verify the data was read correctly
      byte[] actualMetadata = new byte[metadata.length];
      byte[] actualValue = new byte[value.length];
      result.metadataBuffer.getBytes(result.metadataStart, actualMetadata);
      result.valueBuffer.getBytes(result.valueStart, actualValue);

      assertArrayEquals(metadata, actualMetadata);
      assertArrayEquals(value, actualValue);
      assertEquals(1, result.isSet);
    }
  }

  // ========== Transfer Pair Tests ==========

  @Test
  void testTransferPair() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5, 6, 7};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      fromVector.setSafe(0, holder);
      fromVector.setValueCount(1);

      org.apache.arrow.vector.util.TransferPair transferPair =
          fromVector.getTransferPair(allocator);
      VariantVector toVector = (VariantVector) transferPair.getTo();

      transferPair.transfer();

      assertEquals(0, fromVector.getValueCount());
      assertEquals(1, toVector.getValueCount());

      NullableVariantHolder result = new NullableVariantHolder();
      toVector.get(0, result);
      assertEquals(1, result.isSet);

      byte[] actualMetadata = new byte[metadata.length];
      byte[] actualValue = new byte[value.length];
      result.metadataBuffer.getBytes(result.metadataStart, actualMetadata);
      result.valueBuffer.getBytes(result.valueStart, actualValue);

      assertArrayEquals(metadata, actualMetadata);
      assertArrayEquals(value, actualValue);

      toVector.close();
    }
  }

  @Test
  void testSplitAndTransfer() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        ArrowBuf metadataBuf1 = allocator.buffer(10);
        ArrowBuf valueBuf1 = allocator.buffer(10);
        ArrowBuf metadataBuf2 = allocator.buffer(10);
        ArrowBuf valueBuf2 = allocator.buffer(10);
        ArrowBuf metadataBuf3 = allocator.buffer(10);
        ArrowBuf valueBuf3 = allocator.buffer(10)) {

      byte[] metadata1 = new byte[] {1};
      byte[] value1 = new byte[] {2, 3};
      metadataBuf1.setBytes(0, metadata1);
      valueBuf1.setBytes(0, value1);

      byte[] metadata2 = new byte[] {4, 5};
      byte[] value2 = new byte[] {6};
      metadataBuf2.setBytes(0, metadata2);
      valueBuf2.setBytes(0, value2);

      byte[] metadata3 = new byte[] {7, 8, 9};
      byte[] value3 = new byte[] {10, 11, 12};
      metadataBuf3.setBytes(0, metadata3);
      valueBuf3.setBytes(0, value3);

      NullableVariantHolder holder1 =
          createNullableHolder(metadataBuf1, metadata1, valueBuf1, value1);
      NullableVariantHolder holder2 =
          createNullableHolder(metadataBuf2, metadata2, valueBuf2, value2);
      NullableVariantHolder holder3 =
          createNullableHolder(metadataBuf3, metadata3, valueBuf3, value3);

      fromVector.setSafe(0, holder1);
      fromVector.setSafe(1, holder2);
      fromVector.setSafe(2, holder3);
      fromVector.setValueCount(3);

      org.apache.arrow.vector.util.TransferPair transferPair =
          fromVector.getTransferPair(allocator);
      VariantVector toVector = (VariantVector) transferPair.getTo();

      // Split and transfer indices 1-2 (middle and last)
      transferPair.splitAndTransfer(1, 2);

      assertEquals(2, toVector.getValueCount());

      // Verify transferred values
      NullableVariantHolder result1 = new NullableVariantHolder();
      toVector.get(0, result1);
      assertEquals(1, result1.isSet);

      byte[] actualMetadata1 = new byte[metadata2.length];
      byte[] actualValue1 = new byte[value2.length];
      result1.metadataBuffer.getBytes(result1.metadataStart, actualMetadata1);
      result1.valueBuffer.getBytes(result1.valueStart, actualValue1);
      assertArrayEquals(metadata2, actualMetadata1);
      assertArrayEquals(value2, actualValue1);

      NullableVariantHolder result2 = new NullableVariantHolder();
      toVector.get(1, result2);
      assertEquals(1, result2.isSet);

      byte[] actualMetadata2 = new byte[metadata3.length];
      byte[] actualValue2 = new byte[value3.length];
      result2.metadataBuffer.getBytes(result2.metadataStart, actualMetadata2);
      result2.valueBuffer.getBytes(result2.valueStart, actualValue2);
      assertArrayEquals(metadata3, actualMetadata2);
      assertArrayEquals(value3, actualValue2);

      toVector.close();
    }
  }

  @Test
  void testCopyValueSafe() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        VariantVector toVector = new VariantVector("to", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2};
      byte[] value = new byte[] {3, 4, 5};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      fromVector.setSafe(0, holder);
      fromVector.setValueCount(1);

      org.apache.arrow.vector.util.TransferPair transferPair =
          fromVector.makeTransferPair(toVector);

      transferPair.copyValueSafe(0, 0);
      toVector.setValueCount(1);

      // Verify the value was copied
      NullableVariantHolder result = new NullableVariantHolder();
      toVector.get(0, result);
      assertEquals(1, result.isSet);

      byte[] actualMetadata = new byte[metadata.length];
      byte[] actualValue = new byte[value.length];
      result.metadataBuffer.getBytes(result.metadataStart, actualMetadata);
      result.valueBuffer.getBytes(result.valueStart, actualValue);

      assertArrayEquals(metadata, actualMetadata);
      assertArrayEquals(value, actualValue);

      // Original vector should still have the value
      NullableVariantHolder originalResult = new NullableVariantHolder();
      fromVector.get(0, originalResult);
      assertEquals(1, originalResult.isSet);
    }
  }

  @Test
  void testGetTransferPairWithField() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1};
      byte[] value = new byte[] {2};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      fromVector.setSafe(0, holder);
      fromVector.setValueCount(1);

      org.apache.arrow.vector.util.TransferPair transferPair =
          fromVector.getTransferPair(fromVector.getField(), allocator);
      VariantVector toVector = (VariantVector) transferPair.getTo();

      transferPair.transfer();

      assertEquals(1, toVector.getValueCount());
      assertEquals(fromVector.getField().getName(), toVector.getField().getName());

      toVector.close();
    }
  }

  // ========== Copy Operations Tests ==========

  @Test
  void testCopyFrom() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        VariantVector toVector = new VariantVector("to", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2, 3};
      byte[] value = new byte[] {4, 5};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      fromVector.setSafe(0, holder);
      fromVector.setValueCount(1);

      toVector.allocateNew();
      toVector.copyFrom(0, 0, fromVector);
      toVector.setValueCount(1);

      NullableVariantHolder result = new NullableVariantHolder();
      toVector.get(0, result);
      assertEquals(1, result.isSet);

      byte[] actualMetadata = new byte[metadata.length];
      byte[] actualValue = new byte[value.length];
      result.metadataBuffer.getBytes(result.metadataStart, actualMetadata);
      result.valueBuffer.getBytes(result.valueStart, actualValue);

      assertArrayEquals(metadata, actualMetadata);
      assertArrayEquals(value, actualValue);
    }
  }

  @Test
  void testCopyFromSafe() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        VariantVector toVector = new VariantVector("to", allocator);
        ArrowBuf metadataBuf1 = allocator.buffer(10);
        ArrowBuf valueBuf1 = allocator.buffer(10);
        ArrowBuf metadataBuf2 = allocator.buffer(10);
        ArrowBuf valueBuf2 = allocator.buffer(10)) {

      byte[] metadata1 = new byte[] {1};
      byte[] value1 = new byte[] {2, 3};
      metadataBuf1.setBytes(0, metadata1);
      valueBuf1.setBytes(0, value1);

      NullableVariantHolder holder1 =
          createNullableHolder(metadataBuf1, metadata1, valueBuf1, value1);

      byte[] metadata2 = new byte[] {4, 5};
      byte[] value2 = new byte[] {6};
      metadataBuf2.setBytes(0, metadata2);
      valueBuf2.setBytes(0, value2);

      NullableVariantHolder holder2 =
          createNullableHolder(metadataBuf2, metadata2, valueBuf2, value2);

      fromVector.setSafe(0, holder1);
      fromVector.setSafe(1, holder2);
      fromVector.setValueCount(2);

      // Copy without pre-allocating toVector
      for (int i = 0; i < 2; i++) {
        toVector.copyFromSafe(i, i, fromVector);
      }
      toVector.setValueCount(2);

      // Verify both values
      NullableVariantHolder result1 = new NullableVariantHolder();
      toVector.get(0, result1);
      assertEquals(1, result1.isSet);

      byte[] actualMetadata1 = new byte[metadata1.length];
      byte[] actualValue1 = new byte[value1.length];
      result1.metadataBuffer.getBytes(result1.metadataStart, actualMetadata1);
      result1.valueBuffer.getBytes(result1.valueStart, actualValue1);
      assertArrayEquals(metadata1, actualMetadata1);
      assertArrayEquals(value1, actualValue1);

      NullableVariantHolder result2 = new NullableVariantHolder();
      toVector.get(1, result2);
      assertEquals(1, result2.isSet);

      byte[] actualMetadata2 = new byte[metadata2.length];
      byte[] actualValue2 = new byte[value2.length];
      result2.metadataBuffer.getBytes(result2.metadataStart, actualMetadata2);
      result2.valueBuffer.getBytes(result2.valueStart, actualValue2);
      assertArrayEquals(metadata2, actualMetadata2);
      assertArrayEquals(value2, actualValue2);
    }
  }

  @Test
  void testCopyFromWithNulls() {
    try (VariantVector fromVector = new VariantVector("from", allocator);
        VariantVector toVector = new VariantVector("to", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1};
      byte[] value = new byte[] {2};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      fromVector.setSafe(0, holder);
      fromVector.setNull(1);
      fromVector.setSafe(2, holder);
      fromVector.setValueCount(3);

      toVector.allocateNew();
      for (int i = 0; i < 3; i++) {
        toVector.copyFromSafe(i, i, fromVector);
      }
      toVector.setValueCount(3);

      assertFalse(toVector.isNull(0));
      assertTrue(toVector.isNull(1));
      assertFalse(toVector.isNull(2));
    }
  }

  // ========== GetObject Tests ==========

  @Test
  void testGetObject() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1, 2};
      byte[] value = new byte[] {3, 4, 5};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      Object obj = vector.getObject(0);
      assertNotNull(obj);
    }
  }

  @Test
  void testGetObjectNull() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      vector.setNull(0);
      vector.setValueCount(1);

      Object obj = vector.getObject(0);
      assertNull(obj);
    }
  }

  // ========== Allocate and Capacity Tests ==========

  @Test
  void testAllocateNew() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      vector.allocateNew();
      assertTrue(vector.getValueCapacity() > 0);
    }
  }

  @Test
  void testSetInitialCapacity() {
    try (VariantVector vector = new VariantVector("test", allocator)) {
      vector.setInitialCapacity(100);
      vector.allocateNew();
      assertTrue(vector.getValueCapacity() >= 100);
    }
  }

  @Test
  void testClearAndReuse() {
    try (VariantVector vector = new VariantVector("test", allocator);
        ArrowBuf metadataBuf = allocator.buffer(10);
        ArrowBuf valueBuf = allocator.buffer(10)) {

      byte[] metadata = new byte[] {1};
      byte[] value = new byte[] {2};
      metadataBuf.setBytes(0, metadata);
      valueBuf.setBytes(0, value);

      NullableVariantHolder holder = createNullableHolder(metadataBuf, metadata, valueBuf, value);

      vector.setSafe(0, holder);
      vector.setValueCount(1);

      assertFalse(vector.isNull(0));

      vector.clear();
      vector.allocateNew();

      // After clear, vector should be empty
      assertEquals(0, vector.getValueCount());
    }
  }
}
