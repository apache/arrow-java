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

import static org.apache.arrow.vector.extension.UuidType.UUID_BYTE_WIDTH;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.complex.impl.UuidReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.holders.NullableUuidHolder;
import org.apache.arrow.vector.holders.UuidHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.UuidUtility;

/**
 * Vector implementation for UUID values using {@link UuidType}.
 *
 * <p>Supports setting and retrieving UUIDs with efficient storage and nullable value handling.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * UuidVector vector = new UuidVector("uuid_col", allocator);
 * vector.set(0, UUID.randomUUID());
 * UUID value = vector.getObject(0);
 * }</pre>
 *
 * @see {@link UuidType}
 * @see {@link UuidHolder}
 * @see {@link NullableUuidHolder}
 */
public class UuidVector extends ExtensionTypeVector<FixedSizeBinaryVector>
    implements ValueIterableVector<UUID>, FixedWidthVector, FixedSizeExtensionType {
  private final Field field;
  public static final int TYPE_WIDTH = UUID_BYTE_WIDTH;

  public UuidVector(
      String name, BufferAllocator allocator, FixedSizeBinaryVector underlyingVector) {
    super(name, allocator, underlyingVector);
    this.field = new Field(name, FieldType.nullable(new UuidType()), null);
  }

  public UuidVector(
      String name,
      FieldType fieldType,
      BufferAllocator allocator,
      FixedSizeBinaryVector underlyingVector) {
    super(name, allocator, underlyingVector);
    this.field = new Field(name, fieldType, null);
  }

  public UuidVector(String name, BufferAllocator allocator) {
    super(name, allocator, new FixedSizeBinaryVector(name, allocator, UUID_BYTE_WIDTH));
    this.field = new Field(name, FieldType.nullable(new UuidType()), null);
  }

  public UuidVector(Field field, BufferAllocator allocator) {
    super(
        field.getName(),
        allocator,
        new FixedSizeBinaryVector(field.getName(), allocator, UUID_BYTE_WIDTH));
    this.field = field;
  }

  @Override
  public UUID getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    }
    final ByteBuffer bb = ByteBuffer.wrap(getUnderlyingVector().getObject(index));
    return new UUID(bb.getLong(), bb.getLong());
  }

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return getUnderlyingVector().hashCode(index, hasher);
  }

  public int isSet(int index) {
    return getUnderlyingVector().isSet(index);
  }

  public ArrowBuf get(int index) throws IllegalStateException {
    if (NullCheckingForGet.NULL_CHECKING_ENABLED && this.isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    } else {
      return getBufferSlicePostNullCheck(index);
    }
  }

  public void get(int index, NullableUuidHolder holder) {
    if (NullCheckingForGet.NULL_CHECKING_ENABLED && this.isSet(index) == 0) {
      holder.isSet = 0;
    } else {
      holder.isSet = 1;
      holder.buffer = getBufferSlicePostNullCheck(index);
    }
  }

  public void get(int index, UuidHolder holder) {
    if (NullCheckingForGet.NULL_CHECKING_ENABLED && this.isSet(index) == 0) {
      holder.isSet = 0;
    } else {
      holder.isSet = 1;
      holder.buffer = getBufferSlicePostNullCheck(index);
    }
  }

  public void set(int index, UUID value) {
    if (value != null) {
      set(index, UuidUtility.getBytesFromUUID(value));
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  public void set(int index, UuidHolder holder) {
    this.set(index, holder.isSet, holder.buffer);
  }

  public void set(int index, NullableUuidHolder holder) {
    this.set(index, holder.isSet, holder.buffer);
  }

  public void set(int index, int isSet, ArrowBuf buffer) {
    getUnderlyingVector().set(index, isSet, buffer);
  }

  public void set(int index, ArrowBuf value) {
    getUnderlyingVector().set(index, value);
  }

  public void set(int index, ArrowBuf source, int sourceOffset) {
    // Copy bytes from source buffer to target vector data buffer
    ArrowBuf dataBuffer = getUnderlyingVector().getDataBuffer();
    dataBuffer.setBytes((long) index * UUID_BYTE_WIDTH, source, sourceOffset, UUID_BYTE_WIDTH);
    getUnderlyingVector().setIndexDefined(index);
  }

  public void set(int index, byte[] value) {
    getUnderlyingVector().set(index, value);
  }

  public void setSafe(int index, UUID value) {
    if (value != null) {
      setSafe(index, UuidUtility.getBytesFromUUID(value));
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  public void setSafe(int index, NullableUuidHolder holder) {
    if (holder != null) {
      getUnderlyingVector().setSafe(index, holder.isSet, holder.buffer);
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  public void setSafe(int index, UuidHolder holder) {
    if (holder != null) {
      getUnderlyingVector().setSafe(index, holder.isSet, holder.buffer);
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  public void setSafe(int index, byte[] value) {
    getUnderlyingVector().setIndexDefined(index);
    getUnderlyingVector().setSafe(index, value);
  }

  public void setSafe(int index, ArrowBuf value) {
    getUnderlyingVector().setSafe(index, value);
  }

  @Override
  public int getTypeWidth() {
    return UUID_BYTE_WIDTH;
  }

  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    getUnderlyingVector()
        .copyFromSafe(fromIndex, thisIndex, ((UuidVector) from).getUnderlyingVector());
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    getUnderlyingVector()
        .copyFromSafe(fromIndex, thisIndex, ((UuidVector) from).getUnderlyingVector());
  }

  @Override
  public Field getField() {
    return field;
  }

  @Override
  public ArrowBufPointer getDataPointer(int i) {
    return getUnderlyingVector().getDataPointer(i);
  }

  @Override
  public ArrowBufPointer getDataPointer(int i, ArrowBufPointer arrowBufPointer) {
    return getUnderlyingVector().getDataPointer(i, arrowBufPointer);
  }

  @Override
  public void allocateNew(int valueCount) {
    getUnderlyingVector().allocateNew(valueCount);
  }

  @Override
  public void zeroVector() {
    getUnderlyingVector().zeroVector();
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((UuidVector) to);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new UuidReaderImpl(this);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(ref, allocator);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(this.getField().getName(), allocator);
  }

  private ArrowBuf getBufferSlicePostNullCheck(int index) {
    return getUnderlyingVector()
        .getDataBuffer()
        .slice((long) index * UUID_BYTE_WIDTH, UUID_BYTE_WIDTH);
  }

  /** {@link TransferPair} for {@link UuidVector}. */
  public class TransferImpl implements TransferPair {
    UuidVector to;

    public TransferImpl(UuidVector to) {
      this.to = to;
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      this.to = new UuidVector(field, allocator);
    }

    public TransferImpl(String ref, BufferAllocator allocator) {
      this.to = new UuidVector(ref, allocator);
    }

    public UuidVector getTo() {
      return this.to;
    }

    public void transfer() {
      getUnderlyingVector().transferTo(to.getUnderlyingVector());
    }

    public void splitAndTransfer(int startIndex, int length) {
      getUnderlyingVector().splitAndTransferTo(startIndex, length, to.getUnderlyingVector());
    }

    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, (ValueVector) UuidVector.this);
    }
  }
}
