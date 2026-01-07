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
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.impl.UuidReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.holders.ExtensionHolder;
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
 * @see UuidType
 * @see UuidHolder
 * @see NullableUuidHolder
 */
public class UuidVector extends ExtensionTypeVector<FixedSizeBinaryVector>
    implements ValueIterableVector<UUID>, FixedWidthVector {
  private final Field field;

  /** The fixed byte width of UUID values (16 bytes). */
  public static final int TYPE_WIDTH = UUID_BYTE_WIDTH;

  /**
   * Constructs a UUID vector with the given name, allocator, and underlying vector.
   *
   * @param name the name of the vector
   * @param allocator the buffer allocator
   * @param underlyingVector the underlying FixedSizeBinaryVector for storage
   */
  public UuidVector(
      String name, BufferAllocator allocator, FixedSizeBinaryVector underlyingVector) {
    super(name, allocator, underlyingVector);
    this.field = new Field(name, FieldType.nullable(UuidType.INSTANCE), null);
  }

  /**
   * Constructs a UUID vector with the given name, field type, allocator, and underlying vector.
   *
   * @param name the name of the vector
   * @param fieldType the field type (should contain UuidType)
   * @param allocator the buffer allocator
   * @param underlyingVector the underlying FixedSizeBinaryVector for storage
   */
  public UuidVector(
      String name,
      FieldType fieldType,
      BufferAllocator allocator,
      FixedSizeBinaryVector underlyingVector) {
    super(name, allocator, underlyingVector);
    this.field = new Field(name, fieldType, null);
  }

  /**
   * Constructs a UUID vector with the given name and allocator.
   *
   * <p>Creates a new underlying FixedSizeBinaryVector with 16-byte width.
   *
   * @param name the name of the vector
   * @param allocator the buffer allocator
   */
  public UuidVector(String name, BufferAllocator allocator) {
    super(name, allocator, new FixedSizeBinaryVector(name, allocator, UUID_BYTE_WIDTH));
    this.field = new Field(name, FieldType.nullable(UuidType.INSTANCE), null);
  }

  /**
   * Constructs a UUID vector from a field and allocator.
   *
   * @param field the field definition (should contain UuidType)
   * @param allocator the buffer allocator
   */
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
    int start = this.getStartOffset(index);
    return ByteFunctionHelpers.hash(hasher, this.getDataBuffer(), start, start + UUID_BYTE_WIDTH);
  }

  /**
   * Checks if the value at the given index is set (non-null).
   *
   * @param index the index to check
   * @return 1 if the value is set, 0 if null
   */
  public int isSet(int index) {
    return getUnderlyingVector().isSet(index);
  }

  /**
   * Reads the UUID value at the given index into a UuidHolder.
   *
   * @param index the index to read from
   * @param holder the holder to populate with the UUID data
   */
  public void get(int index, UuidHolder holder) {
    Preconditions.checkArgument(index >= 0, "Cannot get negative index in UUID vector.");
    if (NullCheckingForGet.NULL_CHECKING_ENABLED && this.isSet(index) == 0) {
      holder.isSet = 0;
    } else {
      holder.isSet = 1;
      holder.buffer = getDataBuffer();
      holder.start = getStartOffset(index);
    }
  }

  /**
   * Reads the UUID value at the given index into a NullableUuidHolder.
   *
   * @param index the index to read from
   * @param holder the holder to populate with the UUID data
   */
  public void get(int index, NullableUuidHolder holder) {
    Preconditions.checkArgument(index >= 0, "Cannot get negative index in UUID vector.");
    if (NullCheckingForGet.NULL_CHECKING_ENABLED && this.isSet(index) == 0) {
      holder.isSet = 0;
    } else {
      holder.isSet = 1;
      holder.buffer = getDataBuffer();
      holder.start = getStartOffset(index);
    }
  }

  /**
   * Calculates the byte offset for a given index in the data buffer.
   *
   * @param index the index of the UUID value
   * @return the byte offset in the data buffer
   */
  public final int getStartOffset(int index) {
    return index * UUID_BYTE_WIDTH;
  }

  /**
   * Sets the UUID value at the given index.
   *
   * @param index the index to set
   * @param value the UUID value to set, or null to set a null value
   */
  public void set(int index, UUID value) {
    if (value != null) {
      set(index, UuidUtility.getBytesFromUUID(value));
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  /**
   * Sets the UUID value at the given index from a UuidHolder.
   *
   * @param index the index to set
   * @param holder the holder containing the UUID data
   */
  public void set(int index, UuidHolder holder) {
    this.set(index, holder.buffer, holder.start);
  }

  /**
   * Sets the UUID value at the given index from a NullableUuidHolder.
   *
   * @param index the index to set
   * @param holder the holder containing the UUID data
   */
  public void set(int index, NullableUuidHolder holder) {
    if (holder.isSet == 0) {
      getUnderlyingVector().setNull(index);
    } else {
      this.set(index, holder.buffer, holder.start);
    }
  }

  /**
   * Sets the UUID value at the given index by copying from a source buffer.
   *
   * @param index the index to set
   * @param source the source buffer to copy from
   * @param sourceOffset the offset in the source buffer where the UUID data starts
   */
  public void set(int index, ArrowBuf source, int sourceOffset) {
    Preconditions.checkNotNull(source, "Cannot set UUID vector, the source buffer is null.");

    BitVectorHelper.setBit(getUnderlyingVector().getValidityBuffer(), index);
    getUnderlyingVector()
            .getDataBuffer()
            .setBytes((long) index * UUID_BYTE_WIDTH, source, sourceOffset, UUID_BYTE_WIDTH);
  }

  /**
   * Sets the UUID value at the given index from a byte array.
   *
   * @param index the index to set
   * @param value the 16-byte array containing the UUID data
   */
  public void set(int index, byte[] value) {
    getUnderlyingVector().set(index, value);
  }

  /**
   * Sets the UUID value at the given index, expanding capacity if needed.
   *
   * @param index the index to set
   * @param value the UUID value to set, or null to set a null value
   */
  public void setSafe(int index, UUID value) {
    if (value != null) {
      setSafe(index, UuidUtility.getBytesFromUUID(value));
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  /**
   * Sets the UUID value at the given index from a NullableUuidHolder, expanding capacity if needed.
   *
   * @param index the index to set
   * @param holder the holder containing the UUID data, or null to set a null value
   */
  public void setSafe(int index, NullableUuidHolder holder) {
    if (holder == null || holder.isSet == 0) {
      getUnderlyingVector().setNull(index);
    } else {
      this.setSafe(index, holder.buffer, holder.start);
    }
  }

  /**
   * Sets the UUID value at the given index from a UuidHolder, expanding capacity if needed.
   *
   * @param index the index to set
   * @param holder the holder containing the UUID data
   */
  public void setSafe(int index, UuidHolder holder) {
    this.setSafe(index, holder.buffer, holder.start);
  }

  /**
   * Sets the UUID value at the given index by copying from a source buffer, expanding capacity if
   * needed.
   *
   * @param index the index to set
   * @param buffer the source buffer to copy from
   * @param start the offset in the source buffer where the UUID data starts
   */
  public void setSafe(int index, ArrowBuf buffer, int start) {
    getUnderlyingVector().handleSafe(index);
    this.set(index, buffer, start);
  }

  /**
   * Sets the UUID value at the given index from a byte array, expanding capacity if needed.
   *
   * @param index the index to set
   * @param value the 16-byte array containing the UUID data
   */
  public void setSafe(int index, byte[] value) {
    getUnderlyingVector().setIndexDefined(index);
    getUnderlyingVector().setSafe(index, value);
  }

  /**
   * Sets the UUID value at the given index from an ArrowBuf, expanding capacity if needed.
   *
   * @param index the index to set
   * @param value the buffer containing the 16-byte UUID data
   */
  public void setSafe(int index, ArrowBuf value) {
    getUnderlyingVector().setSafe(index, value);
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

  @Override
  public int getTypeWidth() {
    return UUID_BYTE_WIDTH;
  }

  /** {@link TransferPair} for {@link UuidVector}. */
  public class TransferImpl implements TransferPair {
    UuidVector to;

    /**
     * Constructs a transfer pair with the given target vector.
     *
     * @param to the target UUID vector
     */
    public TransferImpl(UuidVector to) {
      this.to = to;
    }

    /**
     * Constructs a transfer pair, creating a new target vector from the field and allocator.
     *
     * @param field the field definition for the target vector
     * @param allocator the buffer allocator for the target vector
     */
    public TransferImpl(Field field, BufferAllocator allocator) {
      this.to = new UuidVector(field, allocator);
    }

    /**
     * Constructs a transfer pair, creating a new target vector with the given name and allocator.
     *
     * @param ref the name for the target vector
     * @param allocator the buffer allocator for the target vector
     */
    public TransferImpl(String ref, BufferAllocator allocator) {
      this.to = new UuidVector(ref, allocator);
    }

    /**
     * Gets the target vector of this transfer pair.
     *
     * @return the target UUID vector
     */
    public UuidVector getTo() {
      return this.to;
    }

    /** Transfers ownership of data from the source vector to the target vector. */
    public void transfer() {
      getUnderlyingVector().transferTo(to.getUnderlyingVector());
    }

    /**
     * Splits and transfers a range of values from the source vector to the target vector.
     *
     * @param startIndex the starting index in the source vector
     * @param length the number of values to transfer
     */
    public void splitAndTransfer(int startIndex, int length) {
      getUnderlyingVector().splitAndTransferTo(startIndex, length, to.getUnderlyingVector());
    }

    /**
     * Copies a value from the source vector to the target vector, expanding capacity if needed.
     *
     * @param fromIndex the index in the source vector
     * @param toIndex the index in the target vector
     */
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, (ValueVector) UuidVector.this);
    }
  }
}
