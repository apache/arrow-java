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
package org.apache.arrow.vector.extension;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.UuidReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

public class UuidVector extends ExtensionTypeVector<FixedSizeBinaryVector>
    implements ValueIterableVector<UUID> {
  private final Field field;

  public UuidVector(
      String name, BufferAllocator allocator, FixedSizeBinaryVector underlyingVector) {
    super(name, allocator, underlyingVector);
    this.field = new Field(name, FieldType.nullable(new UuidType()), null);
  }

  public UuidVector(String name, BufferAllocator allocator) {
    super(name, allocator, new FixedSizeBinaryVector(name, allocator, UuidHolder.WIDTH));
    this.field = new Field(name, FieldType.nullable(new UuidType()), null);
  }

  /** Constructor with field and allocator. */
  public UuidVector(Field field, BufferAllocator allocator) {
    super(
        field.getName(),
        allocator,
        new FixedSizeBinaryVector(field.getName(), allocator, UuidHolder.WIDTH));
    this.field = field;
  }

  @Override
  public UUID getObject(int index) {
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

  /**
   * Set the value at the index of the vector to the given value.
   *
   * @param index position in the vector
   * @param uuid given value
   */
  public void set(int index, UUID uuid) {
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    getUnderlyingVector().set(index, bb.array());
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
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((UuidVector) to);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new UuidReaderImpl(this);
  }

  public void setSafe(int index, ArrowBuf value) {
    getUnderlyingVector().setSafe(index, value);
  }

  public void setSafe(int index, byte[] value) {
    getUnderlyingVector().setSafe(index, value);
  }

  /** Set value at index using UUID. */
  public void setSafe(int index, UUID uuid) {
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    getUnderlyingVector().setSafe(index, bb.array());
  }

  /** Set value at index using NullableUuidHolder. */
  public void setSafe(int index, NullableUuidHolder holder) {
    if (holder != null) {
      getUnderlyingVector().setSafe(index, holder.isSet, holder.buffer);
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  /** Set value at index using UuidHolder. */
  public void setSafe(int index, UuidHolder holder) {
    if (holder != null) {
      getUnderlyingVector().setSafe(index, holder.isSet, holder.buffer);
    } else {
      getUnderlyingVector().setNull(index);
    }
  }

  /** Get value at index into UuidHolder. */
  public void get(int index, UuidHolder holder) {
    holder.buffer =
        getUnderlyingVector()
            .getDataBuffer()
            .slice((long) index * UuidHolder.WIDTH, UuidHolder.WIDTH);
    holder.isSet = 1;
  }

  /**
   * Get the slice at the given index, if present, and assign it to the buffer of the provided
   * NullableUuidHolder.
   *
   * @param index position in the vector
   * @param holder the holder to store the value in
   */
  public void get(int index, NullableUuidHolder holder) {
    if (isNull(index)) {
      holder.isSet = 0;
    } else {
      holder.buffer =
          getUnderlyingVector()
              .getDataBuffer()
              .slice((long) index * UuidHolder.WIDTH, UuidHolder.WIDTH);
      holder.isSet = 1;
    }
  }

  public class TransferImpl implements TransferPair {
    UuidVector to;
    ValueVector targetUnderlyingVector;
    TransferPair tp;

    /**
     * Constructor for TransferImpl.
     *
     * @param to the target vector
     */
    public TransferImpl(UuidVector to) {
      this.to = to;
      targetUnderlyingVector = this.to.getUnderlyingVector();
      tp = getUnderlyingVector().makeTransferPair(targetUnderlyingVector);
    }

    public UuidVector getTo() {
      return this.to;
    }

    public void transfer() {
      tp.transfer();
    }

    public void splitAndTransfer(int startIndex, int length) {
      tp.splitAndTransfer(startIndex, length);
    }

    public void copyValueSafe(int fromIndex, int toIndex) {
      tp.copyValueSafe(fromIndex, toIndex);
    }
  }
}
