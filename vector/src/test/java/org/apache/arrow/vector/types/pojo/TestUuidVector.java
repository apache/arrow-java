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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.util.TransferPair;

public class TestUuidVector {

  public static class UuidType extends ExtensionType {

    @Override
    public ArrowType storageType() {
      return new ArrowType.FixedSizeBinary(16);
    }

    @Override
    public String extensionName() {
      return "uuid";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
      return other instanceof UuidType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
      if (!storageType.equals(storageType())) {
        throw new UnsupportedOperationException(
            "Cannot construct UuidType from underlying type " + storageType);
      }
      return new UuidType();
    }

    @Override
    public String serialize() {
      return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
      return new UuidVector(name, allocator, new FixedSizeBinaryVector(name, allocator, 16));
    }
  }

  public static class UuidVector extends ExtensionTypeVector<FixedSizeBinaryVector>
      implements ValueIterableVector<UUID> {
    private final Field field;

    public UuidVector(
        String name, BufferAllocator allocator, FixedSizeBinaryVector underlyingVector) {
      super(name, allocator, underlyingVector);
      this.field = new Field(name, FieldType.nullable(new UuidType()), null);
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

    public void setSafe(int index, byte[] value) {
      getUnderlyingVector().setIndexDefined(index);
      getUnderlyingVector().setSafe(index, value);
    }

    public class TransferImpl implements TransferPair {
      UuidVector to;
      ValueVector targetUnderlyingVector;
      TransferPair tp;

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
}
