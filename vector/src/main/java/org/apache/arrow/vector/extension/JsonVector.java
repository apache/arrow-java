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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A JSON extension vector backed by a string vector.
 *
 * <p>Use {@link Field#createVector(BufferAllocator)} with a {@link JsonType} field to create an
 * instance. Write UTF-8 JSON through {@link #getUnderlyingVector()}; values are not parsed or
 * validated.
 */
public class JsonVector extends ExtensionTypeVector<FieldVector>
    implements ValueIterableVector<Text> {
  private final Field field;

  JsonVector(Field field, BufferAllocator allocator, FieldVector underlyingVector) {
    super(field, allocator, underlyingVector);
    this.field = field;
  }

  @Override
  public Field getField() {
    return field;
  }

  @Override
  public Text getObject(int index) {
    return (Text) getUnderlyingVector().getObject(index);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(String name, BufferAllocator allocator) {
    return getTransferPair(new Field(name, field.getFieldType(), field.getChildren()), allocator);
  }

  @Override
  public TransferPair getTransferPair(String name, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(Field targetField, BufferAllocator allocator) {
    return makeTransferPair(targetField.createVector(allocator));
  }

  @Override
  public TransferPair getTransferPair(
      Field targetField, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(targetField, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    JsonVector to = (JsonVector) target;
    TransferPair storagePair = getUnderlyingVector().makeTransferPair(to.getUnderlyingVector());
    return new TransferPair() {
      @Override
      public void transfer() {
        storagePair.transfer();
      }

      @Override
      public void splitAndTransfer(int startIndex, int length) {
        storagePair.splitAndTransfer(startIndex, length);
      }

      @Override
      public JsonVector getTo() {
        return to;
      }

      @Override
      public void copyValueSafe(int fromIndex, int toIndex) {
        storagePair.copyValueSafe(fromIndex, toIndex);
      }
    };
  }

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return getUnderlyingVector().hashCode(index, hasher);
  }
}
