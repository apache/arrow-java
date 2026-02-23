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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.variant.Variant;
import org.apache.arrow.variant.holders.NullableVariantHolder;
import org.apache.arrow.variant.holders.VariantHolder;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.AbstractStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Arrow vector for storing {@link VariantType} values.
 *
 * <p>Stores semi-structured data (like JSON) as metadata + value binary pairs, allowing
 * type-flexible columnar storage within Arrow's type system.
 */
public class VariantVector extends ExtensionTypeVector<StructVector> {

  public static final String METADATA_VECTOR_NAME = "metadata";
  public static final String VALUE_VECTOR_NAME = "value";

  private final Field rootField;

  /**
   * Constructs a new VariantVector with the given name and allocator.
   *
   * @param name the name of the vector
   * @param allocator the buffer allocator for memory management
   */
  public VariantVector(String name, BufferAllocator allocator) {
    super(
        name,
        allocator,
        new StructVector(
            name,
            allocator,
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            null,
            AbstractStructVector.ConflictPolicy.CONFLICT_ERROR,
            false));
    rootField = createVariantField(name);
    ((FieldVector) this.getUnderlyingVector())
        .initializeChildrenFromFields(rootField.getChildren());
  }

  /**
   * Creates a new VariantVector with the given name. The Variant Field schema has to be the same
   * everywhere, otherwise ArrowBuffer loading might fail during serialization/deserialization and
   * schema mismatches can occur. This includes CompleteType's VARIANT and VARIANT_REQUIRED types.
   */
  public static Field createVariantField(String name) {
    return new Field(
        name, new FieldType(true, VariantType.INSTANCE, null), createVariantChildFields());
  }

  /**
   * Creates the child fields for the VariantVector. Metadata vector will be index 0 and value
   * vector will be index 1.
   */
  public static List<Field> createVariantChildFields() {
    return List.of(
        new Field(METADATA_VECTOR_NAME, new FieldType(false, Binary.INSTANCE, null), null),
        new Field(VALUE_VECTOR_NAME, new FieldType(false, Binary.INSTANCE, null), null));
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    // No-op, as children are initialized in the constructor
  }

  @Override
  public Field getField() {
    return rootField;
  }

  public VarBinaryVector getMetadataVector() {
    return getUnderlyingVector().getChild(METADATA_VECTOR_NAME, VarBinaryVector.class);
  }

  public VarBinaryVector getValueVector() {
    return getUnderlyingVector().getChild(VALUE_VECTOR_NAME, VarBinaryVector.class);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new VariantTransferPair(this, (VariantVector) target);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new VariantTransferPair(this, new VariantVector(field.getName(), allocator));
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(field, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new VariantTransferPair(this, new VariantVector(ref, allocator));
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
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    getUnderlyingVector()
        .copyFrom(fromIndex, thisIndex, ((VariantVector) from).getUnderlyingVector());
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    getUnderlyingVector()
        .copyFromSafe(fromIndex, thisIndex, ((VariantVector) from).getUnderlyingVector());
  }

  @Override
  public Object getObject(int index) {
    if (isNull(index)) {
      return null;
    }
    VarBinaryVector metadataVector = getMetadataVector();
    VarBinaryVector valueVector = getValueVector();

    int metadataStart = metadataVector.getStartOffset(index);
    int metadataEnd = metadataVector.getEndOffset(index);
    int valueStart = valueVector.getStartOffset(index);
    int valueEnd = valueVector.getEndOffset(index);

    return new Variant(
        metadataVector.getDataBuffer(),
        metadataStart,
        metadataEnd,
        valueVector.getDataBuffer(),
        valueStart,
        valueEnd);
  }

  /**
   * Retrieves the variant value at the specified index into the provided holder.
   *
   * @param index the index of the value to retrieve
   * @param holder the holder to populate with the variant data
   */
  public void get(int index, NullableVariantHolder holder) {
    if (isNull(index)) {
      holder.isSet = 0;
    } else {
      holder.isSet = 1;
      VarBinaryVector metadataVector = getMetadataVector();
      VarBinaryVector valueVector = getValueVector();
      assert !metadataVector.isNull(index) && !valueVector.isNull(index);

      holder.metadataStart = metadataVector.getStartOffset(index);
      holder.metadataEnd = metadataVector.getEndOffset(index);
      holder.metadataBuffer = metadataVector.getDataBuffer();
      holder.valueStart = valueVector.getStartOffset(index);
      holder.valueEnd = valueVector.getEndOffset(index);
      holder.valueBuffer = valueVector.getDataBuffer();
    }
  }

  /**
   * Retrieves the variant value at the specified index into the provided non-nullable holder.
   *
   * @param index the index of the value to retrieve
   * @param holder the holder to populate with the variant data
   */
  public void get(int index, VariantHolder holder) {
    VarBinaryVector metadataVector = getMetadataVector();
    VarBinaryVector valueVector = getValueVector();
    assert !metadataVector.isNull(index) && !valueVector.isNull(index);

    holder.metadataStart = metadataVector.getStartOffset(index);
    holder.metadataEnd = metadataVector.getEndOffset(index);
    holder.metadataBuffer = metadataVector.getDataBuffer();
    holder.valueStart = valueVector.getStartOffset(index);
    holder.valueEnd = valueVector.getEndOffset(index);
    holder.valueBuffer = valueVector.getDataBuffer();
  }

  /**
   * Sets the variant value at the specified index from the provided holder.
   *
   * @param index the index at which to set the value
   * @param holder the holder containing the variant data to set
   */
  public void set(int index, VariantHolder holder) {
    BitVectorHelper.setBit(getUnderlyingVector().getValidityBuffer(), index);
    getMetadataVector()
        .set(index, 1, holder.metadataStart, holder.metadataEnd, holder.metadataBuffer);
    getValueVector().set(index, 1, holder.valueStart, holder.valueEnd, holder.valueBuffer);
  }

  /**
   * Sets the variant value at the specified index from the provided nullable holder.
   *
   * @param index the index at which to set the value
   * @param holder the nullable holder containing the variant data to set
   */
  public void set(int index, NullableVariantHolder holder) {
    BitVectorHelper.setValidityBit(getUnderlyingVector().getValidityBuffer(), index, holder.isSet);
    if (holder.isSet == 0) {
      return;
    }
    getMetadataVector()
        .set(index, 1, holder.metadataStart, holder.metadataEnd, holder.metadataBuffer);
    getValueVector().set(index, 1, holder.valueStart, holder.valueEnd, holder.valueBuffer);
  }

  /**
   * Sets the variant value at the specified index from the provided holder, with bounds checking.
   *
   * @param index the index at which to set the value
   * @param holder the holder containing the variant data to set
   */
  public void setSafe(int index, VariantHolder holder) {
    getUnderlyingVector().setIndexDefined(index);
    getMetadataVector()
        .setSafe(index, 1, holder.metadataStart, holder.metadataEnd, holder.metadataBuffer);
    getValueVector().setSafe(index, 1, holder.valueStart, holder.valueEnd, holder.valueBuffer);
  }

  /**
   * Sets the variant value at the specified index from the provided nullable holder, with bounds
   * checking.
   *
   * @param index the index at which to set the value
   * @param holder the nullable holder containing the variant data to set
   */
  public void setSafe(int index, NullableVariantHolder holder) {
    if (holder.isSet == 0) {
      getUnderlyingVector().setNull(index);
      return;
    }
    getUnderlyingVector().setIndexDefined(index);
    getMetadataVector()
        .setSafe(index, 1, holder.metadataStart, holder.metadataEnd, holder.metadataBuffer);
    getValueVector().setSafe(index, 1, holder.valueStart, holder.valueEnd, holder.valueBuffer);
  }

  /** Sets the value at the given index from the provided Variant. */
  public void setSafe(int index, Variant variant) {
    ByteBuffer metadataBuffer = variant.getMetadataBuffer();
    ByteBuffer valueBuffer = variant.getValueBuffer();
    int metadataLength = metadataBuffer.remaining();
    int valueLength = valueBuffer.remaining();
    try (ArrowBuf metaBuf = getAllocator().buffer(metadataLength);
        ArrowBuf valBuf = getAllocator().buffer(valueLength)) {
      metaBuf.setBytes(0, metadataBuffer.duplicate());
      valBuf.setBytes(0, valueBuffer.duplicate());
      getUnderlyingVector().setIndexDefined(index);
      getMetadataVector().setSafe(index, 1, 0, metadataLength, metaBuf);
      getValueVector().setSafe(index, 1, 0, valueLength, valBuf);
    }
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new org.apache.arrow.variant.impl.VariantReaderImpl(this);
  }

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return getUnderlyingVector().hashCode(index, hasher);
  }

  /**
   * VariantTransferPair is a transfer pair for VariantVector. It transfers the metadata and value
   * together using the underlyingVector's transfer pair.
   */
  protected static class VariantTransferPair implements TransferPair {
    private final TransferPair pair;
    private final VariantVector from;
    private final VariantVector to;

    public VariantTransferPair(VariantVector from, VariantVector to) {
      this.from = from;
      this.to = to;
      this.pair = from.getUnderlyingVector().makeTransferPair((to).getUnderlyingVector());
    }

    @Override
    public void transfer() {
      pair.transfer();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      pair.splitAndTransfer(startIndex, length);
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      pair.copyValueSafe(from, to);
    }
  }
}
