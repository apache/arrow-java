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
package org.apache.arrow.vector.validate;

import static org.apache.arrow.vector.validate.ValidateUtil.validateOrThrow;

import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.UnionVector;

/** Utility for validating vector data. */
public class ValidateVectorDataVisitor implements VectorVisitor<Void, Void> {

  private void validateOffsetBuffer(ValueVector vector, int valueCount) {
    if (valueCount == 0) {
      return;
    }
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();

    // verify that the values in the offset buffer is non-decreasing
    int prevValue = offsetBuffer.getInt(0);
    for (int i = 1; i <= valueCount; i++) {
      int curValue = offsetBuffer.getInt(i * 4);
      validateOrThrow(
          curValue >= 0,
          "The value at position %s of the offset buffer is negative: %s.",
          i,
          curValue);
      validateOrThrow(
          curValue >= prevValue,
          "The values in positions %s and %s of the offset buffer are decreasing: %s, %s.",
          i - 1,
          i,
          prevValue,
          curValue);
      prevValue = curValue;
    }
  }

  private void validateLargeOffsetBuffer(ValueVector vector, int valueCount) {
    if (valueCount == 0) {
      return;
    }
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();

    // verify that the values in the large offset buffer is non-decreasing
    long prevValue = offsetBuffer.getLong(0);
    for (int i = 1; i <= valueCount; i++) {
      long curValue = offsetBuffer.getLong((long) i * 8);
      validateOrThrow(
          curValue >= 0L,
          "The value at position %s of the large offset buffer is negative: %s.",
          i,
          curValue);
      validateOrThrow(
          curValue >= prevValue,
          "The values in positions %s and %s of the large offset buffer are decreasing: %s, %s.",
          i - 1,
          i,
          prevValue,
          curValue);
      prevValue = curValue;
    }
  }

  private void validateTypeBuffer(ArrowBuf typeBuf, int valueCount) {
    for (int i = 0; i < valueCount; i++) {
      validateOrThrow(
          typeBuf.getByte(i) >= 0,
          "The type id at position %s is negative: %s.",
          i,
          typeBuf.getByte(i));
    }
  }

  @Override
  public Void visit(BaseFixedWidthVector vector, Void value) {
    vector.validateScalars();
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthVector vector, Void value) {
    validateOffsetBuffer(vector, vector.getValueCount());
    vector.validateScalars();
    return null;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector vector, Void value) {
    validateLargeOffsetBuffer(vector, vector.getValueCount());
    vector.validateScalars();
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthViewVector vector, Void value) {
    validateViews(vector);
    vector.validateScalars();
    return null;
  }

  private void validateViews(BaseVariableWidthViewVector vector) {
    ArrowBuf viewBuffer = vector.getDataBuffer();
    List<ArrowBuf> dataBuffers = vector.getDataBuffers();
    for (int i = 0; i < vector.getValueCount(); i++) {
      if (vector.isNull(i)) {
        continue;
      }
      long start = (long) i * BaseVariableWidthViewVector.ELEMENT_SIZE;
      int length = viewBuffer.getInt(start);
      validateOrThrow(length >= 0, "The view at position %s has a negative length: %s.", i, length);
      if (length > BaseVariableWidthViewVector.INLINE_SIZE) {
        long bufferIndexStart =
            start
                + BaseVariableWidthViewVector.LENGTH_WIDTH
                + BaseVariableWidthViewVector.PREFIX_WIDTH;
        int bufferIndex = viewBuffer.getInt(bufferIndexStart);
        int offset =
            viewBuffer.getInt(bufferIndexStart + BaseVariableWidthViewVector.BUF_INDEX_WIDTH);
        validateOrThrow(
            bufferIndex >= 0 && bufferIndex < dataBuffers.size(),
            "The view at position %s refers to data buffer %s, but there are %s data buffers.",
            i,
            bufferIndex,
            dataBuffers.size());
        long capacity = dataBuffers.get(bufferIndex).capacity();
        validateOrThrow(
            offset >= 0 && (long) offset + length <= capacity,
            "The view at position %s (offset %s, length %s) is outside data buffer %s of %s bytes.",
            i,
            offset,
            length,
            bufferIndex,
            capacity);
      }
    }
  }

  @Override
  public Void visit(ListVector vector, Void value) {
    validateOffsetBuffer(vector, vector.getValueCount());
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(FixedSizeListVector vector, Void value) {
    // A fixed size list has no offset buffer, and the buffer validation checks the inner length
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(LargeListVector vector, Void value) {
    validateLargeOffsetBuffer(vector, vector.getValueCount());
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(ListViewVector vector, Void value) {
    ValueVector innerVector = vector.getDataVector();
    int innerValueCount = innerVector == null ? 0 : innerVector.getValueCount();
    for (int i = 0; i < vector.getValueCount(); i++) {
      if (vector.isNull(i)) {
        continue;
      }
      int offset = vector.getOffsetBuffer().getInt((long) i * ListViewVector.OFFSET_WIDTH);
      int size = vector.getSizeBuffer().getInt((long) i * ListViewVector.SIZE_WIDTH);
      validateListView(i, offset, size, innerValueCount);
    }
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(LargeListViewVector vector, Void value) {
    ValueVector innerVector = vector.getDataVector();
    int innerValueCount = innerVector == null ? 0 : innerVector.getValueCount();
    for (int i = 0; i < vector.getValueCount(); i++) {
      if (vector.isNull(i)) {
        continue;
      }
      long offset = vector.getOffsetBuffer().getLong((long) i * LargeListViewVector.OFFSET_WIDTH);
      long size = vector.getSizeBuffer().getLong((long) i * LargeListViewVector.SIZE_WIDTH);
      validateListView(i, offset, size, innerValueCount);
    }
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  private void validateListView(int index, long offset, long size, int innerValueCount) {
    validateOrThrow(
        size >= 0, "The size at position %s of the list view is negative: %s.", index, size);
    validateOrThrow(
        size == 0 || (offset >= 0 && offset <= innerValueCount - size),
        "The list view at position %s (offset %s, size %s) is outside the inner vector of %s"
            + " values.",
        index,
        offset,
        size,
        innerValueCount);
  }

  @Override
  public Void visit(RunEndEncodedVector vector, Void value) {
    FieldVector runEndsVector = vector.getRunEndsVector();
    FieldVector valuesVector = vector.getValuesVector();
    if (runEndsVector != null) {
      int runCount = runEndsVector.getValueCount();
      // Run ends are positive and strictly increasing, and each run has a value. Before its
      // children are initialized, the run ends vector is empty and not an integer vector.
      if (runCount > 0) {
        BaseIntVector runEnds = (BaseIntVector) runEndsVector;
        long previousEnd = 0;
        for (int i = 0; i < runCount; i++) {
          long end = runEnds.getValueAsLong(i);
          validateOrThrow(
              end > previousEnd,
              "The run end at position %s (%s) is not greater than the previous run end (%s).",
              i,
              end,
              previousEnd);
          previousEnd = end;
        }
        int valueCount = valuesVector == null ? 0 : valuesVector.getValueCount();
        validateOrThrow(
            valueCount >= runCount,
            "The values vector has %s values, fewer than the %s runs.",
            valueCount,
            runCount);
      }
      runEndsVector.accept(this, null);
    }
    if (valuesVector != null) {
      valuesVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NonNullableStructVector vector, Void value) {
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(UnionVector vector, Void value) {
    validateTypeBuffer(vector.getTypeBuffer(), vector.getValueCount());
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(DenseUnionVector vector, Void value) {
    validateTypeBuffer(vector.getTypeBuffer(), vector.getValueCount());

    // validate offset buffer
    for (int i = 0; i < vector.getValueCount(); i++) {
      int offset = vector.getOffset(i);
      byte typeId = vector.getTypeId(i);
      ValueVector subVector = vector.getVectorByType(typeId);
      validateOrThrow(
          subVector != null,
          "Dense union vector type id %s at position %s has no sub-vector.",
          typeId,
          i);
      validateOrThrow(
          offset >= 0 && offset < subVector.getValueCount(),
          "Dense union vector offset exceeds sub-vector boundary. Vector offset %s, sub vector size %s",
          offset,
          subVector.getValueCount());
    }

    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NullVector vector, Void value) {
    ValidateUtil.validateOrThrow(
        vector.getNullCount() == vector.getValueCount(),
        "NullVector should have only null entries.");
    return null;
  }

  @Override
  public Void visit(ExtensionTypeVector<?> vector, Void value) {
    vector.getUnderlyingVector().accept(this, value);
    return null;
  }
}
