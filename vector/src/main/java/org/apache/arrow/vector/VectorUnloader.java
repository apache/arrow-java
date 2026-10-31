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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Helper class that handles converting a {@link VectorSchemaRoot} to a {@link ArrowRecordBatch}.
 */
public class VectorUnloader {

  private final VectorSchemaRoot root;
  private final boolean includeNullCount;
  private final CompressionCodec codec;
  private final boolean alignBuffers;

  /** Constructs a new instance of the given set of vectors. */
  public VectorUnloader(VectorSchemaRoot root) {
    this(root, true, NoCompressionCodec.INSTANCE, true);
  }

  /**
   * Constructs a new instance.
   *
   * @param root The set of vectors to serialize to an {@link ArrowRecordBatch}.
   * @param includeNullCount Controls whether null count is copied to the {@link ArrowRecordBatch}
   * @param alignBuffers Controls if buffers get aligned to 8-byte boundaries.
   */
  public VectorUnloader(VectorSchemaRoot root, boolean includeNullCount, boolean alignBuffers) {
    this(root, includeNullCount, NoCompressionCodec.INSTANCE, alignBuffers);
  }

  /**
   * Constructs a new instance.
   *
   * @param root The set of vectors to serialize to an {@link ArrowRecordBatch}.
   * @param includeNullCount Controls whether null count is copied to the {@link ArrowRecordBatch}
   * @param codec the codec for compressing data. If it is null, then no compression is needed.
   * @param alignBuffers Controls if buffers get aligned to 8-byte boundaries.
   */
  public VectorUnloader(
      VectorSchemaRoot root,
      boolean includeNullCount,
      CompressionCodec codec,
      boolean alignBuffers) {
    this.root = root;
    this.includeNullCount = includeNullCount;
    this.codec = codec == null ? NoCompressionCodec.INSTANCE : codec;
    this.alignBuffers = alignBuffers;
  }

  /**
   * Performs the depth first traversal of the Vectors to create an {@link ArrowRecordBatch}
   * suitable for serialization.
   *
   * <p>A variable-width, list or map vector whose offsets start past zero, or whose child vector
   * holds elements past its last offset, as one imported through the C Data interface from a sliced
   * array does, is written as the values between its first and last offset, with the offsets
   * re-based to start at zero.
   */
  public ArrowRecordBatch getRecordBatch() {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    List<Long> variadicBufferCounts = new ArrayList<>();
    try {
      for (FieldVector vector : root.getFieldVectors()) {
        appendNodes(vector, nodes, buffers, variadicBufferCounts);
      }
      // Do NOT retain buffers in ArrowRecordBatch constructor since we have already retained them.
      return new ArrowRecordBatch(
          root.getRowCount(),
          nodes,
          buffers,
          CompressionUtil.createBodyCompression(codec),
          variadicBufferCounts,
          alignBuffers, /*retainBuffers*/
          false);
    } catch (RuntimeException | Error e) {
      AutoCloseables.close(e, buffers);
      throw e;
    }
  }

  private long getVariadicBufferCount(FieldVector vector) {
    if (vector instanceof BaseVariableWidthViewVector) {
      return ((BaseVariableWidthViewVector) vector).getDataBuffers().size();
    }
    return 0L;
  }

  private void appendNodes(
      FieldVector vector,
      List<ArrowFieldNode> nodes,
      List<ArrowBuf> buffers,
      List<Long> variadicBufferCounts) {
    FieldVector storageVector = ExtensionTypeVector.getStorageVector(vector);
    if (firstOffset(storageVector) != 0) {
      appendRebased(vector, vector.getValueCount(), nodes, buffers, variadicBufferCounts);
      return;
    }
    nodes.add(
        new ArrowFieldNode(vector.getValueCount(), includeNullCount ? vector.getNullCount() : -1));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    long variadicBufferCount = getVariadicBufferCount(storageVector);
    int expectedBufferCount =
        (int) (TypeLayout.getTypeBufferCount(vector.getField().getType()) + variadicBufferCount);
    // only update variadicBufferCounts for vectors that have variadic buffers
    if (storageVector instanceof BaseVariableWidthViewVector) {
      variadicBufferCounts.add(variadicBufferCount);
    }
    if (fieldBuffers.size() != expectedBufferCount) {
      throw new IllegalArgumentException(
          String.format(
              "wrong number of buffers for field %s in vector %s. found: %s",
              vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
    }
    for (ArrowBuf buf : fieldBuffers) {
      // If the codec is NoCompressionCodec, then it will return the input buffer unchanged. In that
      // case,
      // we need to retain it for ArrowRecordBatch. Otherwise, it will return a new buffer, and also
      // close
      // the input buffer. In that case, we need to retain the input buffer still to avoid modifying
      // the source VectorSchemaRoot.
      buf.getReferenceManager().retain();
      buffers.add(codec.compress(vector.getAllocator(), buf));
    }
    long coveredChildLength = coveredChildLength(storageVector);
    for (FieldVector child : vector.getChildrenFromFields()) {
      if (coveredChildLength >= 0 && child.getValueCount() > coveredChildLength) {
        appendRebased(child, (int) coveredChildLength, nodes, buffers, variadicBufferCounts);
      } else {
        appendNodes(child, nodes, buffers, variadicBufferCounts);
      }
    }
  }

  /** Appends the first {@code length} values of the vector, re-based to start at offset zero. */
  private void appendRebased(
      FieldVector vector,
      int length,
      List<ArrowFieldNode> nodes,
      List<ArrowBuf> buffers,
      List<Long> variadicBufferCounts) {
    TransferPair pair = vector.getTransferPair(vector.getField(), vector.getAllocator());
    pair.splitAndTransfer(0, length);
    try (FieldVector rebased = (FieldVector) pair.getTo()) {
      appendNodes(rebased, nodes, buffers, variadicBufferCounts);
    }
  }

  /** The first offset of a variable-width, list or map vector, or zero for any other vector. */
  private static long firstOffset(FieldVector vector) {
    if (vector.getValueCount() == 0) {
      return 0;
    }
    if (vector instanceof BaseVariableWidthVector) {
      return ((BaseVariableWidthVector) vector).getStartOffset(0);
    }
    if (vector instanceof BaseLargeVariableWidthVector) {
      return ((BaseLargeVariableWidthVector) vector).getStartOffset(0);
    }
    if (vector instanceof ListVector) {
      return ((ListVector) vector).getElementStartIndex(0);
    }
    if (vector instanceof LargeListVector) {
      return ((LargeListVector) vector).getElementStartIndex(0);
    }
    return 0;
  }

  /**
   * The number of child elements a list or map vector's offsets cover, which is its last offset, or
   * -1 for a vector whose child is not addressed by offsets.
   */
  private static long coveredChildLength(FieldVector vector) {
    int valueCount = vector.getValueCount();
    if (valueCount == 0) {
      return -1;
    }
    if (vector instanceof ListVector) {
      return ((ListVector) vector).getElementEndIndex(valueCount - 1);
    }
    if (vector instanceof LargeListVector) {
      return ((LargeListVector) vector).getElementEndIndex(valueCount - 1);
    }
    return -1;
  }
}
