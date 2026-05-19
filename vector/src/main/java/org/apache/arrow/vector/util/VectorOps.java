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
package org.apache.arrow.vector.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;

/**
 * Generic utility operations for creating new vectors and roots from existing ones, without
 * requiring per-type implementations. These operations work by manipulating whole buffers via
 * {@link FieldVector#getFieldBuffers()} and {@link FieldVector#loadFieldBuffers}, rather than
 * type-specific internal logic.
 *
 * <p>Three modes of creation are provided:
 *
 * <ul>
 *   <li>{@code shareCopy} - the new vector shares the same underlying memory as the source, with
 *       reference counts incremented. Both source and result remain usable. The underlying memory
 *       is released only when all sharing vectors have been closed.
 *   <li>{@code transferCopy} - buffer ownership is transferred from the source to the new vector.
 *       The source is left with empty buffers and can be reused via {@code allocateNew()}.
 *   <li>{@code deepCopy} - a deep copy is made. The new vector has independent buffers with the
 *       same data. Both source and result are fully independent.
 * </ul>
 */
public final class VectorOps {

  private VectorOps() {}

  /**
   * Create a new vector sharing the same underlying memory as the source. Reference counts are
   * incremented so that the memory is only released when all sharing vectors have been closed. The
   * source vector is not modified.
   *
   * <p>Uses each source vector's own allocator for the target.
   *
   * @param source the vector to share from
   * @param <V> the vector type
   * @return a new vector sharing the same underlying memory
   */
  @SuppressWarnings("unchecked")
  public static <V extends FieldVector> V shareCopy(V source) {
    return (V) shareCopy(source, source.getAllocator());
  }

  /**
   * Create a new vector sharing the same underlying memory as the source, associated with the given
   * allocator. Reference counts are incremented so that the memory is only released when all
   * sharing vectors have been closed. The source vector is not modified.
   *
   * @param source the vector to share from
   * @param allocator the allocator for the new vector
   * @param <V> the vector type
   * @return a new vector sharing the same underlying memory
   */
  @SuppressWarnings("unchecked")
  public static <V extends FieldVector> V shareCopy(V source, BufferAllocator allocator) {
    FieldVector target = source.getField().createVector(allocator);
    shareCopyInto(source, target);
    return (V) target;
  }

  /**
   * Create a new VectorSchemaRoot sharing the same underlying memory as the source. Reference
   * counts are incremented so that the memory is only released when all sharing roots have been
   * closed. The source root is not modified.
   *
   * <p>Uses each source vector's own allocator for its corresponding target vector.
   *
   * @param source the root to share from
   * @return a new root sharing the same underlying memory
   */
  public static VectorSchemaRoot shareCopy(VectorSchemaRoot source) {
    List<FieldVector> sharedVectors =
        source.getFieldVectors().stream().map(v -> shareCopy(v)).collect(Collectors.toList());
    VectorSchemaRoot result = new VectorSchemaRoot(sharedVectors);
    result.setRowCount(source.getRowCount());
    return result;
  }

  /**
   * Create a new VectorSchemaRoot sharing the same underlying memory as the source, with all
   * vectors associated with the given allocator.
   *
   * @param source the root to share from
   * @param allocator the allocator for all vectors in the new root
   * @return a new root sharing the same underlying memory
   */
  public static VectorSchemaRoot shareCopy(VectorSchemaRoot source, BufferAllocator allocator) {
    List<FieldVector> sharedVectors =
        source.getFieldVectors().stream()
            .map(v -> shareCopy(v, allocator))
            .collect(Collectors.toList());
    VectorSchemaRoot result = new VectorSchemaRoot(sharedVectors);
    result.setRowCount(source.getRowCount());
    return result;
  }

  /**
   * Create a new vector by transferring buffer ownership from the source. The source is left with
   * empty buffers and can be reused via {@code allocateNew()}.
   *
   * <p>Uses each source vector's own allocator for the target.
   *
   * @param source the vector to transfer from
   * @param <V> the vector type
   * @return a new vector owning the transferred buffers
   */
  @SuppressWarnings("unchecked")
  public static <V extends FieldVector> V transferCopy(V source) {
    return (V) transferCopy(source, source.getAllocator());
  }

  /**
   * Create a new vector by transferring buffer ownership from the source, associated with the given
   * allocator. The source is left with empty buffers and can be reused via {@code allocateNew()}.
   *
   * @param source the vector to transfer from
   * @param allocator the allocator for the new vector
   * @param <V> the vector type
   * @return a new vector owning the transferred buffers
   */
  @SuppressWarnings("unchecked")
  public static <V extends FieldVector> V transferCopy(V source, BufferAllocator allocator) {
    FieldVector target = source.getField().createVector(allocator);
    transferCopyInto(source, target);
    return (V) target;
  }

  /**
   * Create a new VectorSchemaRoot by transferring buffer ownership from the source. The source
   * root's vectors are left with empty buffers and can be reused via {@code allocateNew()}.
   *
   * <p>Uses each source vector's own allocator for its corresponding target vector.
   *
   * @param source the root to transfer from
   * @return a new root owning the transferred buffers
   */
  public static VectorSchemaRoot transferCopy(VectorSchemaRoot source) {
    List<FieldVector> transferredVectors =
        source.getFieldVectors().stream().map(v -> transferCopy(v)).collect(Collectors.toList());
    VectorSchemaRoot result = new VectorSchemaRoot(transferredVectors);
    result.setRowCount(source.getRowCount());
    return result;
  }

  /**
   * Create a new VectorSchemaRoot by transferring buffer ownership from the source, with all
   * vectors associated with the given allocator.
   *
   * @param source the root to transfer from
   * @param allocator the allocator for all vectors in the new root
   * @return a new root owning the transferred buffers
   */
  public static VectorSchemaRoot transferCopy(VectorSchemaRoot source, BufferAllocator allocator) {
    List<FieldVector> transferredVectors =
        source.getFieldVectors().stream()
            .map(v -> transferCopy(v, allocator))
            .collect(Collectors.toList());
    VectorSchemaRoot result = new VectorSchemaRoot(transferredVectors);
    result.setRowCount(source.getRowCount());
    return result;
  }

  /**
   * Create a new vector with an independent deep copy of the source data. Both source and result
   * are fully independent after this operation.
   *
   * <p>Uses each source vector's own allocator for the target.
   *
   * @param source the vector to copy
   * @param <V> the vector type
   * @return a new vector with independent copies of all buffers
   */
  @SuppressWarnings("unchecked")
  public static <V extends FieldVector> V deepCopy(V source) {
    return (V) deepCopy(source, source.getAllocator());
  }

  /**
   * Create a new vector with an independent deep copy of the source data, associated with the given
   * allocator. Both source and result are fully independent after this operation.
   *
   * @param source the vector to copy
   * @param allocator the allocator for the new vector
   * @param <V> the vector type
   * @return a new vector with independent copies of all buffers
   */
  @SuppressWarnings("unchecked")
  public static <V extends FieldVector> V deepCopy(V source, BufferAllocator allocator) {
    FieldVector target = source.getField().createVector(allocator);
    deepCopyInto(source, target);
    return (V) target;
  }

  /**
   * Create a new VectorSchemaRoot with an independent deep copy of the source data. Both source and
   * result are fully independent after this operation.
   *
   * <p>Uses each source vector's own allocator for its corresponding target vector.
   *
   * @param source the root to copy
   * @return a new root with independent copies of all data
   */
  public static VectorSchemaRoot deepCopy(VectorSchemaRoot source) {
    List<FieldVector> copiedVectors =
        source.getFieldVectors().stream().map(v -> deepCopy(v)).collect(Collectors.toList());
    VectorSchemaRoot result = new VectorSchemaRoot(copiedVectors);
    result.setRowCount(source.getRowCount());
    return result;
  }

  /**
   * Create a new VectorSchemaRoot with an independent deep copy of the source data, with all
   * vectors associated with the given allocator.
   *
   * @param source the root to copy
   * @param allocator the allocator for all vectors in the new root
   * @return a new root with independent copies of all data
   */
  public static VectorSchemaRoot deepCopy(VectorSchemaRoot source, BufferAllocator allocator) {
    List<FieldVector> copiedVectors =
        source.getFieldVectors().stream()
            .map(v -> deepCopy(v, allocator))
            .collect(Collectors.toList());
    VectorSchemaRoot result = new VectorSchemaRoot(copiedVectors);
    result.setRowCount(source.getRowCount());
    return result;
  }

  private static void shareCopyInto(FieldVector source, FieldVector target) {
    List<ArrowBuf> sourceBuffers = source.getFieldBuffers();
    ArrowFieldNode node = new ArrowFieldNode(source.getValueCount(), source.getNullCount());
    target.loadFieldBuffers(node, sourceBuffers);

    List<FieldVector> sourceChildren = source.getChildrenFromFields();
    List<FieldVector> targetChildren = target.getChildrenFromFields();
    for (int i = 0; i < sourceChildren.size(); i++) {
      shareCopyInto(sourceChildren.get(i), targetChildren.get(i));
    }
  }

  private static void transferCopyInto(FieldVector source, FieldVector target) {
    shareCopyInto(source, target);
    source.clear();
  }

  private static void deepCopyInto(FieldVector source, FieldVector target) {
    List<ArrowBuf> sourceBuffers = source.getFieldBuffers();
    BufferAllocator targetAllocator = target.getAllocator();
    List<ArrowBuf> copiedBuffers = new ArrayList<>(sourceBuffers.size());
    for (ArrowBuf buf : sourceBuffers) {
      long size = buf.readableBytes();
      ArrowBuf copy = targetAllocator.buffer(size);
      copy.setBytes(0, buf, 0, size);
      copy.writerIndex(size);
      copiedBuffers.add(copy);
    }
    ArrowFieldNode node = new ArrowFieldNode(source.getValueCount(), source.getNullCount());
    target.loadFieldBuffers(node, copiedBuffers);
    for (ArrowBuf buf : copiedBuffers) {
      buf.close();
    }

    List<FieldVector> sourceChildren = source.getChildrenFromFields();
    List<FieldVector> targetChildren = target.getChildrenFromFields();
    for (int i = 0; i < sourceChildren.size(); i++) {
      deepCopyInto(sourceChildren.get(i), targetChildren.get(i));
    }
  }
}
