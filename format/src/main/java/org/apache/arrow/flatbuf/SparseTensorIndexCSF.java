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
package org.apache.arrow.flatbuf;

import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Compressed Sparse Fiber (CSF) sparse tensor index.
 */
@SuppressWarnings("unused")
public final class SparseTensorIndexCSF extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static SparseTensorIndexCSF getRootAsSparseTensorIndexCSF(ByteBuffer _bb) { return getRootAsSparseTensorIndexCSF(_bb, new SparseTensorIndexCSF()); }
  public static SparseTensorIndexCSF getRootAsSparseTensorIndexCSF(ByteBuffer _bb, SparseTensorIndexCSF obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public SparseTensorIndexCSF __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * CSF is a generalization of compressed sparse row (CSR) index.
   * See [smith2017knl](http://shaden.io/pub-files/smith2017knl.pdf)
   *
   * CSF index recursively compresses each dimension of a tensor into a set
   * of prefix trees. Each path from a root to leaf forms one tensor
   * non-zero index. CSF is implemented with two arrays of buffers and one
   * arrays of integers.
   *
   * For example, let X be a 2x3x4x5 tensor and let it have the following
   * 8 non-zero values:
   * ```text
   *   X[0, 0, 0, 1] := 1
   *   X[0, 0, 0, 2] := 2
   *   X[0, 1, 0, 0] := 3
   *   X[0, 1, 0, 2] := 4
   *   X[0, 1, 1, 0] := 5
   *   X[1, 1, 1, 0] := 6
   *   X[1, 1, 1, 1] := 7
   *   X[1, 1, 1, 2] := 8
   * ```
   * As a prefix tree this would be represented as:
   * ```text
   *         0          1
   *        / \         |
   *       0   1        1
   *      /   / \       |
   *     0   0   1      1
   *    /|  /|   |    /| |
   *   1 2 0 2   0   0 1 2
   * ```
   * The type of values in indptrBuffers
   */
  public org.apache.arrow.flatbuf.Int indptrType() { return indptrType(new org.apache.arrow.flatbuf.Int()); }
  public org.apache.arrow.flatbuf.Int indptrType(org.apache.arrow.flatbuf.Int obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * indptrBuffers stores the sparsity structure.
   * Each two consecutive dimensions in a tensor correspond to a buffer in
   * indptrBuffers. A pair of consecutive values at `indptrBuffers[dim][i]`
   * and `indptrBuffers[dim][i + 1]` signify a range of nodes in
   * `indicesBuffers[dim + 1]` who are children of `indicesBuffers[dim][i]` node.
   *
   * For example, the indptrBuffers for the above X is:
   * ```text
   *   indptrBuffer(X) = [
   *                       [0, 2, 3],
   *                       [0, 1, 3, 4],
   *                       [0, 2, 4, 5, 8]
   *                     ].
   * ```
   */
  public org.apache.arrow.flatbuf.Buffer indptrBuffers(int j) { return indptrBuffers(new org.apache.arrow.flatbuf.Buffer(), j); }
  public org.apache.arrow.flatbuf.Buffer indptrBuffers(org.apache.arrow.flatbuf.Buffer obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o) + j * 16, bb) : null; }
  public int indptrBuffersLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.Buffer.Vector indptrBuffersVector() { return indptrBuffersVector(new org.apache.arrow.flatbuf.Buffer.Vector()); }
  public org.apache.arrow.flatbuf.Buffer.Vector indptrBuffersVector(org.apache.arrow.flatbuf.Buffer.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 16, bb) : null; }
  /**
   * The type of values in indicesBuffers
   */
  public org.apache.arrow.flatbuf.Int indicesType() { return indicesType(new org.apache.arrow.flatbuf.Int()); }
  public org.apache.arrow.flatbuf.Int indicesType(org.apache.arrow.flatbuf.Int obj) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * indicesBuffers stores values of nodes.
   * Each tensor dimension corresponds to a buffer in indicesBuffers.
   * For example, the indicesBuffers for the above X is:
   * ```text
   *   indicesBuffer(X) = [
   *                        [0, 1],
   *                        [0, 1, 1],
   *                        [0, 0, 1, 1],
   *                        [1, 2, 0, 2, 0, 0, 1, 2]
   *                      ].
   * ```
   */
  public org.apache.arrow.flatbuf.Buffer indicesBuffers(int j) { return indicesBuffers(new org.apache.arrow.flatbuf.Buffer(), j); }
  public org.apache.arrow.flatbuf.Buffer indicesBuffers(org.apache.arrow.flatbuf.Buffer obj, int j) { int o = __offset(10); return o != 0 ? obj.__assign(__vector(o) + j * 16, bb) : null; }
  public int indicesBuffersLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.Buffer.Vector indicesBuffersVector() { return indicesBuffersVector(new org.apache.arrow.flatbuf.Buffer.Vector()); }
  public org.apache.arrow.flatbuf.Buffer.Vector indicesBuffersVector(org.apache.arrow.flatbuf.Buffer.Vector obj) { int o = __offset(10); return o != 0 ? obj.__assign(__vector(o), 16, bb) : null; }
  /**
   * axisOrder stores the sequence in which dimensions were traversed to
   * produce the prefix tree.
   * For example, the axisOrder for the above X is:
   * ```text
   *   axisOrder(X) = [0, 1, 2, 3].
   * ```
   */
  public int axisOrder(int j) { int o = __offset(12); return o != 0 ? bb.getInt(__vector(o) + j * 4) : 0; }
  public int axisOrderLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }
  public IntVector axisOrderVector() { return axisOrderVector(new IntVector()); }
  public IntVector axisOrderVector(IntVector obj) { int o = __offset(12); return o != 0 ? obj.__assign(__vector(o), bb) : null; }
  public ByteBuffer axisOrderAsByteBuffer() { return __vector_as_bytebuffer(12, 4); }
  public ByteBuffer axisOrderInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 12, 4); }

  public static int createSparseTensorIndexCSF(FlatBufferBuilder builder,
      int indptrTypeOffset,
      int indptrBuffersOffset,
      int indicesTypeOffset,
      int indicesBuffersOffset,
      int axisOrderOffset) {
    builder.startTable(5);
    SparseTensorIndexCSF.addAxisOrder(builder, axisOrderOffset);
    SparseTensorIndexCSF.addIndicesBuffers(builder, indicesBuffersOffset);
    SparseTensorIndexCSF.addIndicesType(builder, indicesTypeOffset);
    SparseTensorIndexCSF.addIndptrBuffers(builder, indptrBuffersOffset);
    SparseTensorIndexCSF.addIndptrType(builder, indptrTypeOffset);
    return SparseTensorIndexCSF.endSparseTensorIndexCSF(builder);
  }

  public static void startSparseTensorIndexCSF(FlatBufferBuilder builder) { builder.startTable(5); }
  public static void addIndptrType(FlatBufferBuilder builder, int indptrTypeOffset) { builder.addOffset(0, indptrTypeOffset, 0); }
  public static void addIndptrBuffers(FlatBufferBuilder builder, int indptrBuffersOffset) { builder.addOffset(1, indptrBuffersOffset, 0); }
  public static void startIndptrBuffersVector(FlatBufferBuilder builder, int numElems) { builder.startVector(16, numElems, 8); }
  public static void addIndicesType(FlatBufferBuilder builder, int indicesTypeOffset) { builder.addOffset(2, indicesTypeOffset, 0); }
  public static void addIndicesBuffers(FlatBufferBuilder builder, int indicesBuffersOffset) { builder.addOffset(3, indicesBuffersOffset, 0); }
  public static void startIndicesBuffersVector(FlatBufferBuilder builder, int numElems) { builder.startVector(16, numElems, 8); }
  public static void addAxisOrder(FlatBufferBuilder builder, int axisOrderOffset) { builder.addOffset(4, axisOrderOffset, 0); }
  public static int createAxisOrderVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addInt(data[i]); return builder.endVector(); }
  public static void startAxisOrderVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endSparseTensorIndexCSF(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 4);  // indptrType
    builder.required(o, 6);  // indptrBuffers
    builder.required(o, 8);  // indicesType
    builder.required(o, 10);  // indicesBuffers
    builder.required(o, 12);  // axisOrder
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public SparseTensorIndexCSF get(int j) { return get(new SparseTensorIndexCSF(), j); }
    public SparseTensorIndexCSF get(SparseTensorIndexCSF obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
