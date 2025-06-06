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
 * A data header describing the shared memory layout of a "record" or "row"
 * batch. Some systems call this a "row batch" internally and others a "record
 * batch".
 */
@SuppressWarnings("unused")
public final class RecordBatch extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static RecordBatch getRootAsRecordBatch(ByteBuffer _bb) { return getRootAsRecordBatch(_bb, new RecordBatch()); }
  public static RecordBatch getRootAsRecordBatch(ByteBuffer _bb, RecordBatch obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public RecordBatch __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * number of records / rows. The arrays in the batch should all have this
   * length
   */
  public long length() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  /**
   * Nodes correspond to the pre-ordered flattened logical schema
   */
  public org.apache.arrow.flatbuf.FieldNode nodes(int j) { return nodes(new org.apache.arrow.flatbuf.FieldNode(), j); }
  public org.apache.arrow.flatbuf.FieldNode nodes(org.apache.arrow.flatbuf.FieldNode obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o) + j * 16, bb) : null; }
  public int nodesLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.FieldNode.Vector nodesVector() { return nodesVector(new org.apache.arrow.flatbuf.FieldNode.Vector()); }
  public org.apache.arrow.flatbuf.FieldNode.Vector nodesVector(org.apache.arrow.flatbuf.FieldNode.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 16, bb) : null; }
  /**
   * Buffers correspond to the pre-ordered flattened buffer tree
   *
   * The number of buffers appended to this list depends on the schema. For
   * example, most primitive arrays will have 2 buffers, 1 for the validity
   * bitmap and 1 for the values. For struct arrays, there will only be a
   * single buffer for the validity (nulls) bitmap
   */
  public org.apache.arrow.flatbuf.Buffer buffers(int j) { return buffers(new org.apache.arrow.flatbuf.Buffer(), j); }
  public org.apache.arrow.flatbuf.Buffer buffers(org.apache.arrow.flatbuf.Buffer obj, int j) { int o = __offset(8); return o != 0 ? obj.__assign(__vector(o) + j * 16, bb) : null; }
  public int buffersLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.Buffer.Vector buffersVector() { return buffersVector(new org.apache.arrow.flatbuf.Buffer.Vector()); }
  public org.apache.arrow.flatbuf.Buffer.Vector buffersVector(org.apache.arrow.flatbuf.Buffer.Vector obj) { int o = __offset(8); return o != 0 ? obj.__assign(__vector(o), 16, bb) : null; }
  /**
   * Optional compression of the message body
   */
  public org.apache.arrow.flatbuf.BodyCompression compression() { return compression(new org.apache.arrow.flatbuf.BodyCompression()); }
  public org.apache.arrow.flatbuf.BodyCompression compression(org.apache.arrow.flatbuf.BodyCompression obj) { int o = __offset(10); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * Some types such as Utf8View are represented using a variable number of buffers.
   * For each such Field in the pre-ordered flattened logical schema, there will be
   * an entry in variadicBufferCounts to indicate the number of number of variadic
   * buffers which belong to that Field in the current RecordBatch.
   *
   * For example, the schema
   *     col1: Struct<alpha: Int32, beta: BinaryView, gamma: Float64>
   *     col2: Utf8View
   * contains two Fields with variadic buffers so variadicBufferCounts will have
   * two entries, the first counting the variadic buffers of `col1.beta` and the
   * second counting `col2`'s.
   *
   * This field may be omitted if and only if the schema contains no Fields with
   * a variable number of buffers, such as BinaryView and Utf8View.
   */
  public long variadicBufferCounts(int j) { int o = __offset(12); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int variadicBufferCountsLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }
  public LongVector variadicBufferCountsVector() { return variadicBufferCountsVector(new LongVector()); }
  public LongVector variadicBufferCountsVector(LongVector obj) { int o = __offset(12); return o != 0 ? obj.__assign(__vector(o), bb) : null; }
  public ByteBuffer variadicBufferCountsAsByteBuffer() { return __vector_as_bytebuffer(12, 8); }
  public ByteBuffer variadicBufferCountsInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 12, 8); }

  public static int createRecordBatch(FlatBufferBuilder builder,
      long length,
      int nodesOffset,
      int buffersOffset,
      int compressionOffset,
      int variadicBufferCountsOffset) {
    builder.startTable(5);
    RecordBatch.addLength(builder, length);
    RecordBatch.addVariadicBufferCounts(builder, variadicBufferCountsOffset);
    RecordBatch.addCompression(builder, compressionOffset);
    RecordBatch.addBuffers(builder, buffersOffset);
    RecordBatch.addNodes(builder, nodesOffset);
    return RecordBatch.endRecordBatch(builder);
  }

  public static void startRecordBatch(FlatBufferBuilder builder) { builder.startTable(5); }
  public static void addLength(FlatBufferBuilder builder, long length) { builder.addLong(0, length, 0L); }
  public static void addNodes(FlatBufferBuilder builder, int nodesOffset) { builder.addOffset(1, nodesOffset, 0); }
  public static void startNodesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(16, numElems, 8); }
  public static void addBuffers(FlatBufferBuilder builder, int buffersOffset) { builder.addOffset(2, buffersOffset, 0); }
  public static void startBuffersVector(FlatBufferBuilder builder, int numElems) { builder.startVector(16, numElems, 8); }
  public static void addCompression(FlatBufferBuilder builder, int compressionOffset) { builder.addOffset(3, compressionOffset, 0); }
  public static void addVariadicBufferCounts(FlatBufferBuilder builder, int variadicBufferCountsOffset) { builder.addOffset(4, variadicBufferCountsOffset, 0); }
  public static int createVariadicBufferCountsVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startVariadicBufferCountsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endRecordBatch(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public RecordBatch get(int j) { return get(new RecordBatch(), j); }
    public RecordBatch get(RecordBatch obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
