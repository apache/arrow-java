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
 * Optional compression for the memory buffers constituting IPC message
 * bodies. Intended for use with RecordBatch but could be used for other
 * message types
 */
@SuppressWarnings("unused")
public final class BodyCompression extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static BodyCompression getRootAsBodyCompression(ByteBuffer _bb) { return getRootAsBodyCompression(_bb, new BodyCompression()); }
  public static BodyCompression getRootAsBodyCompression(ByteBuffer _bb, BodyCompression obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public BodyCompression __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * Compressor library.
   * For LZ4_FRAME, each compressed buffer must consist of a single frame.
   */
  public byte codec() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  /**
   * Indicates the way the record batch body was compressed
   */
  public byte method() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) : 0; }

  public static int createBodyCompression(FlatBufferBuilder builder,
      byte codec,
      byte method) {
    builder.startTable(2);
    BodyCompression.addMethod(builder, method);
    BodyCompression.addCodec(builder, codec);
    return BodyCompression.endBodyCompression(builder);
  }

  public static void startBodyCompression(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addCodec(FlatBufferBuilder builder, byte codec) { builder.addByte(0, codec, 0); }
  public static void addMethod(FlatBufferBuilder builder, byte method) { builder.addByte(1, method, 0); }
  public static int endBodyCompression(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public BodyCompression get(int j) { return get(new BodyCompression(), j); }
    public BodyCompression get(BodyCompression obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
