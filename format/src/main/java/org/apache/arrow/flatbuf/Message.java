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

@SuppressWarnings("unused")
public final class Message extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static Message getRootAsMessage(ByteBuffer _bb) { return getRootAsMessage(_bb, new Message()); }
  public static Message getRootAsMessage(ByteBuffer _bb, Message obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Message __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public short version() { int o = __offset(4); return o != 0 ? bb.getShort(o + bb_pos) : 0; }
  public byte headerType() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table header(Table obj) { int o = __offset(8); return o != 0 ? __union(obj, o + bb_pos) : null; }
  public long bodyLength() { int o = __offset(10); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public org.apache.arrow.flatbuf.KeyValue customMetadata(int j) { return customMetadata(new org.apache.arrow.flatbuf.KeyValue(), j); }
  public org.apache.arrow.flatbuf.KeyValue customMetadata(org.apache.arrow.flatbuf.KeyValue obj, int j) { int o = __offset(12); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int customMetadataLength() { int o = __offset(12); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.KeyValue.Vector customMetadataVector() { return customMetadataVector(new org.apache.arrow.flatbuf.KeyValue.Vector()); }
  public org.apache.arrow.flatbuf.KeyValue.Vector customMetadataVector(org.apache.arrow.flatbuf.KeyValue.Vector obj) { int o = __offset(12); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createMessage(FlatBufferBuilder builder,
      short version,
      byte headerType,
      int headerOffset,
      long bodyLength,
      int customMetadataOffset) {
    builder.startTable(5);
    Message.addBodyLength(builder, bodyLength);
    Message.addCustomMetadata(builder, customMetadataOffset);
    Message.addHeader(builder, headerOffset);
    Message.addVersion(builder, version);
    Message.addHeaderType(builder, headerType);
    return Message.endMessage(builder);
  }

  public static void startMessage(FlatBufferBuilder builder) { builder.startTable(5); }
  public static void addVersion(FlatBufferBuilder builder, short version) { builder.addShort(0, version, 0); }
  public static void addHeaderType(FlatBufferBuilder builder, byte headerType) { builder.addByte(1, headerType, 0); }
  public static void addHeader(FlatBufferBuilder builder, int headerOffset) { builder.addOffset(2, headerOffset, 0); }
  public static void addBodyLength(FlatBufferBuilder builder, long bodyLength) { builder.addLong(3, bodyLength, 0L); }
  public static void addCustomMetadata(FlatBufferBuilder builder, int customMetadataOffset) { builder.addOffset(4, customMetadataOffset, 0); }
  public static int createCustomMetadataVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startCustomMetadataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endMessage(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishMessageBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedMessageBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Message get(int j) { return get(new Message(), j); }
    public Message get(Message obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}
