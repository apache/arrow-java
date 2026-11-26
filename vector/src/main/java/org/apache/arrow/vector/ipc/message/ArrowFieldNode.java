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
package org.apache.arrow.vector.ipc.message;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.util.Preconditions;

/** Metadata about Vectors/Arrays that is written to a channel. */
public class ArrowFieldNode implements FBSerializable {

  private final int length;
  private final int nullCount;

  /**
   * Constructs a new instance.
   *
   * @param length The number of values written.
   * @param nullCount The number of null values, or -1 if unknown.
   */
  public ArrowFieldNode(long length, long nullCount) {
    super();
    Preconditions.checkArgument(length >= 0, "Expected length >= 0, got %s", length);
    Preconditions.checkArgument(nullCount >= -1, "Expected nullCount >= -1, got %s", nullCount);
    this.length = Math.toIntExact(length);
    this.nullCount = Math.toIntExact(nullCount);
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    return FieldNode.createFieldNode(builder, length, nullCount);
  }

  public int getNullCount() {
    return nullCount;
  }

  public int getLength() {
    return length;
  }

  @Override
  public String toString() {
    return "ArrowFieldNode [length=" + length + ", nullCount=" + nullCount + "]";
  }
}
