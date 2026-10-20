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
package org.apache.arrow.memory.util;

import java.nio.ByteBuffer;

/**
 * Pluggable backend for {@link MemoryUtil}'s low-level memory operations. The default
 * implementation ({@link UnsafeMemoryAccessor}) is backed by {@code sun.misc.Unsafe}; the {@code
 * arrow-memory-ffm} module supplies an alternative backed by {@code java.lang.foreign}.
 */
public interface MemoryUtilAccessor {
  long allocateMemory(long bytes);

  void freeMemory(long address);

  byte getByte(long address);

  void putByte(long address, byte value);

  short getShort(long address);

  void putShort(long address, short value);

  int getInt(long address);

  void putInt(long address, int value);

  long getLong(long address);

  void putLong(long address, long value);

  void setMemory(long address, long bytes, byte value);

  void copyMemory(long srcAddress, long destAddress, long bytes);

  void copyToMemory(byte[] src, long srcIndex, long destAddress, long bytes);

  void copyFromMemory(long srcAddress, byte[] dest, long destIndex, long bytes);

  int getInt(byte[] bytes, int index);

  long getLong(byte[] bytes, int index);

  long getByteBufferAddress(ByteBuffer buf);

  ByteBuffer directBuffer(long address, int capacity);
}
