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
import java.nio.ByteOrder;

/** Utilities for memory related operations. */
public class MemoryUtil {

  /** If the native byte order is little-endian. */
  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private static final MemoryUtilAccessor ACCESSOR = UnsafeMemoryAccessor.INSTANCE;

  private MemoryUtil() {}

  /**
   * Given a {@link ByteBuffer}, gets the address the underlying memory space.
   *
   * @param buf the byte buffer.
   * @return address of the underlying memory.
   */
  public static long getByteBufferAddress(ByteBuffer buf) {
    return ACCESSOR.getByteBufferAddress(buf);
  }

  /** Create nio byte buffer. */
  public static ByteBuffer directBuffer(long address, int capacity) {
    return ACCESSOR.directBuffer(address, capacity);
  }

  public static void copyMemory(long srcAddress, long destAddress, long bytes) {
    ACCESSOR.copyMemory(srcAddress, destAddress, bytes);
  }

  public static void copyToMemory(byte[] src, long srcIndex, long destAddress, long bytes) {
    ACCESSOR.copyToMemory(src, srcIndex, destAddress, bytes);
  }

  public static void copyFromMemory(long srcAddress, byte[] dest, long destIndex, long bytes) {
    ACCESSOR.copyFromMemory(srcAddress, dest, destIndex, bytes);
  }

  public static byte getByte(long address) {
    return ACCESSOR.getByte(address);
  }

  public static void putByte(long address, byte value) {
    ACCESSOR.putByte(address, value);
  }

  public static short getShort(long address) {
    return ACCESSOR.getShort(address);
  }

  public static void putShort(long address, short value) {
    ACCESSOR.putShort(address, value);
  }

  public static int getInt(long address) {
    return ACCESSOR.getInt(address);
  }

  public static void putInt(long address, int value) {
    ACCESSOR.putInt(address, value);
  }

  public static long getLong(long address) {
    return ACCESSOR.getLong(address);
  }

  public static void putLong(long address, long value) {
    ACCESSOR.putLong(address, value);
  }

  public static void setMemory(long address, long bytes, byte value) {
    ACCESSOR.setMemory(address, bytes, value);
  }

  public static int getInt(byte[] bytes, int index) {
    return ACCESSOR.getInt(bytes, index);
  }

  public static long getLong(byte[] bytes, int index) {
    return ACCESSOR.getLong(bytes, index);
  }

  public static long allocateMemory(long bytes) {
    return ACCESSOR.allocateMemory(bytes);
  }

  public static void freeMemory(long address) {
    ACCESSOR.freeMemory(address);
  }
}
