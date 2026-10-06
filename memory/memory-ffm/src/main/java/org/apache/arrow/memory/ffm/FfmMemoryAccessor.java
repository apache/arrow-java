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
package org.apache.arrow.memory.ffm;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.util.MemoryUtilAccessor;

/**
 * {@link MemoryUtilAccessor} backed by {@code java.lang.foreign} ({@link MemorySegment} and the
 * foreign {@link java.lang.foreign.Linker}). Does not use {@code sun.misc.Unsafe} or reflection
 * into {@code java.nio} internals, so it requires neither {@code --add-opens} nor {@code
 * sun.misc.Unsafe} availability.
 *
 * <p><b>Required JVM flag.</b> This accessor calls the restricted methods {@link
 * MemorySegment#reinterpret(long)} and {@link java.lang.foreign.Linker#downcallHandle}, the latter
 * for {@code malloc} and {@code free}. That is a real, current requirement, not a caveat that this
 * module already handles for you: classpath (unnamed-module) consumers must pass {@code
 * --enable-native-access=ALL-UNNAMED} and module-path consumers must pass {@code
 * --enable-native-access=org.apache.arrow.memory.ffm} on the JVM command line. Without it the JVM
 * prints a warning on every restricted call today, and a future JDK that enables restricted-method
 * enforcement by default will turn that into a hard {@link IllegalCallerException}. The {@code
 * Enable-Native-Access: ALL-UNNAMED} entry in this module's jar manifest does <em>not</em> cover
 * this: the JVM only honours that attribute in the manifest of the jar it was launched with via
 * {@code java -jar}, never for a jar that is merely a classpath dependency.
 *
 * <p>{@link #allocateMemory}/{@link #freeMemory} call {@code malloc}/{@code free} directly, like
 * the {@code sun.misc.Unsafe}-backed accessor, so {@link #freeMemory} accepts any address returned
 * by {@code malloc}.
 */
public final class FfmMemoryAccessor implements MemoryUtilAccessor {

  public static final MemoryUtilAccessor INSTANCE = new FfmMemoryAccessor();

  private FfmMemoryAccessor() {}

  private static MemorySegment segment(long address, long byteSize) {
    return MemorySegment.ofAddress(address).reinterpret(byteSize);
  }

  private static int checkedInt(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("value out of int range: " + value);
    }
    return (int) value;
  }

  /** Allocates {@code bytes} of uninitialized native memory with the C library's {@code malloc}. */
  @Override
  public long allocateMemory(long bytes) {
    return NativeMemory.allocate(bytes);
  }

  /** Frees native memory with the C library's {@code free}. */
  @Override
  public void freeMemory(long address) {
    NativeMemory.free(address);
  }

  @Override
  public byte getByte(long address) {
    return segment(address, Byte.BYTES).get(ValueLayout.JAVA_BYTE, 0);
  }

  @Override
  public void putByte(long address, byte value) {
    segment(address, Byte.BYTES).set(ValueLayout.JAVA_BYTE, 0, value);
  }

  @Override
  public short getShort(long address) {
    return segment(address, Short.BYTES).get(ValueLayout.JAVA_SHORT_UNALIGNED, 0);
  }

  @Override
  public void putShort(long address, short value) {
    segment(address, Short.BYTES).set(ValueLayout.JAVA_SHORT_UNALIGNED, 0, value);
  }

  @Override
  public int getInt(long address) {
    return segment(address, Integer.BYTES).get(ValueLayout.JAVA_INT_UNALIGNED, 0);
  }

  @Override
  public void putInt(long address, int value) {
    segment(address, Integer.BYTES).set(ValueLayout.JAVA_INT_UNALIGNED, 0, value);
  }

  @Override
  public long getLong(long address) {
    return segment(address, Long.BYTES).get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
  }

  @Override
  public void putLong(long address, long value) {
    segment(address, Long.BYTES).set(ValueLayout.JAVA_LONG_UNALIGNED, 0, value);
  }

  @Override
  public void setMemory(long address, long bytes, byte value) {
    segment(address, bytes).fill(value);
  }

  @Override
  public void copyMemory(long srcAddress, long destAddress, long bytes) {
    MemorySegment.copy(segment(srcAddress, bytes), 0, segment(destAddress, bytes), 0, bytes);
  }

  @Override
  public void copyToMemory(byte[] src, long srcIndex, long destAddress, long bytes) {
    MemorySegment.copy(
        src,
        checkedInt(srcIndex),
        segment(destAddress, bytes),
        ValueLayout.JAVA_BYTE,
        0,
        checkedInt(bytes));
  }

  @Override
  public void copyFromMemory(long srcAddress, byte[] dest, long destIndex, long bytes) {
    MemorySegment.copy(
        segment(srcAddress, bytes),
        ValueLayout.JAVA_BYTE,
        0,
        dest,
        checkedInt(destIndex),
        checkedInt(bytes));
  }

  @Override
  public int getInt(byte[] bytes, int index) {
    return MemorySegment.ofArray(bytes).get(ValueLayout.JAVA_INT_UNALIGNED, index);
  }

  @Override
  public long getLong(byte[] bytes, int index) {
    return MemorySegment.ofArray(bytes).get(ValueLayout.JAVA_LONG_UNALIGNED, index);
  }

  /**
   * Returns the address of byte 0 of {@code buf}'s backing memory, independent of {@code buf}'s
   * current position.
   *
   * @implNote {@code MemorySegment.ofBuffer(buf)} only spans {@code [position, limit)}, so its
   *     address shifts with the position. Callers (notably {@link
   *     org.apache.arrow.memory.ArrowBuf}) add {@code position()} or an explicit index on top of
   *     the returned address themselves, matching the {@code sun.misc.Unsafe}-backed accessor,
   *     which reads the raw {@code java.nio.Buffer.address} field. Clearing a duplicate (rather
   *     than {@code buf} itself, whose state must not change) resets position to 0 and limit to
   *     capacity so the segment spans the whole backing buffer from byte 0.
   */
  @Override
  public long getByteBufferAddress(ByteBuffer buf) {
    return MemorySegment.ofBuffer(buf.duplicate().clear()).address();
  }

  @Override
  public ByteBuffer directBuffer(long address, int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException("Capacity is negative, has to be positive or 0");
    }
    return segment(address, capacity).asByteBuffer();
  }
}
