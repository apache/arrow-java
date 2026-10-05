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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.util.MemoryUtilAccessor;

/**
 * {@link MemoryUtilAccessor} backed by {@code java.lang.foreign} ({@link MemorySegment}/{@link
 * Arena}). Does not use {@code sun.misc.Unsafe} or reflection into {@code java.nio} internals, so
 * it requires neither {@code --add-opens} nor {@code sun.misc.Unsafe} availability.
 *
 * <p><b>Required JVM flag.</b> This accessor calls the restricted method {@link
 * MemorySegment#reinterpret(long)}. That is a real, current requirement, not a caveat that this
 * module already handles for you: classpath (unnamed-module) consumers must pass {@code
 * --enable-native-access=ALL-UNNAMED} and module-path consumers must pass {@code
 * --enable-native-access=org.apache.arrow.memory.ffm} on the JVM command line. Without it the JVM
 * prints a warning on every restricted call today, and a future JDK that enables restricted-method
 * enforcement by default will turn that into a hard {@link IllegalCallerException}. The {@code
 * Enable-Native-Access: ALL-UNNAMED} entry in this module's jar manifest does <em>not</em> cover
 * this: the JVM only honours that attribute in the manifest of the jar it was launched with via
 * {@code java -jar}, never for a jar that is merely a classpath dependency.
 *
 * <p>{@link #allocateMemory}/{@link #freeMemory} are provided for standalone callers of {@link
 * org.apache.arrow.memory.util.MemoryUtil#allocateMemory}/{@code #freeMemory}; each call gets its
 * own {@link Arena}, tracked by address so {@link #freeMemory} can close the right one.
 * Allocation-manager-owned memory instead goes through {@link FfmAllocationManager}, which holds
 * its {@link Arena} directly rather than round-tripping through this map.
 *
 * <p><b>Note.</b> Because freeing is arena-based rather than address-based, {@link #freeMemory} can
 * only release addresses that came from this accessor's own {@link #allocateMemory}. An address
 * from any other source (JNI, a foreign {@code malloc}, another accessor) is a silent no-op. This
 * differs from the {@code sun.misc.Unsafe}-backed accessor, which frees any valid native address
 * unconditionally.
 */
public final class FfmMemoryAccessor implements MemoryUtilAccessor {

  public static final MemoryUtilAccessor INSTANCE = new FfmMemoryAccessor();

  private static final ConcurrentHashMap<Long, Arena> STANDALONE_ARENAS = new ConcurrentHashMap<>();

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

  /**
   * Allocates {@code bytes} of native memory in a dedicated shared {@link Arena}, kept alive until
   * {@link #freeMemory} is called with the returned address.
   */
  @Override
  public long allocateMemory(long bytes) {
    Arena arena = Arena.ofShared();
    long address = arena.allocate(bytes).address();
    STANDALONE_ARENAS.put(address, arena);
    return address;
  }

  /**
   * Frees memory previously returned by {@link #allocateMemory}.
   *
   * @implNote Only addresses obtained from this accessor's {@link #allocateMemory} are actually
   *     freed; the address is looked up in a map of owning arenas. An address from any other source
   *     (JNI, a foreign {@code malloc}, another accessor) is not tracked here and the call is a
   *     silent no-op. The {@code sun.misc.Unsafe}-backed accessor instead frees any valid native
   *     address unconditionally.
   */
  @Override
  public void freeMemory(long address) {
    Arena arena = STANDALONE_ARENAS.remove(address);
    if (arena != null) {
      arena.close();
    }
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
