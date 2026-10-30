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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for {@link ArrowBuf}'s direct-{@link ByteBuffer} paths under the FFM accessor.
 *
 * <p>{@link ArrowBuf#getBytes(long, ByteBuffer)} and {@link ArrowBuf#setBytes(long, ByteBuffer)}
 * add {@code position()} (or an explicit index) on top of what {@link
 * MemoryUtil#getByteBufferAddress} returns, so that address must be position-independent. An
 * earlier version of {@link FfmMemoryAccessor#getByteBufferAddress} returned {@code
 * MemorySegment.ofBuffer(buf).address()}, which spans {@code [position, limit)} and therefore
 * double-applied the offset: reads and writes silently landed at the wrong native address for any
 * buffer with a non-zero position. Every buffer here is direct and positioned past 0 on purpose.
 *
 * <p>Runs in a Surefire execution with {@code arrow.memory.accessor.type=FFM} (see
 * memory-ffm/pom.xml), since {@link ArrowBuf} reaches the accessor through {@link MemoryUtil},
 * whose choice of accessor is fixed at class-initialization time.
 */
public class TestFfmArrowBufByteBuffer {

  private static BufferAllocator createFfmAllocator() {
    return new RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(FfmAllocationManager.FACTORY)
            .build());
  }

  private static ByteBuffer directBufferOf(byte... values) {
    ByteBuffer buf = ByteBuffer.allocateDirect(values.length);
    buf.put(values);
    buf.clear();
    return buf;
  }

  @Test
  public void accessorIsFfm() {
    // Guards against this test silently exercising the Unsafe accessor instead.
    assertEquals(
        "FFM", System.getProperty(MemoryUtil.MEMORY_ACCESSOR_TYPE_PROPERTY_NAME, "Unsafe"));
  }

  @Test
  public void setBytesFromPositionedDirectBuffer() {
    ByteBuffer src = directBufferOf((byte) 10, (byte) 11, (byte) 12, (byte) 13, (byte) 14);
    src.position(2);

    try (BufferAllocator allocator = createFfmAllocator();
        ArrowBuf buffer = allocator.buffer(8)) {
      buffer.setZero(0, 8);
      buffer.setBytes(0, src);

      assertEquals((byte) 12, buffer.getByte(0));
      assertEquals((byte) 13, buffer.getByte(1));
      assertEquals((byte) 14, buffer.getByte(2));
      assertEquals((byte) 0, buffer.getByte(3));
      assertEquals(5, src.position());
    }
  }

  @Test
  public void setBytesFromPositionedDirectBufferWithSrcIndex() {
    ByteBuffer src = directBufferOf((byte) 20, (byte) 21, (byte) 22, (byte) 23, (byte) 24);
    src.position(4);

    try (BufferAllocator allocator = createFfmAllocator();
        ArrowBuf buffer = allocator.buffer(8)) {
      buffer.setZero(0, 8);
      // srcIndex is absolute, so position() must not shift the source address.
      buffer.setBytes(0, src, 1, 2);

      assertEquals((byte) 21, buffer.getByte(0));
      assertEquals((byte) 22, buffer.getByte(1));
      assertEquals((byte) 0, buffer.getByte(2));
    }
  }

  @Test
  public void getBytesIntoPositionedDirectBuffer() {
    ByteBuffer dst = ByteBuffer.allocateDirect(8);
    dst.position(5);

    try (BufferAllocator allocator = createFfmAllocator();
        ArrowBuf buffer = allocator.buffer(8)) {
      for (int i = 0; i < 8; i++) {
        buffer.setByte(i, (byte) (30 + i));
      }
      buffer.getBytes(0, dst);

      assertEquals(8, dst.position());
      assertEquals((byte) 30, dst.get(5));
      assertEquals((byte) 31, dst.get(6));
      assertEquals((byte) 32, dst.get(7));
      // Bytes before the destination position must be untouched.
      for (int i = 0; i < 5; i++) {
        assertEquals((byte) 0, dst.get(i));
      }
    }
  }

  @Test
  public void roundTripThroughPositionedDirectBuffers() {
    ByteBuffer src = directBufferOf((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6);
    src.position(3);
    ByteBuffer dst = ByteBuffer.allocateDirect(6);
    dst.position(3);

    try (BufferAllocator allocator = createFfmAllocator();
        ArrowBuf buffer = allocator.buffer(8)) {
      buffer.setBytes(0, src);
      buffer.getBytes(0, dst);

      assertEquals((byte) 4, dst.get(3));
      assertEquals((byte) 5, dst.get(4));
      assertEquals((byte) 6, dst.get(5));
    }
  }
}
