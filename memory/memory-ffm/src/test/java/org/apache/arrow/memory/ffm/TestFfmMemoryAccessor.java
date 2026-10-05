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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestFfmMemoryAccessor {

  private long address = 0;

  @AfterEach
  public void after() {
    if (address != 0) {
      FfmMemoryAccessor.INSTANCE.freeMemory(address);
      address = 0;
    }
  }

  @Test
  public void putAndGetPrimitives() {
    address = FfmMemoryAccessor.INSTANCE.allocateMemory(16);

    FfmMemoryAccessor.INSTANCE.putByte(address, (byte) 42);
    assertEquals((byte) 42, FfmMemoryAccessor.INSTANCE.getByte(address));

    FfmMemoryAccessor.INSTANCE.putShort(address + 2, (short) 1234);
    assertEquals((short) 1234, FfmMemoryAccessor.INSTANCE.getShort(address + 2));

    FfmMemoryAccessor.INSTANCE.putInt(address + 4, 123456789);
    assertEquals(123456789, FfmMemoryAccessor.INSTANCE.getInt(address + 4));

    FfmMemoryAccessor.INSTANCE.putLong(address + 8, 9876543210123L);
    assertEquals(9876543210123L, FfmMemoryAccessor.INSTANCE.getLong(address + 8));
  }

  @Test
  public void copyToAndFromMemoryRoundTrips() {
    address = FfmMemoryAccessor.INSTANCE.allocateMemory(8);
    byte[] src = {1, 2, 3, 4, 5, 6, 7, 8};
    FfmMemoryAccessor.INSTANCE.copyToMemory(src, 0, address, 8);

    byte[] dest = new byte[8];
    FfmMemoryAccessor.INSTANCE.copyFromMemory(address, dest, 0, 8);
    assertArrayEquals(src, dest);
  }

  @Test
  public void copyMemoryBetweenAddresses() {
    long src = FfmMemoryAccessor.INSTANCE.allocateMemory(8);
    long dst = FfmMemoryAccessor.INSTANCE.allocateMemory(8);
    try {
      FfmMemoryAccessor.INSTANCE.putLong(src, 555L);
      FfmMemoryAccessor.INSTANCE.copyMemory(src, dst, 8);
      assertEquals(555L, FfmMemoryAccessor.INSTANCE.getLong(dst));
    } finally {
      FfmMemoryAccessor.INSTANCE.freeMemory(dst);
      FfmMemoryAccessor.INSTANCE.freeMemory(src);
    }
  }

  @Test
  public void setMemoryFillsBytes() {
    address = FfmMemoryAccessor.INSTANCE.allocateMemory(4);
    FfmMemoryAccessor.INSTANCE.setMemory(address, 4, (byte) 0xAB);
    for (int i = 0; i < 4; i++) {
      assertEquals((byte) 0xAB, FfmMemoryAccessor.INSTANCE.getByte(address + i));
    }
  }

  @Test
  public void byteArrayIndexedAccessors() {
    byte[] bytes = new byte[Long.BYTES];
    ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()).putLong(0, 42L);
    assertEquals(42L, FfmMemoryAccessor.INSTANCE.getLong(bytes, 0));

    byte[] intBytes = new byte[Integer.BYTES * 2];
    ByteBuffer.wrap(intBytes).order(ByteOrder.nativeOrder()).putInt(0, 123456789);
    ByteBuffer.wrap(intBytes).order(ByteOrder.nativeOrder()).putInt(Integer.BYTES, -987654321);
    assertEquals(123456789, FfmMemoryAccessor.INSTANCE.getInt(intBytes, 0));
    assertEquals(-987654321, FfmMemoryAccessor.INSTANCE.getInt(intBytes, Integer.BYTES));
  }

  /**
   * {@link FfmMemoryAccessor#getByteBufferAddress} must return the address of byte 0 of the backing
   * memory regardless of the buffer's position, matching {@code UnsafeMemoryAccessor}: callers add
   * {@code position()} themselves. It must also not disturb the caller's buffer state.
   */
  @Test
  public void getByteBufferAddressIsPositionIndependent() {
    address = FfmMemoryAccessor.INSTANCE.allocateMemory(16);
    ByteBuffer buf = FfmMemoryAccessor.INSTANCE.directBuffer(address, 16);
    assertEquals(address, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(buf));

    buf.position(5);
    buf.limit(12);
    assertEquals(address, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(buf));
    assertEquals(5, buf.position());
    assertEquals(12, buf.limit());

    ByteBuffer slice = buf.slice();
    assertEquals(address + 5, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(slice));
    slice.position(3);
    assertEquals(address + 5, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(slice));

    ByteBuffer readOnly = buf.asReadOnlyBuffer();
    readOnly.position(7);
    assertEquals(address, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(readOnly));
  }

  @Test
  public void directBufferRejectsNegativeCapacity() {
    assertThrows(
        IllegalArgumentException.class, () -> FfmMemoryAccessor.INSTANCE.directBuffer(1, -1));
  }
}
