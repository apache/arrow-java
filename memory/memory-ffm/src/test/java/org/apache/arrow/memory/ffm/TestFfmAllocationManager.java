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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Test cases for {@link FfmAllocationManager}. */
public class TestFfmAllocationManager {

  private BufferAllocator createFfmAllocator() {
    return new RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(FfmAllocationManager.FACTORY)
            .build());
  }

  private void readWriteArrowBuf(ArrowBuf buffer) {
    for (long i = 0; i < buffer.capacity() / 8; i++) {
      buffer.setLong(i * 8, i);
    }
    for (long i = 0; i < buffer.capacity() / 8; i++) {
      assertEquals(i, buffer.getLong(i * 8));
    }
  }

  @Test
  public void testBufferAllocation() {
    final long bufSize = 4096L;
    try (BufferAllocator allocator = createFfmAllocator();
        ArrowBuf buffer = allocator.buffer(bufSize)) {
      assertInstanceOf(BufferLedger.class, buffer.getReferenceManager());
      BufferLedger bufferLedger = (BufferLedger) buffer.getReferenceManager();

      AllocationManager allocMgr = bufferLedger.getAllocationManager();
      assertInstanceOf(FfmAllocationManager.class, allocMgr);
      FfmAllocationManager ffmMgr = (FfmAllocationManager) allocMgr;

      assertEquals(bufSize, ffmMgr.getSize());
      readWriteArrowBuf(buffer);
    }
  }

  @Test
  public void testBufferIsFreedOnClose() {
    try (BufferAllocator allocator = createFfmAllocator()) {
      assertEquals(0, allocator.getAllocatedMemory());
      try (ArrowBuf buffer = allocator.buffer(1024)) {
        assertEquals(1024, allocator.getAllocatedMemory());
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
