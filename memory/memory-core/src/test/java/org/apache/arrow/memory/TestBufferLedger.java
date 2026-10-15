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
package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Tests for the reference counting in {@link BufferLedger}. */
public class TestBufferLedger {

  /**
   * Regression test for https://github.com/apache/arrow-java/issues/906: retain on a closed buffer
   * must not revive it.
   */
  @Test
  public void testRetainAfterCloseThrowsEveryTime() {
    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowBuf buf = allocator.buffer(10);
      buf.close();
      assertEquals(0, buf.refCnt());

      for (int i = 0; i < 3; i++) {
        assertThrows(IllegalArgumentException.class, () -> buf.getReferenceManager().retain());
        assertEquals(0, buf.refCnt());
        assertThrows(IllegalArgumentException.class, () -> buf.getReferenceManager().retain(5));
        assertEquals(0, buf.refCnt());
      }
    }
  }

  /**
   * After a failed retain the released memory stays released: it is neither usable nor freed again.
   */
  @Test
  public void testFailedRetainDoesNotReviveReleasedMemory() {
    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowBuf buf = allocator.buffer(10);
      buf.close();
      assertEquals(0, allocator.getAllocatedMemory());

      assertThrows(IllegalArgumentException.class, () -> buf.getReferenceManager().retain());
      assertThrows(IllegalArgumentException.class, () -> buf.getReferenceManager().retain());

      // still closed: no access to memory that may already belong to another buffer
      assertThrows(IllegalStateException.class, () -> buf.getByte(0));
      // and a stray release cannot free the chunk a second time
      assertThrows(IllegalStateException.class, () -> buf.getReferenceManager().release());
      assertEquals(0, allocator.getAllocatedMemory());

      // the allocator still hands out memory and accounts for it correctly
      try (ArrowBuf other = allocator.buffer(10)) {
        assertEquals(1, other.refCnt());
        assertEquals(other.capacity(), allocator.getAllocatedMemory());
      }
    }
  }

  /** Retaining a closed buffer into another allocator must not revive its memory. */
  @Test
  public void testRetainToAllocatorAfterCloseThrows() {
    try (BufferAllocator root = new RootAllocator();
        BufferAllocator child = root.newChildAllocator("child", 0, Long.MAX_VALUE)) {
      ArrowBuf buf = root.buffer(10);
      buf.close();

      assertThrows(IllegalStateException.class, () -> buf.getReferenceManager().retain(buf, child));
      assertThrows(IllegalStateException.class, () -> buf.getReferenceManager().retain(buf, root));
      assertEquals(0, child.getAllocatedMemory());
      assertEquals(0, root.getAllocatedMemory());
    }
  }

  /** Transferring a closed buffer to another allocator must not revive its memory. */
  @Test
  public void testTransferOwnershipAfterCloseThrows() {
    try (BufferAllocator root = new RootAllocator();
        BufferAllocator child = root.newChildAllocator("child", 0, Long.MAX_VALUE)) {
      ArrowBuf buf = root.buffer(10);
      buf.close();

      assertThrows(
          IllegalStateException.class,
          () -> buf.getReferenceManager().transferOwnership(buf, child));
      assertEquals(0, child.getAllocatedMemory());
      assertEquals(0, root.getAllocatedMemory());
    }
  }

  /** Retaining and transferring a live buffer into another allocator still work. */
  @Test
  public void testRetainAndTransferToAllocatorWhileReferenced() {
    try (BufferAllocator root = new RootAllocator();
        BufferAllocator child = root.newChildAllocator("child", 0, Long.MAX_VALUE)) {
      ArrowBuf buf = root.buffer(10);
      try (ArrowBuf retained = buf.getReferenceManager().retain(buf, child)) {
        assertEquals(1, retained.refCnt());
        ArrowBuf transferred =
            buf.getReferenceManager().transferOwnership(buf, child).getTransferredBuffer();
        assertEquals(2, transferred.refCnt());
        buf.close();
        transferred.setByte(0, 1);
        assertEquals(1, retained.getByte(0));
        transferred.close();
      }
      assertEquals(0, child.getAllocatedMemory());
      assertEquals(0, root.getAllocatedMemory());
    }
  }

  /**
   * Races retains against the final release of a buffer. Once a release has dropped the count to 0,
   * every retain that starts afterwards must fail, and the memory must be freed exactly once.
   */
  @Test
  public void testRetainRacingFinalReleaseNeverRevivesBuffer() throws Exception {
    final int racers = 4;
    final int rounds = 2_000;
    final int attemptsPerRacer = 64;
    final ExecutorService pool = Executors.newFixedThreadPool(racers);
    try (BufferAllocator allocator = new RootAllocator()) {
      for (int round = 0; round < rounds; round++) {
        final ArrowBuf buf = allocator.buffer(8);
        final ReferenceManager refs = buf.getReferenceManager();
        final CyclicBarrier start = new CyclicBarrier(racers + 1);
        final AtomicInteger frees = new AtomicInteger();
        final AtomicInteger revivals = new AtomicInteger();
        final RoundState state = new RoundState();

        List<Future<?>> futures = new ArrayList<>();
        for (int r = 0; r < racers; r++) {
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    for (int i = 0; i < attemptsPerRacer; i++) {
                      boolean freedBefore = state.freed;
                      try {
                        refs.retain();
                      } catch (IllegalArgumentException e) {
                        return null;
                      }
                      if (freedBefore) {
                        revivals.incrementAndGet();
                      }
                      if (refs.release()) {
                        frees.incrementAndGet();
                        state.freed = true;
                      }
                    }
                    return null;
                  }));
        }

        start.await();
        spin(ThreadLocalRandom.current().nextInt(200));
        if (refs.release()) {
          frees.incrementAndGet();
          state.freed = true;
        }
        for (Future<?> future : futures) {
          future.get(10, TimeUnit.SECONDS);
        }

        assertEquals(0, revivals.get(), "retain succeeded after the buffer was freed");
        assertEquals(1, frees.get(), "memory must be freed exactly once");
        assertEquals(0, buf.refCnt());
        assertEquals(0, allocator.getAllocatedMemory(), "allocator leaked memory");
      }
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static final class RoundState {
    volatile boolean freed;
  }

  private static void spin(int iterations) {
    for (int i = 0; i < iterations; i++) {
      Thread.onSpinWait();
    }
  }
}
