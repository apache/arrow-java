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

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Native memory from the C library's {@code malloc} and {@code free}, called through the foreign
 * {@link Linker}.
 *
 * <p>An {@link java.lang.foreign.Arena} per buffer is avoided on purpose: closing a shared arena
 * handshakes with every JVM thread, which makes freeing orders of magnitude slower than {@code
 * sun.misc.Unsafe}, and its allocations are always zeroed.
 */
final class NativeMemory {

  private static final Linker LINKER = Linker.nativeLinker();
  private static final MethodHandle MALLOC =
      LINKER.downcallHandle(
          find("malloc"), FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
  private static final MethodHandle FREE =
      LINKER.downcallHandle(find("free"), FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private NativeMemory() {}

  private static MemorySegment find(String name) {
    return LINKER
        .defaultLookup()
        .find(name)
        .orElseThrow(() -> new IllegalStateException("C library function not found: " + name));
  }

  /**
   * Returns the address of {@code bytes} of uninitialized native memory.
   *
   * @throws IllegalArgumentException if {@code bytes} is negative, like {@code
   *     sun.misc.Unsafe#allocateMemory}
   * @throws OutOfMemoryError if {@code malloc} cannot allocate them, like {@code
   *     sun.misc.Unsafe#allocateMemory}
   */
  static long allocate(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("Negative allocation size: " + bytes);
    }
    long address;
    try {
      address = ((MemorySegment) MALLOC.invokeExact(bytes)).address();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
    // malloc(0) may legitimately return NULL
    if (address == 0 && bytes != 0) {
      throw new OutOfMemoryError("Unable to allocate " + bytes + " bytes of native memory");
    }
    return address;
  }

  /** Frees memory returned by {@link #allocate}, or by any other call to {@code malloc}. */
  static void free(long address) {
    try {
      FREE.invokeExact(MemorySegment.ofAddress(address));
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }
}
