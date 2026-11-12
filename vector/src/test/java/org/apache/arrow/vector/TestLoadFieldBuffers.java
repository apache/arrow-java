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
package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Checks that a vector whose buffers fail to load can still be used and closed. */
public class TestLoadFieldBuffers {

  static Stream<Arguments> vectors() {
    return Stream.of(
        vector(a -> new IntVector("v", a), 2),
        vector(a -> new VarCharVector("v", a), 3),
        vector(a -> new LargeVarCharVector("v", a), 3),
        vector(a -> new ViewVarCharVector("v", a), 2),
        vector(a -> ListVector.empty("v", a), 2),
        vector(a -> LargeListVector.empty("v", a), 2),
        vector(a -> FixedSizeListVector.empty("v", 2, a), 1),
        vector(a -> ListViewVector.empty("v", a), 3),
        vector(a -> LargeListViewVector.empty("v", a), 3),
        vector(a -> StructVector.empty("v", a), 1));
  }

  private static Arguments vector(Function<BufferAllocator, FieldVector> create, int bufferCount) {
    return Arguments.of(create, bufferCount);
  }

  @ParameterizedTest
  @MethodSource("vectors")
  public void testFailedValidityLoadKeepsVector(
      Function<BufferAllocator, FieldVector> create, int bufferCount) {
    try (BufferAllocator allocator = new RootAllocator(64 * 1024)) {
      FieldVector vector = create.apply(allocator);
      vector.setInitialCapacity(16);
      vector.allocateNew();
      List<ArrowBuf> buffers = Collections.nCopies(bufferCount, allocator.getEmpty());

      // Without a validity buffer, 1,000,000 values need a 125 KB validity buffer, which is more
      // than the allocator allows
      assertThrows(
          OutOfMemoryException.class,
          () -> vector.loadFieldBuffers(new ArrowFieldNode(1_000_000, 0), buffers));
      // A missing validity buffer is rejected when only some values are null
      assertThrows(
          IllegalArgumentException.class,
          () -> vector.loadFieldBuffers(new ArrowFieldNode(16, 3), buffers));

      // The vector still owns its own buffers, so closing it releases each of them once
      vector.close();
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }
}
