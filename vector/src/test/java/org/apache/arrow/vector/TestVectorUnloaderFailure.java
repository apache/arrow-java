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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Exception-safety tests for record batch serialization. */
class TestVectorUnloaderFailure {

  @ParameterizedTest
  @ValueSource(longs = {0, 16})
  void compressionAllocationFailureReleasesBuffers(long availableBytes) {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector vector = new IntVector("values", allocator);
        VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
      vector.allocateNew(1);
      vector.set(0, 42);
      root.setRowCount(1);
      long allocatedBefore = allocator.getAllocatedMemory();
      int referencesBefore = vector.getDataBuffer().getReferenceManager().getRefCount();
      allocator.setLimit(allocatedBefore + availableBytes);
      VectorUnloader unloader = new VectorUnloader(root, true, new CopyCodec(), true);

      assertThrows(OutOfMemoryException.class, unloader::getRecordBatch);
      assertEquals(allocatedBefore, allocator.getAllocatedMemory());
      assertEquals(referencesBefore, vector.getDataBuffer().getReferenceManager().getRefCount());
      assertEquals(42, vector.get(0));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compressionFailurePreservesExceptionAndSource(boolean throwError) {
    Throwable failure =
        throwError
            ? new OutOfMemoryError("compression failed")
            : new IllegalStateException("compression failed");
    try (BufferAllocator allocator = new RootAllocator();
        IntVector vector = new IntVector("values", allocator);
        VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
      vector.allocateNew(1);
      vector.set(0, 42);
      root.setRowCount(1);
      long allocatedBefore = allocator.getAllocatedMemory();
      int referencesBefore = vector.getDataBuffer().getReferenceManager().getRefCount();
      CopyCodec codec =
          new CopyCodec() {
            private int calls;

            @Override
            protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf input) {
              if (++calls == 2) {
                if (failure instanceof Error) {
                  throw (Error) failure;
                }
                throw (RuntimeException) failure;
              }
              return super.doCompress(allocator, input);
            }
          };
      VectorUnloader unloader = new VectorUnloader(root, true, codec, true);

      assertSame(failure, assertThrows(failure.getClass(), unloader::getRecordBatch));
      assertEquals(allocatedBefore, allocator.getAllocatedMemory());
      assertEquals(referencesBefore, vector.getDataBuffer().getReferenceManager().getRefCount());
      assertEquals(42, vector.get(0));
    }
  }

  @Test
  void emptyBufferAllocationFailureReleasesInput() {
    try (BufferAllocator allocator = new RootAllocator()) {
      ArrowBuf input = allocator.buffer(8);
      allocator.setLimit(allocator.getAllocatedMemory());

      assertThrows(OutOfMemoryException.class, () -> new CopyCodec().compress(allocator, input));
      assertEquals(0, input.getReferenceManager().getRefCount());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void invalidLaterVectorReleasesPreviouslyRetainedBuffers() {
    try (BufferAllocator allocator = new RootAllocator();
        IntVector first = new IntVector("first", allocator)) {
      FieldVector invalid =
          new NullVector("invalid") {
            @Override
            public List<ArrowBuf> getFieldBuffers() {
              return Collections.singletonList(allocator.getEmpty());
            }
          };
      try (VectorSchemaRoot root = VectorSchemaRoot.of(first, invalid)) {
        first.allocateNew(1);
        first.set(0, 42);
        root.setRowCount(1);
        int referencesBefore = first.getDataBuffer().getReferenceManager().getRefCount();

        assertThrows(IllegalArgumentException.class, new VectorUnloader(root)::getRecordBatch);
        assertEquals(referencesBefore, first.getDataBuffer().getReferenceManager().getRefCount());
        assertEquals(42, first.get(0));
      }
    }
  }

  private static class CopyCodec extends AbstractCompressionCodec {
    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf input) {
      return CompressionUtil.packageRawBuffer(allocator, input);
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return CompressionUtil.CodecType.LZ4_FRAME;
    }
  }
}
