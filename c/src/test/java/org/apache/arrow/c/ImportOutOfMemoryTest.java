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
package org.apache.arrow.c;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test: a mid-import {@link OutOfMemoryException} must not leak the imported array.
 *
 * <p>A "producer" allocator owns the exported batch; if the C Data release callback fires, the
 * producer drains to zero. A too-small consumer allocator forces an OOM part-way through the
 * import. The test asserts the producer drains, confirming the release callback fired despite
 * the failure.
 */
final class ImportOutOfMemoryTest {
  private static final int ROWS = 1024;
  private static final int VALUE_BYTES = 256;
  private static final int COLUMNS = 4;
  // Far smaller than the exported batch, so the import OOMs part-way through the buffers.
  private static final long TINY_LIMIT = 16 * 1024;

  private RootAllocator root;

  @BeforeEach
  public void setUp() {
    root = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    root.close();
  }

  @Test
  public void importOomDoesNotLeakExportedArray() {
    // "producer" owns only the exported batch buffers; the C Data struct containers live on a
    // separate allocator (they are consumed/closed by import, which would otherwise muddy the
    // producer's balance). So producer draining to zero is an exact signal that the array's release
    // callback fired.
    BufferAllocator producer = root.newChildAllocator("producer", 0, Long.MAX_VALUE);
    BufferAllocator structs = root.newChildAllocator("structs", 0, Long.MAX_VALUE);
    try (ArrowArray array = ArrowArray.allocateNew(structs);
        ArrowSchema schema = ArrowSchema.allocateNew(structs)) {
      exportBatch(producer, array, schema);
      assertTrue(
          producer.getAllocatedMemory() > 0, "producer holds the exported batch before import");

      // A consumer allocator far too small to hold the batch: the import throws part-way through.
      BufferAllocator consumer = root.newChildAllocator("consumer", 0, TINY_LIMIT);
      try (CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
        Schema importSchema = Data.importSchema(consumer, schema, provider);
        try (VectorSchemaRoot importRoot = VectorSchemaRoot.create(importSchema, consumer)) {
          Exception thrown =
              assertThrows(
                  Exception.class,
                  () -> Data.importIntoVectorSchemaRoot(consumer, array, importRoot, provider));
          assertTrue(
              hasOutOfMemoryCause(thrown),
              "mid-import failure must be an allocator OOM: " + thrown);
        }
      }
      consumer.close();

      // The array's release callback must have fired despite the mid-import OOM, freeing the whole
      // exported batch. On the unfixed retain-before-wrap code the batch is stranded instead.
      assertEquals(
          0L,
          producer.getAllocatedMemory(),
          "import OOM leaked the exported batch (producer not drained)");
    }
    // Closing these (and the RootAllocator in tearDown) additionally asserts nothing leaked at all.
    producer.close();
    structs.close();
  }

  /** True if {@code t} is, or is caused by, an Arrow {@link OutOfMemoryException}. */
  private static boolean hasOutOfMemoryCause(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof OutOfMemoryException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds a wide multi-column VarChar batch on {@code alloc} and exports it into the C structs.
   */
  private void exportBatch(BufferAllocator alloc, ArrowArray array, ArrowSchema schema) {
    byte[] value = new byte[VALUE_BYTES];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) 'x';
    }
    List<FieldVector> vectors = new ArrayList<>(COLUMNS);
    for (int c = 0; c < COLUMNS; c++) {
      VarCharVector vector = new VarCharVector("col" + c, alloc);
      vector.allocateNew((long) ROWS * VALUE_BYTES, ROWS);
      for (int r = 0; r < ROWS; r++) {
        vector.setSafe(r, value);
      }
      vector.setValueCount(ROWS);
      vectors.add(vector);
    }
    try (VectorSchemaRoot source = new VectorSchemaRoot(vectors)) {
      long total = 0;
      for (FieldVector vector : source.getFieldVectors()) {
        total += vector.getBufferSize();
      }
      assertTrue(total > TINY_LIMIT, "test setup: batch must exceed the consumer limit");
      Data.exportVectorSchemaRoot(alloc, source, null, array, schema);
    }
  }
}
