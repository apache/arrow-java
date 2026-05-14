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
package org.apache.arrow.vector.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVectorOps {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testShareCopyVector() {
    try (IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(4);
      source.set(0, 10);
      source.set(1, 20);
      source.setNull(2);
      source.set(3, 40);
      source.setValueCount(4);

      try (IntVector shared = VectorOps.shareCopy(source)) {
        assertEquals(4, shared.getValueCount());
        assertEquals(10, shared.get(0));
        assertEquals(20, shared.get(1));
        assertTrue(shared.isNull(2));
        assertEquals(40, shared.get(3));

        // Source is unmodified
        assertEquals(4, source.getValueCount());
        assertEquals(10, source.get(0));
      }
      // Source still usable after shared copy is closed
      assertEquals(10, source.get(0));
    }
  }

  @Test
  public void testShareCopyVectorWithAllocator() {
    try (BufferAllocator childAlloc = allocator.newChildAllocator("child", 0, Long.MAX_VALUE);
        IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(2);
      source.set(0, 100);
      source.set(1, 200);
      source.setValueCount(2);

      try (IntVector shared = VectorOps.shareCopy(source, childAlloc)) {
        assertEquals(childAlloc, shared.getAllocator());
        assertEquals(2, shared.getValueCount());
        assertEquals(100, shared.get(0));
        assertEquals(200, shared.get(1));
      }
    }
  }

  @Test
  public void testShareCopyVectorSchemaRoot() {
    try (IntVector intVec = new IntVector("ints", allocator);
        VarCharVector strVec = new VarCharVector("strings", allocator)) {
      intVec.allocateNew(3);
      intVec.set(0, 1);
      intVec.set(1, 2);
      intVec.set(2, 3);
      intVec.setValueCount(3);

      strVec.allocateNew(3);
      strVec.set(0, "hello".getBytes());
      strVec.set(1, "world".getBytes());
      strVec.set(2, "!".getBytes());
      strVec.setValueCount(3);

      VectorSchemaRoot source =
          new VectorSchemaRoot(Arrays.asList((FieldVector) intVec, (FieldVector) strVec));
      source.setRowCount(3);

      try (VectorSchemaRoot shared = VectorOps.shareCopy(source)) {
        assertEquals(3, shared.getRowCount());
        assertEquals(2, shared.getFieldVectors().size());

        IntVector sharedInts = (IntVector) shared.getVector("ints");
        assertEquals(1, sharedInts.get(0));
        assertEquals(2, sharedInts.get(1));
        assertEquals(3, sharedInts.get(2));

        VarCharVector sharedStrs = (VarCharVector) shared.getVector("strings");
        assertEquals("hello", new String(sharedStrs.get(0)));
        assertEquals("world", new String(sharedStrs.get(1)));
        assertEquals("!", new String(sharedStrs.get(2)));

        // Source still readable
        assertEquals(1, intVec.get(0));
      }
      source.close();
    }
  }

  @Test
  public void testTransferCopyVector() {
    try (IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(3);
      source.set(0, 100);
      source.set(1, 200);
      source.set(2, 300);
      source.setValueCount(3);

      try (IntVector transferred = VectorOps.transferCopy(source)) {
        assertEquals(3, transferred.getValueCount());
        assertEquals(100, transferred.get(0));
        assertEquals(200, transferred.get(1));
        assertEquals(300, transferred.get(2));

        // Source is emptied
        assertEquals(0, source.getValueCount());
      }
    }
  }

  @Test
  public void testTransferCopyVectorWithAllocator() {
    try (BufferAllocator childAlloc = allocator.newChildAllocator("child", 0, Long.MAX_VALUE);
        IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(2);
      source.set(0, 42);
      source.set(1, 99);
      source.setValueCount(2);

      try (IntVector transferred = VectorOps.transferCopy(source, childAlloc)) {
        assertEquals(childAlloc, transferred.getAllocator());
        assertEquals(2, transferred.getValueCount());
        assertEquals(42, transferred.get(0));
        assertEquals(99, transferred.get(1));
      }
    }
  }

  @Test
  public void testTransferCopyVectorSchemaRoot() {
    IntVector intVec = new IntVector("ints", allocator);
    intVec.allocateNew(2);
    intVec.set(0, 7);
    intVec.set(1, 8);
    intVec.setValueCount(2);

    VectorSchemaRoot source =
        new VectorSchemaRoot(Arrays.asList((FieldVector) intVec));
    source.setRowCount(2);

    try (VectorSchemaRoot transferred = VectorOps.transferCopy(source)) {
      assertEquals(2, transferred.getRowCount());
      IntVector transferredInts = (IntVector) transferred.getVector("ints");
      assertEquals(7, transferredInts.get(0));
      assertEquals(8, transferredInts.get(1));

      // Source vectors are emptied
      assertEquals(0, intVec.getValueCount());
    }
    source.close();
  }

  @Test
  public void testDeepCopyVector() {
    try (IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(3);
      source.set(0, 11);
      source.set(1, 22);
      source.set(2, 33);
      source.setValueCount(3);

      try (IntVector copied = VectorOps.deepCopy(source)) {
        assertEquals(3, copied.getValueCount());
        assertEquals(11, copied.get(0));
        assertEquals(22, copied.get(1));
        assertEquals(33, copied.get(2));

        // Source is unmodified
        assertEquals(3, source.getValueCount());
        assertEquals(11, source.get(0));
      }
      // Source still usable after copy is closed
      assertEquals(11, source.get(0));
    }
  }

  @Test
  public void testDeepCopyVectorIndependence() {
    try (IntVector source = new IntVector("ints", allocator);
        IntVector copied = VectorOps.deepCopy(source)) {
      source.allocateNew(2);
      source.set(0, 50);
      source.set(1, 60);
      source.setValueCount(2);

      // Modifying source after deep copy doesn't affect the copy
      // (copy was made when source was empty, so it should be empty)
      assertEquals(0, copied.getValueCount());
    }
  }

  @Test
  public void testDeepCopyVectorWithAllocator() {
    try (BufferAllocator childAlloc = allocator.newChildAllocator("child", 0, Long.MAX_VALUE);
        IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(2);
      source.set(0, 77);
      source.set(1, 88);
      source.setValueCount(2);

      try (IntVector copied = VectorOps.deepCopy(source, childAlloc)) {
        assertEquals(childAlloc, copied.getAllocator());
        assertEquals(2, copied.getValueCount());
        assertEquals(77, copied.get(0));
        assertEquals(88, copied.get(1));
      }
    }
  }

  @Test
  public void testDeepCopyVectorSchemaRoot() {
    try (IntVector intVec = new IntVector("ints", allocator);
        VarCharVector strVec = new VarCharVector("strings", allocator)) {
      intVec.allocateNew(2);
      intVec.set(0, 5);
      intVec.set(1, 6);
      intVec.setValueCount(2);

      strVec.allocateNew(2);
      strVec.set(0, "foo".getBytes());
      strVec.set(1, "bar".getBytes());
      strVec.setValueCount(2);

      VectorSchemaRoot source =
          new VectorSchemaRoot(Arrays.asList((FieldVector) intVec, (FieldVector) strVec));
      source.setRowCount(2);

      try (VectorSchemaRoot copied = VectorOps.deepCopy(source)) {
        assertEquals(2, copied.getRowCount());
        IntVector copiedInts = (IntVector) copied.getVector("ints");
        assertEquals(5, copiedInts.get(0));
        assertEquals(6, copiedInts.get(1));

        VarCharVector copiedStrs = (VarCharVector) copied.getVector("strings");
        assertEquals("foo", new String(copiedStrs.get(0)));
        assertEquals("bar", new String(copiedStrs.get(1)));

        // Source still intact
        assertEquals(5, intVec.get(0));
      }
      source.close();
    }
  }

  @Test
  public void testShareCopyVarCharVector() {
    try (VarCharVector source = new VarCharVector("strs", allocator)) {
      source.allocateNew(3);
      source.set(0, "alpha".getBytes());
      source.set(1, "beta".getBytes());
      source.set(2, "gamma".getBytes());
      source.setValueCount(3);

      try (VarCharVector shared = VectorOps.shareCopy(source)) {
        assertEquals(3, shared.getValueCount());
        assertEquals("alpha", new String(shared.get(0)));
        assertEquals("beta", new String(shared.get(1)));
        assertEquals("gamma", new String(shared.get(2)));
      }
      // Source still valid
      assertEquals("alpha", new String(source.get(0)));
    }
  }

  @Test
  public void testShareCopySourceClosedFirst() {
    IntVector shared;
    try (IntVector source = new IntVector("ints", allocator)) {
      source.allocateNew(2);
      source.set(0, 999);
      source.set(1, 888);
      source.setValueCount(2);

      shared = VectorOps.shareCopy(source);
    }
    // Source is now closed; shared copy should still be readable
    assertEquals(2, shared.getValueCount());
    assertEquals(999, shared.get(0));
    assertEquals(888, shared.get(1));
    shared.close();
  }
}
