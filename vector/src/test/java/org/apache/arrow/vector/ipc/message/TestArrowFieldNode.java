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
package org.apache.arrow.vector.ipc.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestArrowFieldNode {

  @Test
  public void testNegativeLengthRejected() {
    // A negative length that still fits in an int must be rejected. Left unchecked it flows into
    // BitVectorHelper.getValidityBufferSizeFromCount (unsigned shift -> ~512MB) and into
    // ArrowBuf.setOne(0, length / 8) with a negative length, corrupting memory when bounds
    // checking is disabled. See the deserialization paths in MessageSerializer and JsonFileReader.
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> new ArrowFieldNode(-100, 0));
    assertEquals("Expected length >= 0, got -100", e.getMessage());
  }

  @Test
  public void testLargeNegativeLengthRejected() {
    assertThrows(IllegalArgumentException.class, () -> new ArrowFieldNode(-5_000_000_000L, 0));
  }

  @Test
  public void testZeroAndPositiveLengthAccepted() {
    assertEquals(0, new ArrowFieldNode(0, 0).getLength());
    assertEquals(42, new ArrowFieldNode(42, 0).getLength());
  }

  @Test
  public void testNegativeNullCountSentinelPreserved() {
    // VectorUnloader uses nullCount == -1 to signal "null count not included"; it must survive.
    ArrowFieldNode node = new ArrowFieldNode(10, -1);
    assertEquals(10, node.getLength());
    assertEquals(-1, node.getNullCount());
  }
}
