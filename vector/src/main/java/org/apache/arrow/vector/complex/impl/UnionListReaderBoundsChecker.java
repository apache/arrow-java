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
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.memory.ArrowBuf;

/** Shared position validation for union list readers backed by offset buffers. */
final class UnionListReaderBoundsChecker {

  private UnionListReaderBoundsChecker() {}

  static boolean isEmptyVectorPosition(int index, int valueCount) {
    return valueCount == 0 && index == 0;
  }

  static void checkIndex(int index, int valueCount) {
    if (index < 0 || index >= valueCount) {
      throw new IndexOutOfBoundsException(
          String.format("index: %s, expected range (0, %s)", index, valueCount));
    }
  }

  static void checkOffsetBuffer(ArrowBuf offsetBuffer, int index, long offsetWidth) {
    long requiredBytes = ((long) index + 2L) * offsetWidth;
    long capacity = offsetBuffer.capacity();
    if (capacity < requiredBytes) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Offset buffer has capacity %s but reading index %s requires %s bytes",
              capacity, index, requiredBytes));
    }
  }
}
