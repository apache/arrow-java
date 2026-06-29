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

/**
 * Utility class for validating list and list view reader positions.
 *
 * <p>A list vector contains an offset buffer that denotes lists boundaries. A list view vector
 * contains an offset buffer and a size buffer.
 */
final class UnionListReaderPositionValidator {

  private UnionListReaderPositionValidator() {}

  /** Check if the given index is within the current value count of the vector. */
  static void checkIndex(int index, int valueCount) {
    if (index < 0 || index >= valueCount) {
      throw new IndexOutOfBoundsException(
          String.format("index: %s, expected range [0, %s)", index, valueCount));
    }
  }

  /** Check that the list offset buffer contains entries for {@code index} and {@code index + 1}. */
  static void checkListBufferReadable(ArrowBuf offsetBuffer, int index, long offsetWidth) {
    long requiredBytes = ((long) index + 2L) * offsetWidth;
    long capacity = offsetBuffer.capacity();
    if (capacity < requiredBytes) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Offset buffer has capacity %s but reading index %s requires %s bytes",
              capacity, index, requiredBytes));
    }
  }

  /** Check that the list view offset and size buffers contain entries for {@code index}. */
  static void checkListViewBufferReadable(
      ArrowBuf offsetBuffer, ArrowBuf sizeBuffer, int index, long offsetWidth, long sizeWidth) {
    long requiredOffsetBytes = ((long) index + 1L) * offsetWidth;
    long offsetCapacity = offsetBuffer.capacity();
    if (offsetCapacity < requiredOffsetBytes) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Offset buffer has capacity %s but reading index %s requires %s bytes",
              offsetCapacity, index, requiredOffsetBytes));
    }

    long requiredSizeBytes = ((long) index + 1L) * sizeWidth;
    long sizeCapacity = sizeBuffer.capacity();
    if (sizeCapacity < requiredSizeBytes) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Size buffer has capacity %s but reading index %s requires %s bytes",
              sizeCapacity, index, requiredSizeBytes));
    }
  }
}
