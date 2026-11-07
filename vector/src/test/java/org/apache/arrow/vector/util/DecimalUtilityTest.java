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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DecimalUtilityTest {
  @ParameterizedTest
  @ValueSource(ints = {4, 8, 16, 32})
  public void testWriteLongChecksBufferBounds(int byteWidth) {
    try (BufferAllocator allocator = new RootAllocator(128);
        ArrowBuf buffer = allocator.buffer(3L * byteWidth)) {
      for (long i = 0; i < buffer.capacity(); i++) {
        buffer.setByte(i, 100);
      }
      // The surrounding allocation keeps this regression test safe even if bounds checks fail.
      ArrowBuf value = buffer.slice(byteWidth, byteWidth);
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> DecimalUtility.writeLongToArrowBuf(1, value, -1, byteWidth));
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> DecimalUtility.writeLongToArrowBuf(1, value, 1, byteWidth));
      for (long i = 0; i < buffer.capacity(); i++) {
        assertEquals((byte) 100, buffer.getByte(i));
      }
    }
  }

  private static final BigInteger[] MAX_BIG_INT =
      new BigInteger[] {
        BigInteger.valueOf(10).pow(38).subtract(java.math.BigInteger.ONE),
        java.math.BigInteger.valueOf(10).pow(76)
      };
  private static final BigInteger[] MIN_BIG_INT =
      new BigInteger[] {
        MAX_BIG_INT[0].multiply(BigInteger.valueOf(-1)),
        MAX_BIG_INT[1].multiply(BigInteger.valueOf(-1))
      };

  @Test
  public void testSetLongInDecimalArrowBuf() {
    int[] byteLengths = new int[] {16, 32};
    for (int x = 0; x < 2; x++) {
      try (BufferAllocator allocator = new RootAllocator(128);
          ArrowBuf buf = allocator.buffer(byteLengths[x]); ) {
        int[] intValues = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        for (int val : intValues) {
          buf.clear();
          DecimalUtility.writeLongToArrowBuf((long) val, buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = BigDecimal.valueOf(val);
          assertEquals(expected, actual);
        }
      }
    }
  }

  @Test
  public void testSetByteArrayInDecimalArrowBuf() {
    int[] byteLengths = new int[] {16, 32};
    for (int x = 0; x < 2; x++) {
      try (BufferAllocator allocator = new RootAllocator(128);
          ArrowBuf buf = allocator.buffer(byteLengths[x]); ) {
        int[] intValues = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        for (int val : intValues) {
          buf.clear();
          DecimalUtility.writeByteArrayToArrowBuf(
              BigInteger.valueOf(val).toByteArray(), buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = BigDecimal.valueOf(val);
          assertEquals(expected, actual);
        }

        long[] longValues = new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE};
        for (long val : longValues) {
          buf.clear();
          DecimalUtility.writeByteArrayToArrowBuf(
              BigInteger.valueOf(val).toByteArray(), buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = BigDecimal.valueOf(val);
          assertEquals(expected, actual);
        }

        BigInteger[] decimals =
            new BigInteger[] {MAX_BIG_INT[x], new BigInteger("0"), MIN_BIG_INT[x]};
        for (BigInteger val : decimals) {
          buf.clear();
          DecimalUtility.writeByteArrayToArrowBuf(val.toByteArray(), buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = new BigDecimal(val);
          assertEquals(expected, actual);
        }
      }
    }
  }

  @Test
  public void testSetBigDecimalInDecimalArrowBuf() {
    int[] byteLengths = new int[] {16, 32};
    for (int x = 0; x < 2; x++) {
      try (BufferAllocator allocator = new RootAllocator(128);
          ArrowBuf buf = allocator.buffer(byteLengths[x]); ) {
        int[] intValues = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 0};
        for (int val : intValues) {
          buf.clear();
          DecimalUtility.writeBigDecimalToArrowBuf(BigDecimal.valueOf(val), buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = BigDecimal.valueOf(val);
          assertEquals(expected, actual);
        }

        long[] longValues = new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE};
        for (long val : longValues) {
          buf.clear();
          DecimalUtility.writeBigDecimalToArrowBuf(BigDecimal.valueOf(val), buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = BigDecimal.valueOf(val);
          assertEquals(expected, actual);
        }

        BigInteger[] decimals =
            new BigInteger[] {MAX_BIG_INT[x], new BigInteger("0"), MIN_BIG_INT[x]};
        for (BigInteger val : decimals) {
          buf.clear();
          DecimalUtility.writeBigDecimalToArrowBuf(new BigDecimal(val), buf, 0, byteLengths[x]);
          BigDecimal actual = DecimalUtility.getBigDecimalFromArrowBuf(buf, 0, 0, byteLengths[x]);
          BigDecimal expected = new BigDecimal(val);
          assertEquals(expected, actual);
        }
      }
    }
  }
}
