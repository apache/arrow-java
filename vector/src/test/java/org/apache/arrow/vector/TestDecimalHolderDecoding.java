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

import java.math.BigDecimal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.impl.Decimal256HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.DecimalHolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableDecimal256HolderReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableDecimalHolderReaderImpl;
import org.apache.arrow.vector.holders.Decimal256Holder;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.NullableDecimal256Holder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.junit.jupiter.api.Test;

public class TestDecimalHolderDecoding {
  @Test
  public void testDecimalHolderByteOrderAndOffset() {
    try (BufferAllocator allocator = new RootAllocator();
        DecimalVector vector = new DecimalVector("decimal", allocator, 38, 2)) {
      vector.allocateNew(3);
      vector.set(0, new BigDecimal("1.23"));
      BigDecimal[] values = {new BigDecimal("1234567.89"), new BigDecimal("-1234567.89")};
      for (int i = 0; i < values.length; i++) {
        vector.set(i + 1, values[i]);
        NullableDecimalHolder nullableHolder = new NullableDecimalHolder();
        vector.get(i + 1, nullableHolder);
        assertEquals(
            values[i], new NullableDecimalHolderReaderImpl(nullableHolder).readBigDecimal());
        DecimalHolder holder = new DecimalHolder();
        holder.buffer = nullableHolder.buffer;
        holder.start = nullableHolder.start;
        holder.precision = nullableHolder.precision;
        holder.scale = nullableHolder.scale;
        assertEquals(values[i], new DecimalHolderReaderImpl(holder).readBigDecimal());
      }
    }
  }

  @Test
  public void testDecimal256HolderByteOrderAndOffset() {
    try (BufferAllocator allocator = new RootAllocator();
        Decimal256Vector vector = new Decimal256Vector("decimal", allocator, 76, 2)) {
      vector.allocateNew(3);
      vector.set(0, new BigDecimal("1.23"));
      BigDecimal[] values = {new BigDecimal("1234567.89"), new BigDecimal("-1234567.89")};
      for (int i = 0; i < values.length; i++) {
        vector.set(i + 1, values[i]);
        NullableDecimal256Holder nullableHolder = new NullableDecimal256Holder();
        vector.get(i + 1, nullableHolder);
        assertEquals(
            values[i], new NullableDecimal256HolderReaderImpl(nullableHolder).readBigDecimal());
        Decimal256Holder holder = new Decimal256Holder();
        holder.buffer = nullableHolder.buffer;
        holder.start = nullableHolder.start;
        holder.precision = nullableHolder.precision;
        holder.scale = nullableHolder.scale;
        assertEquals(values[i], new Decimal256HolderReaderImpl(holder).readBigDecimal());
      }
    }
  }
}
