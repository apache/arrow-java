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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.math.BigDecimal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGenerateSampleData {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }

  // Sample values derive from the vector's scale (not hardcoded), so they fit any precision/scale,
  // including Decimal32's precision-9 limit that the scale-10 DecimalVector values would exceed.

  @Test
  public void testDecimal32() {
    try (Decimal32Vector vector =
        new Decimal32Vector(
            "decimal32", FieldType.nullable(new ArrowType.Decimal(9, 2, 32)), allocator)) {
      GenerateSampleData.generateTestData(vector, 10);
      assertEquals(10, vector.getValueCount());
      assertEquals(new BigDecimal("0.01"), vector.getObject(0));
      assertEquals(new BigDecimal("0.02"), vector.getObject(1));
      assertFalse(vector.isNull(9));
    }
  }

  @Test
  public void testDecimal64() {
    try (Decimal64Vector vector =
        new Decimal64Vector(
            "decimal64", FieldType.nullable(new ArrowType.Decimal(18, 4, 64)), allocator)) {
      GenerateSampleData.generateTestData(vector, 10);
      assertEquals(10, vector.getValueCount());
      assertEquals(new BigDecimal("0.0001"), vector.getObject(0));
      assertEquals(new BigDecimal("0.0002"), vector.getObject(1));
      assertFalse(vector.isNull(9));
    }
  }

  @Test
  public void testDecimal256() {
    try (Decimal256Vector vector =
        new Decimal256Vector(
            "decimal256", FieldType.nullable(new ArrowType.Decimal(40, 6, 256)), allocator)) {
      GenerateSampleData.generateTestData(vector, 10);
      assertEquals(10, vector.getValueCount());
      assertEquals(new BigDecimal("0.000001"), vector.getObject(0));
      assertEquals(new BigDecimal("0.000002"), vector.getObject(1));
      assertFalse(vector.isNull(9));
    }
  }
}
