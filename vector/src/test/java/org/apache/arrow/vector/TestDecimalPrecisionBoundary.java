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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.validate.ValidateUtil.ValidateException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestDecimalPrecisionBoundary {
  @ParameterizedTest
  @ValueSource(ints = {128, 256})
  public void testValidateScalarsAtDeclaredPrecision(int bitWidth) {
    try (BufferAllocator allocator = new RootAllocator();
        BaseFixedWidthVector vector =
            bitWidth == 128
                ? new DecimalVector("decimal", allocator, 5, 2)
                : new Decimal256Vector("decimal", allocator, 5, 2)) {
      vector.allocateNew(2);
      DecimalUtility.writeBigDecimalToArrowBuf(
          new BigDecimal("999.99"), vector.getDataBuffer(), 0, bitWidth / 8);
      DecimalUtility.writeBigDecimalToArrowBuf(
          new BigDecimal("-999.99"), vector.getDataBuffer(), 1, bitWidth / 8);
      vector.setIndexDefined(0);
      vector.setIndexDefined(1);
      vector.setValueCount(2);
      vector.validateScalars();
      // Values beyond the declared precision must still be rejected.
      DecimalUtility.writeBigDecimalToArrowBuf(
          new BigDecimal("1000.00"), vector.getDataBuffer(), 0, bitWidth / 8);
      assertThrows(ValidateException.class, vector::validateScalars);
    }
  }
}
