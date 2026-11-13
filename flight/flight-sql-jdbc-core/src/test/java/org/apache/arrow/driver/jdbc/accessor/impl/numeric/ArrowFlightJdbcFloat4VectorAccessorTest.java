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
package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.Float4Vector;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ArrowFlightJdbcFloat4VectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private Float4Vector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcFloat4VectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) ->
              new ArrowFlightJdbcFloat4VectorAccessor(
                  (Float4Vector) vector, getCurrentRow, (boolean wasNull) -> {});

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcFloat4VectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  @BeforeEach
  public void setup() {
    this.vector = rootAllocatorTestExtension.createFloat4Vector();
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void testShouldGetFloatMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getFloat,
        (accessor, currentRow) -> is(vector.get(currentRow)));
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getObject,
        (accessor) -> is(accessor.getFloat()));
  }

  @Test
  public void testShouldGetStringMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getString,
        accessor -> is(Float.toString(accessor.getFloat())));
  }

  @Test
  public void testShouldGetStringMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(
          float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getString, CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetFloatMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(
          float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getFloat, is(0.0f));
    }
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(
          float4Vector,
          ArrowFlightJdbcFloat4VectorAccessor::getBigDecimal,
          CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(
          float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getObject, CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetBooleanMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getBoolean,
        accessor -> is(accessor.getFloat() != 0.0f));
  }

  @Test
  public void testShouldGetByteMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getByte,
        accessor -> is((byte) accessor.getFloat()));
  }

  @Test
  public void testShouldGetShortMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getShort,
        accessor -> is((short) accessor.getFloat()));
  }

  @Test
  public void testShouldGetIntMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getInt,
        accessor -> is((int) accessor.getFloat()));
  }

  @Test
  public void testShouldGetLongMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getLong,
        accessor -> is((long) accessor.getFloat()));
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getDouble,
        accessor -> is(Double.parseDouble(Float.toString(accessor.getFloat()))));
  }

  @Test
  public void testShouldGetDoubleThatRoundTripsToTheSameFloat() throws Exception {
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) ->
            assertThat((float) accessor.getDouble(), is(accessor.getFloat())));
  }

  @Test
  public void testShouldGetDecimalValuesWithoutFloatWideningArtifacts() throws Exception {
    final float[] values = {0.1f, -0.1f, 0.2f, 0.3f, 1.1f, 123.456f, 3.4e-5f, -0.0f};
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      for (int i = 0; i < values.length; i++) {
        float4Vector.setSafe(i, values[i]);
      }
      float4Vector.setValueCount(values.length);

      accessorIterator.iterate(
          float4Vector,
          (accessor, currentRow) -> {
            final String expected = Float.toString(values[currentRow]);
            assertThat(accessor.getDouble(), is(Double.parseDouble(expected)));
            assertThat(accessor.getObject(Double.class), is(Double.parseDouble(expected)));
            assertThat(accessor.getBigDecimal(), is(new BigDecimal(expected)));
            assertThat(
                accessor.getBigDecimal(9),
                is(new BigDecimal(expected).setScale(9, RoundingMode.HALF_UP)));
          });
    }
  }

  @Test
  public void testShouldGetDoubleFromFloat4VectorWithZeroPointOne() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setSafe(0, 0.1f);
      float4Vector.setValueCount(1);

      accessorIterator.iterate(
          float4Vector,
          (accessor, currentRow) -> {
            assertThat(accessor.getDouble(), is(0.1));
            assertThat(accessor.getBigDecimal(), is(new BigDecimal("0.1")));
            assertThat(accessor.getBigDecimal(9), is(new BigDecimal("0.100000000")));
          });
    }
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(
          float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getDouble, is(0.0));
    }
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat4VectorWithNaN() throws Exception {
    try (final Float4Vector float4Vector =
        new Float4Vector("ID", rootAllocatorTestExtension.getRootAllocator())) {
      float4Vector.setSafe(0, Float.NaN);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(
          float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getDouble, is(Double.NaN));
    }
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4Vector() throws Exception {
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          float value = accessor.getFloat();
          if (Float.isInfinite(value) || Float.isNaN(value)) {
            assertThrows(SQLException.class, accessor::getBigDecimal);
          } else {
            assertThat(accessor.getBigDecimal(), is(new BigDecimal(Float.toString(value))));
          }
        });
  }

  @Test
  public void testShouldGetBigDecimalWithScaleMethodFromFloat4Vector() throws Exception {
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          float value = accessor.getFloat();
          if (Float.isInfinite(value) || Float.isNaN(value)) {
            assertThrows(SQLException.class, () -> accessor.getBigDecimal(9));
          } else {
            assertThat(
                accessor.getBigDecimal(9),
                is(new BigDecimal(Float.toString(value)).setScale(9, RoundingMode.HALF_UP)));
          }
        });
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcFloat4VectorAccessor::getObjectClass,
        accessor -> equalTo(Float.class));
  }
}
