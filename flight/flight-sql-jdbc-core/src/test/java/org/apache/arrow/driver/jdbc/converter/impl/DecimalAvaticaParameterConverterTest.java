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
package org.apache.arrow.driver.jdbc.converter.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link DecimalAvaticaParameterConverter}.
 *
 * <p>Clients call {@code setDouble} on parameters whose Arrow type is {@code Decimal}. The
 * converter must widen the Avatica reps to BigDecimal and scale them to the vector instead of
 * throwing.
 */
public class DecimalAvaticaParameterConverterTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private static final int PRECISION = 38;
  private static final int SCALE = 3;

  private DecimalVector vector;
  private DecimalAvaticaParameterConverter converter;

  @BeforeEach
  public void setUp() {
    vector =
        new DecimalVector(
            "decimal_param", rootAllocatorTestExtension.getRootAllocator(), PRECISION, SCALE);
    vector.allocateNew(5);
    converter = new DecimalAvaticaParameterConverter(new ArrowType.Decimal(PRECISION, SCALE, 128));
  }

  @AfterEach
  public void tearDown() {
    vector.close();
  }

  @Test
  public void testBindDouble() {
    // The reported shape: setDouble of 123.456 against a DECIMAL(38,3) parameter.
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, 123.456d);

    boolean result = converter.bindParameter(vector, typedValue, 0);

    assertTrue(result);
    assertThat(vector.getObject(0), is(new BigDecimal("123.456")));
  }

  @Test
  public void testBindDoubleKeepsTheDigitsTheClientSent() {
    // new BigDecimal(0.1) would store 0.1000000000000000055511151231257827 and fail the scale
    // check.
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, 0.1d);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("0.100")));
  }

  @Test
  public void testBindDoubleRoundsToTheVectorScale() {
    // 1.2345 has one digit more than DECIMAL(38,3) holds; HALF_UP is what setBigDecimal does.
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, 1.2345d);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("1.235")));
  }

  @Test
  public void testBindFloat() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.FLOAT, 2.5f);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("2.500")));
  }

  @Test
  public void testBindInteger() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.INTEGER, 42);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("42.000")));
  }

  @Test
  public void testBindLong() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.LONG, 1000L);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("1000.000")));
  }

  @Test
  public void testBindBigDecimalStillWorks() {
    TypedValue typedValue =
        TypedValue.ofLocal(ColumnMetaData.Rep.NUMBER, new BigDecimal("1000.000"));

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("1000.000")));
  }

  @Test
  public void testBindBigDecimalOfAnotherScaleIsScaled() {
    // setBigDecimal of a value the client scaled differently used to fail the vector's scale check.
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.NUMBER, new BigDecimal("7.5"));

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("7.500")));
  }

  @Test
  public void testBindNegativeDouble() {
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, -123.456d);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(new BigDecimal("-123.456")));
  }

  @Test
  public void testBindBigIntegerArrivesAsBigDecimal() {
    // Avatica has no rep for BigInteger, so toLocal() hands one over as a BigDecimal and the
    // value survives whole. Nothing reaches the longValue() branch, which would overflow here.
    final BigDecimal tooBigForALong = new BigDecimal("123456789012345678901234567890");
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.NUMBER, tooBigForALong);

    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertThat(vector.getObject(0), is(tooBigForALong.setScale(SCALE)));
  }

  @Test
  public void testBindNaNIsReportedAsUnsupported() {
    // A cast error is what the binder turns into "Binding value of type DOUBLE is not yet
    // supported"; NumberFormatException would escape it.
    TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, Double.NaN);

    assertThrows(ClassCastException.class, () -> converter.bindParameter(vector, typedValue, 0));
  }

  @Test
  public void testBindInfinityIsReportedAsUnsupported() {
    for (double value : new double[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
      TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, value);

      assertThrows(ClassCastException.class, () -> converter.bindParameter(vector, typedValue, 0));
    }
  }

  @Test
  public void testBindReturnsFalseForANonDecimalVector() {
    try (IntVector other = new IntVector("other", rootAllocatorTestExtension.getRootAllocator())) {
      other.allocateNew(1);
      TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.DOUBLE, 1.0d);

      // Returning false is what makes the binder report the type as unsupported.
      assertThat(converter.bindParameter(other, typedValue, 0), is(false));
    }
  }
}
