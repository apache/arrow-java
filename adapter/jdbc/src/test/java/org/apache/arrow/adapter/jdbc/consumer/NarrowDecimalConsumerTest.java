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
package org.apache.arrow.adapter.jdbc.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adapter.jdbc.ResultSetUtility;
import org.apache.arrow.vector.Decimal32Vector;
import org.apache.arrow.vector.Decimal64Vector;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Decimal32Consumer} and {@link Decimal64Consumer}, the consumers for the narrow
 * (32-bit and 64-bit) Arrow decimal vectors, including dispatch through {@link
 * JdbcToArrowUtils#getConsumer}.
 */
public class NarrowDecimalConsumerTest extends AbstractConsumerTest {

  /** Builds a single-column {@link ResultSet} of decimals; the first value must be non-null. */
  private ResultSet decimalResultSet(List<BigDecimal> values) throws SQLException {
    ResultSetUtility.MockResultSet.Builder builder = ResultSetUtility.MockResultSet.builder();
    for (BigDecimal value : values) {
      builder.addDataElement(value, Types.DECIMAL).finishRow();
    }
    return builder.build();
  }

  @Test
  void decimal32Nullable() throws SQLException, IOException {
    List<BigDecimal> values =
        Arrays.asList(
            new BigDecimal("123.45"), null, new BigDecimal("-67.89"), new BigDecimal("0.00"));
    try (Decimal32Vector vector = new Decimal32Vector("decimal32", allocator, 9, 2)) {
      vector.allocateNew(values.size());
      JdbcConsumer<Decimal32Vector> consumer =
          Decimal32Consumer.createConsumer(vector, 1, /* nullable= */ true, null);
      ResultSet rs = decimalResultSet(values);
      for (int i = 0; i < values.size(); i++) {
        assertTrue(rs.next());
        consumer.consume(rs);
      }
      vector.setValueCount(values.size());

      for (int i = 0; i < values.size(); i++) {
        if (values.get(i) == null) {
          assertTrue(vector.isNull(i), "expected null at row " + i);
        } else {
          assertEquals(0, values.get(i).compareTo(vector.getObject(i)), "mismatch at row " + i);
        }
      }
    }
  }

  @Test
  void decimal32NonNullable() throws SQLException, IOException {
    List<BigDecimal> values =
        Arrays.asList(
            new BigDecimal("1.23"), new BigDecimal("-9999999.99"), new BigDecimal("0.00"));
    try (Decimal32Vector vector = new Decimal32Vector("decimal32", allocator, 9, 2)) {
      vector.allocateNew(values.size());
      JdbcConsumer<Decimal32Vector> consumer =
          Decimal32Consumer.createConsumer(vector, 1, /* nullable= */ false, null);
      ResultSet rs = decimalResultSet(values);
      for (int i = 0; i < values.size(); i++) {
        assertTrue(rs.next());
        consumer.consume(rs);
      }
      vector.setValueCount(values.size());

      for (int i = 0; i < values.size(); i++) {
        assertEquals(0, values.get(i).compareTo(vector.getObject(i)), "mismatch at row " + i);
      }
    }
  }

  @Test
  void decimal32RoundingMode() throws SQLException, IOException {
    // ResultSet supplies scale-4 values; the vector is scale-2, so the consumer must coerce.
    List<BigDecimal> input = Arrays.asList(new BigDecimal("1.2345"), new BigDecimal("6.7891"));
    try (Decimal32Vector vector = new Decimal32Vector("decimal32", allocator, 9, 2)) {
      vector.allocateNew(input.size());
      JdbcConsumer<Decimal32Vector> consumer =
          Decimal32Consumer.createConsumer(vector, 1, /* nullable= */ false, RoundingMode.HALF_UP);
      ResultSet rs = decimalResultSet(input);
      for (int i = 0; i < input.size(); i++) {
        assertTrue(rs.next());
        consumer.consume(rs);
      }
      vector.setValueCount(input.size());

      assertEquals(0, new BigDecimal("1.23").compareTo(vector.getObject(0)));
      assertEquals(0, new BigDecimal("6.79").compareTo(vector.getObject(1)));
    }
  }

  @Test
  void decimal64Nullable() throws SQLException, IOException {
    List<BigDecimal> values =
        Arrays.asList(
            new BigDecimal("123456789.0123"),
            null,
            new BigDecimal("-98765.4321"),
            new BigDecimal("0.0000"));
    try (Decimal64Vector vector = new Decimal64Vector("decimal64", allocator, 18, 4)) {
      vector.allocateNew(values.size());
      JdbcConsumer<Decimal64Vector> consumer =
          Decimal64Consumer.createConsumer(vector, 1, /* nullable= */ true, null);
      ResultSet rs = decimalResultSet(values);
      for (int i = 0; i < values.size(); i++) {
        assertTrue(rs.next());
        consumer.consume(rs);
      }
      vector.setValueCount(values.size());

      for (int i = 0; i < values.size(); i++) {
        if (values.get(i) == null) {
          assertTrue(vector.isNull(i), "expected null at row " + i);
        } else {
          assertEquals(0, values.get(i).compareTo(vector.getObject(i)), "mismatch at row " + i);
        }
      }
    }
  }

  @Test
  void decimal64RoundingMode() throws SQLException, IOException {
    // ResultSet supplies scale-6 values; the vector is scale-4, so the consumer must coerce.
    List<BigDecimal> input =
        Arrays.asList(new BigDecimal("12.345678"), new BigDecimal("-0.987654"));
    try (Decimal64Vector vector = new Decimal64Vector("decimal64", allocator, 18, 4)) {
      vector.allocateNew(input.size());
      JdbcConsumer<Decimal64Vector> consumer =
          Decimal64Consumer.createConsumer(vector, 1, /* nullable= */ false, RoundingMode.HALF_UP);
      ResultSet rs = decimalResultSet(input);
      for (int i = 0; i < input.size(); i++) {
        assertTrue(rs.next());
        consumer.consume(rs);
      }
      vector.setValueCount(input.size());

      assertEquals(0, new BigDecimal("12.3457").compareTo(vector.getObject(0)));
      assertEquals(0, new BigDecimal("-0.9877").compareTo(vector.getObject(1)));
    }
  }

  @Test
  void getConsumerDispatchesNarrowDecimals() {
    JdbcToArrowConfig config =
        new JdbcToArrowConfigBuilder(allocator, JdbcToArrowUtils.getUtcCalendar()).build();
    try (Decimal32Vector v32 = new Decimal32Vector("d32", allocator, 9, 2);
        Decimal64Vector v64 = new Decimal64Vector("d64", allocator, 18, 4)) {
      JdbcConsumer<?> c32 =
          JdbcToArrowUtils.getConsumer(
              v32.getField().getType(), 1, /* nullable= */ true, v32, config);
      JdbcConsumer<?> c64 =
          JdbcToArrowUtils.getConsumer(
              v64.getField().getType(), 1, /* nullable= */ true, v64, config);
      assertTrue(c32 instanceof Decimal32Consumer, "expected a Decimal32Consumer");
      assertTrue(c64 instanceof Decimal64Consumer, "expected a Decimal64Consumer");
    }
  }
}
