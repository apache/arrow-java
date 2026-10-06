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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.arrow.vector.Decimal64Vector;

/**
 * Consumer which consume decimal type values from {@link ResultSet}. Write the data to {@link
 * org.apache.arrow.vector.Decimal64Vector}.
 */
public abstract class Decimal64Consumer extends BaseConsumer<Decimal64Vector> {
  private final RoundingMode bigDecimalRoundingMode;
  private final int scale;

  /**
   * Constructs a new consumer.
   *
   * @param vector the underlying vector for the consumer.
   * @param index the column id for the consumer.
   */
  public Decimal64Consumer(Decimal64Vector vector, int index) {
    this(vector, index, null);
  }

  /**
   * Constructs a new consumer, with optional coercibility.
   *
   * @param vector the underlying vector for the consumer.
   * @param index the column index for the consumer.
   * @param bigDecimalRoundingMode java.math.RoundingMode to be applied if the BigDecimal scale does
   *     not match that of the target vector. Set to null to retain strict matching behavior (scale
   *     of source and target vector must match exactly).
   */
  public Decimal64Consumer(Decimal64Vector vector, int index, RoundingMode bigDecimalRoundingMode) {
    super(vector, index);
    this.bigDecimalRoundingMode = bigDecimalRoundingMode;
    this.scale = vector.getScale();
  }

  /** Creates a consumer for {@link Decimal64Vector}. */
  public static JdbcConsumer<Decimal64Vector> createConsumer(
      Decimal64Vector vector, int index, boolean nullable, RoundingMode bigDecimalRoundingMode) {
    if (nullable) {
      return new NullableDecimal64Consumer(vector, index, bigDecimalRoundingMode);
    } else {
      return new NonNullableDecimal64Consumer(vector, index, bigDecimalRoundingMode);
    }
  }

  protected void set(BigDecimal value) {
    if (bigDecimalRoundingMode != null && value.scale() != scale) {
      value = value.setScale(scale, bigDecimalRoundingMode);
    }
    vector.set(currentIndex, value);
  }

  /** Consumer for nullable decimal. */
  static class NullableDecimal64Consumer extends Decimal64Consumer {

    /** Instantiate a Decimal64Consumer. */
    public NullableDecimal64Consumer(
        Decimal64Vector vector, int index, RoundingMode bigDecimalRoundingMode) {
      super(vector, index, bigDecimalRoundingMode);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      BigDecimal value = resultSet.getBigDecimal(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        // for fixed width vectors, we have allocated enough memory proactively,
        // so there is no need to call the setSafe method here.
        set(value);
      }
      currentIndex++;
    }
  }

  /** Consumer for non-nullable decimal. */
  static class NonNullableDecimal64Consumer extends Decimal64Consumer {

    /** Instantiate a Decimal64Consumer. */
    public NonNullableDecimal64Consumer(
        Decimal64Vector vector, int index, RoundingMode bigDecimalRoundingMode) {
      super(vector, index, bigDecimalRoundingMode);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException {
      BigDecimal value = resultSet.getBigDecimal(columnIndexInResultSet);
      // for fixed width vectors, we have allocated enough memory proactively,
      // so there is no need to call the setSafe method here.
      set(value);
      currentIndex++;
    }
  }
}
