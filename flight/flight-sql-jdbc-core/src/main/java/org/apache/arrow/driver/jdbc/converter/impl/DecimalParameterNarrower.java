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

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.calcite.avatica.remote.TypedValue;

/**
 * Widens Avatica {@link TypedValue}s bound to DECIMAL parameters to {@link BigDecimal}, so calls
 * like {@code setDouble} against an Arrow {@code Decimal} parameter bind instead of throwing.
 */
final class DecimalParameterNarrower {

  private DecimalParameterNarrower() {}

  /**
   * Returns the {@link BigDecimal} value of a {@link TypedValue} bound to a DECIMAL parameter,
   * scaled to what the target vector stores.
   *
   * <p>A value is scaled rather than rejected on a mismatch: the client sent the number it had, and
   * the column's scale is what it has to fit. Rounding is HALF_UP, as {@code setBigDecimal} does.
   */
  static BigDecimal toBigDecimal(TypedValue typedValue, int scale) {
    return widen(typedValue).setScale(scale, RoundingMode.HALF_UP);
  }

  /**
   * Converts the local value to a {@link BigDecimal}, keeping the digits the client sent.
   *
   * <p>{@code BigDecimal.valueOf} rather than {@code new BigDecimal(double)}: the constructor keeps
   * the binary expansion, so 0.1 arrives as 0.1000000000000000055511151231257827.
   */
  private static BigDecimal widen(TypedValue typedValue) {
    final Object local = typedValue.toLocal();
    if (local instanceof BigDecimal) {
      return (BigDecimal) local;
    }
    if (local instanceof Double || local instanceof Float) {
      return BigDecimal.valueOf(((Number) local).doubleValue());
    }
    if (local instanceof Number) {
      return BigDecimal.valueOf(((Number) local).longValue());
    }
    // Anything else keeps the upstream failure: a cast error the binder reports as unsupported.
    return (BigDecimal) local;
  }
}
