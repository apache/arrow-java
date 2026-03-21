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

import java.sql.Timestamp;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/** AvaticaParameterConverter for Timestamp Arrow types. */
public class TimestampAvaticaParameterConverter extends BaseAvaticaParameterConverter {

  private final ArrowType.Timestamp type;

  public TimestampAvaticaParameterConverter(ArrowType.Timestamp type) {
    this.type = type;
  }

  /**
   * Converts a raw java.sql.Timestamp to the value in the target Arrow time unit, preserving
   * sub-millisecond precision from Timestamp.getNanos().
   */
  private long convertFromTimestamp(Timestamp ts) {
    // Timestamp.getTime() returns epoch millis (truncated, no sub-ms precision).
    // Timestamp.getNanos() returns the fractional-second component in nanoseconds (0..999_999_999).
    // We reconstruct the full-precision value from epoch seconds + nanos to avoid double-counting.
    long epochSeconds = Math.floorDiv(ts.getTime(), 1_000L);
    int nanos = ts.getNanos(); // 0..999_999_999, full fractional second
    switch (type.getUnit()) {
      case SECOND:
        return epochSeconds;
      case MILLISECOND:
        return epochSeconds * 1_000L + nanos / 1_000_000;
      case MICROSECOND:
        return epochSeconds * 1_000_000L + nanos / 1_000;
      case NANOSECOND:
        return epochSeconds * 1_000_000_000L + nanos;
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + type.getUnit());
    }
  }

  /** Converts an epoch millisecond value from Avatica to the target time unit. */
  private long convertFromMillis(long epochMillis) {
    switch (type.getUnit()) {
      case SECOND:
        return epochMillis / 1_000L;
      case MILLISECOND:
        return epochMillis;
      case MICROSECOND:
        return epochMillis * 1_000L;
      case NANOSECOND:
        return epochMillis * 1_000_000L;
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + type.getUnit());
    }
  }

  /**
   * Bind a timestamp parameter, using the raw java.sql.Timestamp if available for full precision.
   *
   * @param vector FieldVector to bind to.
   * @param typedValue TypedValue from Avatica (epoch millis, may have lost sub-ms precision).
   * @param index Vector index to bind the value at.
   * @param rawTimestamp Optional raw java.sql.Timestamp preserving sub-millisecond nanos.
   * @return true if binding was successful.
   */
  public boolean bindParameter(
      FieldVector vector, TypedValue typedValue, int index, @Nullable Timestamp rawTimestamp) {
    long value;
    if (rawTimestamp != null) {
      value = convertFromTimestamp(rawTimestamp);
    } else {
      value = convertFromMillis((long) typedValue.toLocal());
    }
    return setTimestampVector(vector, index, value);
  }

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    long value = convertFromMillis((long) typedValue.toLocal());
    return setTimestampVector(vector, index, value);
  }

  private boolean setTimestampVector(FieldVector vector, int index, long value) {
    if (vector instanceof TimeStampSecVector) {
      ((TimeStampSecVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampMicroVector) {
      ((TimeStampMicroVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampMilliVector) {
      ((TimeStampMilliVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampNanoVector) {
      ((TimeStampNanoVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampSecTZVector) {
      ((TimeStampSecTZVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampMicroTZVector) {
      ((TimeStampMicroTZVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampMilliTZVector) {
      ((TimeStampMilliTZVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeStampNanoTZVector) {
      ((TimeStampNanoTZVector) vector).setSafe(index, value);
      return true;
    }
    return false;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    return createParameter(field, false);
  }
}
