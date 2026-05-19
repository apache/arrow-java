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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Tests for {@link TimestampAvaticaParameterConverter}. */
public class TimestampAvaticaParameterConverterTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  // 2024-11-03 12:45:09.869 UTC — fractional seconds exercise the SECOND truncation path
  private static final long TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS = 1730637909869L;

  @Test
  public void testSecVector() {
    assertBindConvertsMillis(
        TimeUnit.SECOND, null, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS / 1_000L);
  }

  @Test
  public void testMilliVector() {
    assertBindConvertsMillis(TimeUnit.MILLISECOND, null, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
  }

  @Test
  public void testMicroVector() {
    assertBindConvertsMillis(
        TimeUnit.MICROSECOND, null, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS * 1_000L);
  }

  @Test
  public void testNanoVector() {
    assertBindConvertsMillis(
        TimeUnit.NANOSECOND, null, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS * 1_000_000L);
  }

  @Test
  public void testSecTZVector() {
    assertBindConvertsMillis(
        TimeUnit.SECOND, "UTC", TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS / 1_000L);
  }

  @Test
  public void testMicroTZVector() {
    assertBindConvertsMillis(
        TimeUnit.MICROSECOND, "UTC", TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS * 1_000L);
  }

  @Test
  public void testNanoTZVector() {
    assertBindConvertsMillis(
        TimeUnit.NANOSECOND, "UTC", TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS * 1_000_000L);
  }

  @Test
  public void testMicroVectorPreservesSubMillisecondPrecision() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    // Issue #838 exact values: 2024-11-03 12:45:09.869885001
    Timestamp ts = new Timestamp(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
    ts.setNanos(869885001); // .869885001 seconds — sub-ms precision

    try (TimeStampMicroVector vector = new TimeStampMicroVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(
              ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
      assertTrue(converter.bindParameter(vector, typedValue, 0, ts));
      // epochSeconds=1730637909, nanos=869885001 → micros = 1730637909 * 1_000_000 + 869885
      assertEquals(1730637909869885L, vector.get(0));
    }
  }

  @Test
  public void testNanoVectorPreservesFullNanosecondPrecision() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    Timestamp ts = new Timestamp(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
    ts.setNanos(869885001);

    try (TimeStampNanoVector vector = new TimeStampNanoVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(
              ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
      assertTrue(converter.bindParameter(vector, typedValue, 0, ts));
      // epochSeconds=1730637909, nanos=869885001 → nanos = 1730637909 * 1_000_000_000 + 869885001
      assertEquals(1730637909869885001L, vector.get(0));
    }
  }

  @Test
  public void testMilliVectorFromRawTimestampTruncatesSubMillisecond() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    Timestamp ts = new Timestamp(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
    ts.setNanos(869885001); // sub-ms nanos are truncated for MILLISECOND target

    try (TimeStampMilliVector vector = new TimeStampMilliVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(
              ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
      assertTrue(converter.bindParameter(vector, typedValue, 0, ts));
      assertEquals(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS, vector.get(0));
    }
  }

  @Test
  public void testSecVectorFromRawTimestampTruncatesSubSecond() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.SECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    Timestamp ts = new Timestamp(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
    ts.setNanos(869885001);

    try (TimeStampSecVector vector = new TimeStampSecVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(
              ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
      assertTrue(converter.bindParameter(vector, typedValue, 0, ts));
      assertEquals(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS / 1_000L, vector.get(0));
    }
  }

  @Test
  public void testFallbackToMillisWhenRawTimestampIsNull() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampMicroVector vector = new TimeStampMicroVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(
              ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
      // null rawTimestamp → falls back to convertFromMillis
      assertTrue(converter.bindParameter(vector, typedValue, 0, null));
      assertEquals(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS * 1_000L, vector.get(0));
    }
  }

  @Test
  public void testSecVectorTruncatesSubSecond() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.SECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampSecVector vector = new TimeStampSecVector("ts", allocator)) {
      vector.allocateNew(1);
      // 1999 millis should truncate to 1 second, not round to 2
      TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, 1999L);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      assertEquals(1L, vector.get(0));
    }
  }

  @Test
  public void testSecVectorNegativeEpochFloorDivision() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.SECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampSecVector vector = new TimeStampSecVector("ts", allocator)) {
      vector.allocateNew(3);
      // -999 millis: regular division gives 0 (wrong), floorDiv gives -1 (correct)
      TypedValue tv1 = TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, -999L);
      assertTrue(converter.bindParameter(vector, tv1, 0));
      assertEquals(-1L, vector.get(0));

      // -1000 millis: exactly -1 second
      TypedValue tv2 = TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, -1000L);
      assertTrue(converter.bindParameter(vector, tv2, 1));
      assertEquals(-1L, vector.get(1));

      // -1001 millis: floorDiv gives -2 (correct), regular division gives -1 (wrong)
      TypedValue tv3 = TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, -1001L);
      assertTrue(converter.bindParameter(vector, tv3, 2));
      assertEquals(-2L, vector.get(2));
    }
  }

  @Test
  public void testNegativeEpochRawTimestampPreservesSubMillis() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    // 1969-12-31 23:59:59.000123456 UTC → epochMillis=-1000, nanos=123456
    // epochSeconds = floorDiv(-1000, 1000) = -1
    // micros = -1 * 1_000_000 + 123456/1000 = -1_000_000 + 123 = -999877
    Timestamp ts = new Timestamp(-1000L);
    ts.setNanos(123456);

    try (TimeStampMicroVector vector = new TimeStampMicroVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, -1000L);
      assertTrue(converter.bindParameter(vector, typedValue, 0, ts));
      assertEquals(-999877L, vector.get(0));
    }
  }

  @Test
  public void testStaleRawTimestampIgnoredWhenTypedValueIsNotTimestamp() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    // Simulate stale map: rawTimestamp is present but TypedValue was set via setLong()
    Timestamp staleTs = new Timestamp(TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
    staleTs.setNanos(869885001);

    // A different value set via setLong — TypedValue type will be PRIMITIVE_LONG, not
    // JAVA_SQL_TIMESTAMP
    long longValue = 1774261392L;

    try (TimeStampMicroVector vector = new TimeStampMicroVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.LONG, longValue);
      // Even though staleTs is provided, the converter should ignore it because
      // typedValue.type != JAVA_SQL_TIMESTAMP, and fall back to convertFromMillis
      assertTrue(converter.bindParameter(vector, typedValue, 0, staleTs));
      assertEquals(longValue * 1_000L, vector.get(0));
    }
  }

  private void assertBindConvertsMillis(TimeUnit unit, String tz, long expectedValue) {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(unit, tz);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (FieldVector vector = createTestTimestampVector(unit, tz, allocator)) {
      ((BaseFixedWidthVector) vector).allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(
              ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, TEST_EPOCH_MILLIS_WITH_FRACTIONAL_SECONDS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      long actual = vector.getDataBuffer().getLong(0);
      assertEquals(expectedValue, actual);
    }
  }

  private static FieldVector createTestTimestampVector(
      TimeUnit unit, String tz, BufferAllocator allocator) {
    boolean hasTz = tz != null;
    switch (unit) {
      case SECOND:
        return hasTz
            ? new TimeStampSecTZVector("ts", allocator, tz)
            : new TimeStampSecVector("ts", allocator);
      case MILLISECOND:
        return hasTz
            ? new TimeStampMilliTZVector("ts", allocator, tz)
            : new TimeStampMilliVector("ts", allocator);
      case MICROSECOND:
        return hasTz
            ? new TimeStampMicroTZVector("ts", allocator, tz)
            : new TimeStampMicroVector("ts", allocator);
      case NANOSECOND:
        return hasTz
            ? new TimeStampNanoTZVector("ts", allocator, tz)
            : new TimeStampNanoVector("ts", allocator);
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }
}
