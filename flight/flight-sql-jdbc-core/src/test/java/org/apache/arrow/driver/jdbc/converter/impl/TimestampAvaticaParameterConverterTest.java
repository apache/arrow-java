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

import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
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

  // A known epoch millis value: 2024-11-03 12:45:09.869 UTC
  private static final long EPOCH_MILLIS = 1730637909869L;

  @Test
  public void testBindParameterMilliVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampMilliVector vector = new TimeStampMilliVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      assertEquals(EPOCH_MILLIS, vector.get(0));
    }
  }

  @Test
  public void testBindParameterMicroVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampMicroVector vector = new TimeStampMicroVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      // Millis should be converted to micros
      assertEquals(EPOCH_MILLIS * 1_000L, vector.get(0));
    }
  }

  @Test
  public void testBindParameterNanoVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampNanoVector vector = new TimeStampNanoVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      // Millis should be converted to nanos
      assertEquals(EPOCH_MILLIS * 1_000_000L, vector.get(0));
    }
  }

  @Test
  public void testBindParameterSecVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.SECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampSecVector vector = new TimeStampSecVector("ts", allocator)) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      // Millis should be converted to seconds
      assertEquals(EPOCH_MILLIS / 1_000L, vector.get(0));
    }
  }

  @Test
  public void testBindParameterMicroTZVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampMicroTZVector vector = new TimeStampMicroTZVector("ts", allocator, "UTC")) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      assertEquals(EPOCH_MILLIS * 1_000L, vector.get(0));
    }
  }

  @Test
  public void testBindParameterNanoTZVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampNanoTZVector vector = new TimeStampNanoTZVector("ts", allocator, "UTC")) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      assertEquals(EPOCH_MILLIS * 1_000_000L, vector.get(0));
    }
  }

  @Test
  public void testBindParameterSecTZVector() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.SECOND, "UTC");
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampSecTZVector vector = new TimeStampSecTZVector("ts", allocator, "UTC")) {
      vector.allocateNew(1);
      TypedValue typedValue =
          TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, EPOCH_MILLIS);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      assertEquals(EPOCH_MILLIS / 1_000L, vector.get(0));
    }
  }

  @Test
  public void testBindParameterSecVectorTruncatesSubSecond() {
    BufferAllocator allocator = rootAllocatorTestExtension.getRootAllocator();
    ArrowType.Timestamp type = new ArrowType.Timestamp(TimeUnit.SECOND, null);
    TimestampAvaticaParameterConverter converter = new TimestampAvaticaParameterConverter(type);

    try (TimeStampSecVector vector = new TimeStampSecVector("ts", allocator)) {
      vector.allocateNew(1);
      // 1999 millis should truncate to 1 second, not round to 2
      long millis = 1999L;
      TypedValue typedValue = TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, millis);
      assertTrue(converter.bindParameter(vector, typedValue, 0));
      assertEquals(1L, vector.get(0));
    }
  }
}
