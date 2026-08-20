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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimestampAvaticaParameterConverterTest {
  private BufferAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void testBindIsoStringToMilliVector() {
    TimeStampMilliVector vector = new TimeStampMilliVector("ts", allocator);
    vector.allocateNew();
    TimestampAvaticaParameterConverter converter =
        new TimestampAvaticaParameterConverter(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    String isoString = "2025-08-14T15:53:00.000Z";
    TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.STRING.toString(), isoString);
    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertEquals(Instant.parse(isoString).toEpochMilli(), vector.get(0));
    vector.close();
  }

  @Test
  void testBindLongToMilliVector() {
    TimeStampMilliVector vector = new TimeStampMilliVector("ts", allocator);
    vector.allocateNew();
    TimestampAvaticaParameterConverter converter =
        new TimestampAvaticaParameterConverter(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    long millis = 1755253980000L;
    TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.LONG.toString(), millis);
    assertTrue(converter.bindParameter(vector, typedValue, 0));
    assertEquals(millis, vector.get(0));
    vector.close();
  }

  @Test
  void testUnsupportedValueType() {
    TimeStampMilliVector vector = new TimeStampMilliVector("ts", allocator);
    vector.allocateNew();
    TimestampAvaticaParameterConverter converter =
        new TimestampAvaticaParameterConverter(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
    TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.DOUBLE.toString(), 3.14);
    assertFalse(converter.bindParameter(vector, typedValue, 0));
    vector.close();
  }
}
