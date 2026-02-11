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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimeAvaticaParameterConverterTest {
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
  void testBindParameterWithIsoStringMilli() {
    TimeMilliVector vector = new TimeMilliVector("t", allocator);
    vector.allocateNew(1);
    TimeAvaticaParameterConverter converter =
        new TimeAvaticaParameterConverter(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    boolean result =
        converter.bindParameter(
            vector, TypedValue.create(ColumnMetaData.Rep.STRING.toString(), "21:39:50"), 0);
    assertTrue(result);
    assertEquals(77990000, vector.get(0));
    vector.close();
  }

  @Test
  void testBindParameterWithIsoStringMicro() {
    TimeMicroVector vector = new TimeMicroVector("t", allocator);
    vector.allocateNew(1);
    TimeAvaticaParameterConverter converter =
        new TimeAvaticaParameterConverter(new ArrowType.Time(TimeUnit.MICROSECOND, 32));
    boolean result =
        converter.bindParameter(
            vector, TypedValue.create(ColumnMetaData.Rep.STRING.toString(), "21:39:50.123456"), 0);
    assertTrue(result);
    assertEquals(77990123456L, (long) vector.get(0));
    vector.close();
  }

  @Test
  void testBindParameterWithIsoStringNano() {
    TimeNanoVector vector = new TimeNanoVector("t", allocator);
    vector.allocateNew(1);
    TimeAvaticaParameterConverter converter =
        new TimeAvaticaParameterConverter(new ArrowType.Time(TimeUnit.NANOSECOND, 64));
    boolean result =
        converter.bindParameter(
            vector,
            TypedValue.create(ColumnMetaData.Rep.STRING.toString(), "21:39:50.123456789"),
            0);
    assertTrue(result);
    assertEquals(77990123456789L, (long) vector.get(0));
    vector.close();
  }

  @Test
  void testBindParameterWithIsoStringSec() {
    TimeSecVector vector = new TimeSecVector("t", allocator);
    vector.allocateNew(1);
    TimeAvaticaParameterConverter converter =
        new TimeAvaticaParameterConverter(new ArrowType.Time(TimeUnit.SECOND, 32));
    boolean result =
        converter.bindParameter(
            vector, TypedValue.create(ColumnMetaData.Rep.STRING.toString(), "21:39:50"), 0);
    assertTrue(result);
    assertEquals(77990, vector.get(0));
    vector.close();
  }

  @Test
  void testBindParameterWithIntValueMilli() {
    TimeMilliVector vector = new TimeMilliVector("t", allocator);
    vector.allocateNew(1);
    TimeAvaticaParameterConverter converter =
        new TimeAvaticaParameterConverter(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
    boolean result =
        converter.bindParameter(
            vector, TypedValue.create(ColumnMetaData.Rep.INTEGER.toString(), 123456), 0);
    assertTrue(result);
    assertEquals(123456, vector.get(0));
    vector.close();
  }

  @Test
  void testBindParameterWithIntValueSec() {
    TimeSecVector vector = new TimeSecVector("t", allocator);
    vector.allocateNew(1);
    TimeAvaticaParameterConverter converter =
        new TimeAvaticaParameterConverter(new ArrowType.Time(TimeUnit.SECOND, 32));
    boolean result =
        converter.bindParameter(
            vector, TypedValue.create(ColumnMetaData.Rep.INTEGER.toString(), 42), 0);
    assertTrue(result);
    assertEquals(42, vector.get(0));
    vector.close();
  }
}
