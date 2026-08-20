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

import java.time.LocalDate;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DateAvaticaParameterConverterTest {
  private RootAllocator allocator;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  void tearDown() {
    allocator.close();
  }

  @Test
  void testBindParameterWithStringDateMilliVector() {
    try (DateMilliVector vector = new DateMilliVector("date", allocator)) {
      DateAvaticaParameterConverter converter =
          new DateAvaticaParameterConverter(new ArrowType.Date(DateUnit.DAY));
      String dateStr = "2023-09-15";
      TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.STRING.toString(), dateStr);
      boolean result = converter.bindParameter(vector, typedValue, 0);
      assertTrue(result);
      assertEquals(
          (int) (LocalDate.parse(dateStr).toEpochDay() * 24 * 60 * 60 * 1000), vector.get(0));
    }
  }

  @Test
  void testBindParameterWithStringDateDayVector() {
    try (DateDayVector vector = new DateDayVector("date", allocator)) {
      DateAvaticaParameterConverter converter =
          new DateAvaticaParameterConverter(new ArrowType.Date(DateUnit.DAY));
      String dateStr = "2023-09-15";
      TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.STRING.toString(), dateStr);
      boolean result = converter.bindParameter(vector, typedValue, 0);
      assertTrue(result);
      assertEquals((int) LocalDate.parse(dateStr).toEpochDay(), vector.get(0));
    }
  }

  @Test
  void testBindParameterWithEpochDays() {
    try (DateDayVector vector = new DateDayVector("date", allocator)) {
      DateAvaticaParameterConverter converter =
          new DateAvaticaParameterConverter(new ArrowType.Date(DateUnit.DAY));
      int days = 19500;
      TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.INTEGER.toString(), days);
      boolean result = converter.bindParameter(vector, typedValue, 0);
      assertTrue(result);
      assertEquals(days, vector.get(0));
    }
  }

  @Test
  void testBindParameterWithUnsupportedVector() {
    try (IntVector vector = new IntVector("int", allocator)) {
      DateAvaticaParameterConverter converter =
          new DateAvaticaParameterConverter(new ArrowType.Date(DateUnit.DAY));
      TypedValue typedValue = TypedValue.create(ColumnMetaData.Rep.STRING.toString(), "2023-09-15");
      boolean result = converter.bindParameter(vector, typedValue, 0);
      assertFalse(result);
    }
  }
}
