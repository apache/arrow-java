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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;

/** AvaticaParameterConverter for Time Arrow types. */
public class TimeAvaticaParameterConverter extends BaseAvaticaParameterConverter {

  public TimeAvaticaParameterConverter(ArrowType.Time type) {}

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    Object value = typedValue.value;
    if (value instanceof String) {
      return bindTimeAsString(vector, (String) value, index);
    } else if (value instanceof Integer) {
      return bindTimeAsInt(vector, (int) value, index);
    } else {
      return false;
    }
  }

  private boolean bindTimeAsString(FieldVector vector, String value, int index) {
    java.time.LocalTime localTime = java.time.LocalTime.parse(value);
    long nanos = localTime.toNanoOfDay();
    if (vector instanceof TimeMilliVector) {
      int v = (int) (nanos / 1_000_000);
      ((TimeMilliVector) vector).setSafe(index, v);
      return true;
    } else if (vector instanceof TimeMicroVector) {
      long v = nanos / 1_000;
      ((TimeMicroVector) vector).setSafe(index, v);
      return true;
    } else if (vector instanceof TimeNanoVector) {
      long v = nanos;
      ((TimeNanoVector) vector).setSafe(index, v);
      return true;
    } else if (vector instanceof TimeSecVector) {
      int v = (int) (nanos / 1_000_000_000);
      ((TimeSecVector) vector).setSafe(index, v);
      return true;
    }
    return false;
  }

  private boolean bindTimeAsInt(FieldVector vector, int value, int index) {
    if (vector instanceof TimeMilliVector) {
      ((TimeMilliVector) vector).setSafe(index, value);
      return true;
    } else if (vector instanceof TimeMicroVector) {
      ((TimeMicroVector) vector).setSafe(index, (long) value);
      return true;
    } else if (vector instanceof TimeNanoVector) {
      ((TimeNanoVector) vector).setSafe(index, (long) value);
      return true;
    } else if (vector instanceof TimeSecVector) {
      ((TimeSecVector) vector).setSafe(index, value);
      return true;
    }
    return false;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    return createParameter(field, false);
  }
}
