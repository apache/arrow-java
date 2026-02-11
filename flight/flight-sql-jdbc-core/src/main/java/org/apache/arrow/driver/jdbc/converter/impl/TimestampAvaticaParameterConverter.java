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

import java.time.Instant;
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

  public TimestampAvaticaParameterConverter(ArrowType.Timestamp type) {}

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    Object valueObj = typedValue.toLocal();
    if (valueObj instanceof String) {
      return bindTimestampAsString(vector, (String) valueObj, index);
    } else if (valueObj instanceof Long) {
      return bindTimestampAsLong(vector, (Long) valueObj, index);
    } else {
      return false;
    }
  }

  private boolean bindTimestampAsString(FieldVector vector, String value, int index) {
    Instant instant = Instant.parse(value);
    long result;
    if (vector instanceof TimeStampSecVector || vector instanceof TimeStampSecTZVector) {
      result = instant.getEpochSecond();
    } else if (vector instanceof TimeStampMilliVector || vector instanceof TimeStampMilliTZVector) {
      result = instant.toEpochMilli();
    } else if (vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector) {
      result = instant.toEpochMilli() * 1000;
    } else if (vector instanceof TimeStampNanoVector || vector instanceof TimeStampNanoTZVector) {
      result = instant.toEpochMilli() * 1000000;
    } else {
      return false;
    }
    return bindTimestampAsLong(vector, result, index);
  }

  private boolean bindTimestampAsLong(FieldVector vector, long value, int index) {
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
