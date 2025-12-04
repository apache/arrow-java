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

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.UUID;
import org.apache.arrow.driver.jdbc.converter.AvaticaParameterConverter;
import org.apache.arrow.driver.jdbc.utils.SqlTypes;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.UuidUtility;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.ByteString;

/**
 * AvaticaParameterConverter for UUID Arrow extension type.
 *
 * <p>Handles conversion of UUID values from JDBC parameters to Arrow's UUID extension type. Accepts
 * both {@link UUID} objects and String representations of UUIDs.
 */
public class UuidAvaticaParameterConverter implements AvaticaParameterConverter {

  public UuidAvaticaParameterConverter() {}

  @Override
  public boolean bindParameter(FieldVector vector, TypedValue typedValue, int index) {
    if (!(vector instanceof UuidVector)) {
      return false;
    }

    UuidVector uuidVector = (UuidVector) vector;
    Object value = typedValue.toJdbc(null);

    if (value == null) {
      uuidVector.setNull(index);
      return true;
    }

    UUID uuid;
    if (value instanceof UUID) {
      uuid = (UUID) value;
    } else if (value instanceof String) {
      uuid = UUID.fromString((String) value);
    } else if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      if (bytes.length != 16) {
        throw new IllegalArgumentException("UUID byte array must be 16 bytes, got " + bytes.length);
      }
      uuid = uuidFromBytes(bytes);
    } else if (value instanceof ByteString) {
      byte[] bytes = ((ByteString) value).getBytes();
      if (bytes.length != 16) {
        throw new IllegalArgumentException("UUID byte array must be 16 bytes, got " + bytes.length);
      }
      uuid = uuidFromBytes(bytes);
    } else {
      throw new IllegalArgumentException(
          "Cannot convert " + value.getClass().getName() + " to UUID");
    }

    uuidVector.setSafe(index, UuidUtility.getBytesFromUUID(uuid));
    return true;
  }

  @Override
  public AvaticaParameter createParameter(Field field) {
    final String name = field.getName();
    final int jdbcType = Types.OTHER;
    final String typeName = SqlTypes.UUID_TYPE_NAME;
    final String className = UUID.class.getCanonicalName();
    return new AvaticaParameter(false, 0, 0, jdbcType, typeName, className, name);
  }

  private static UUID uuidFromBytes(byte[] bytes) {
//    long msb = 0;
//    long lsb = 0;
//    for (int i = 0; i < 8; i++) {
//      msb = (msb << 8) | (bytes[i] & 0xff);
//    }
//    for (int i = 8; i < 16; i++) {
//      lsb = (lsb << 8) | (bytes[i] & 0xff);
//    }

    final long mostSignificantBits;
    final long leastSignificantBits;
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    mostSignificantBits = bb.getLong();
    leastSignificantBits = bb.getLong();

    return new UUID(mostSignificantBits, leastSignificantBits);
  }
}
