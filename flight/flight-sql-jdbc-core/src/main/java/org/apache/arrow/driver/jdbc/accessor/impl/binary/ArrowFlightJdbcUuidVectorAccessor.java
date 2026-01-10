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
package org.apache.arrow.driver.jdbc.accessor.impl.binary;

import java.util.UUID;
import java.util.function.IntSupplier;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.util.UuidUtility;

/**
 * Accessor for the Arrow UUID extension type ({@link UuidVector}).
 *
 * <p>This accessor provides JDBC-compatible access to UUID values stored in Arrow's canonical UUID
 * extension type ('arrow.uuid'). It follows PostgreSQL JDBC driver conventions:
 *
 * <ul>
 *   <li>{@link #getObject()} returns {@link java.util.UUID}
 *   <li>{@link #getString()} returns the hyphenated string format (e.g.,
 *       "550e8400-e29b-41d4-a716-446655440000")
 *   <li>{@link #getBytes()} returns the 16-byte binary representation
 * </ul>
 */
public class ArrowFlightJdbcUuidVectorAccessor extends ArrowFlightJdbcAccessor {

  private final UuidVector vector;

  /**
   * Creates a new accessor for a UUID vector.
   *
   * @param vector the UUID vector to access
   * @param currentRowSupplier supplier for the current row index
   * @param setCursorWasNull consumer to set the wasNull flag
   */
  public ArrowFlightJdbcUuidVectorAccessor(
      UuidVector vector,
      IntSupplier currentRowSupplier,
      ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
  }

  @Override
  public Object getObject() {
    UUID uuid = vector.getObject(getCurrentRow());
    this.wasNull = uuid == null;
    this.wasNullConsumer.setWasNull(this.wasNull);
    return uuid;
  }

  @Override
  public Class<?> getObjectClass() {
    return UUID.class;
  }

  @Override
  public String getString() {
    UUID uuid = (UUID) getObject();
    if (uuid == null) {
      return null;
    }
    return uuid.toString();
  }

  @Override
  public byte[] getBytes() {
    UUID uuid = (UUID) getObject();
    if (uuid == null) {
      return null;
    }
    return UuidUtility.getBytesFromUUID(uuid);
  }
}
