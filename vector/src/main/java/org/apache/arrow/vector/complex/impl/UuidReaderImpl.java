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
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.holders.NullableUuidHolder;
import org.apache.arrow.vector.holders.UuidHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Reader implementation for {@link UuidVector}.
 *
 * <p>Provides methods to read UUID values from a vector, including support for reading into {@link
 * UuidHolder} and retrieving values as {@link java.util.UUID} objects.
 *
 * @see UuidVector
 * @see org.apache.arrow.vector.extension.UuidType
 */
public class UuidReaderImpl extends AbstractFieldReader {

  private final UuidVector vector;

  /**
   * Constructs a reader for the given UUID vector.
   *
   * @param vector the UUID vector to read from
   */
  public UuidReaderImpl(UuidVector vector) {
    super();
    this.vector = vector;
  }

  @Override
  public MinorType getMinorType() {
    return vector.getMinorType();
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  @Override
  public boolean isSet() {
    return !vector.isNull(idx());
  }

  @Override
  public void read(ExtensionHolder holder) {
    if (holder instanceof NullableUuidHolder) {
      vector.get(idx(), (NullableUuidHolder) holder);
    } else {
      throw new IllegalArgumentException(
          "Unsupported holder type for UuidReader: " + holder.getClass());
    }
  }

  @Override
  public void read(int arrayIndex, ExtensionHolder holder) {
    if (holder instanceof NullableUuidHolder) {
      vector.get(arrayIndex, (NullableUuidHolder) holder);
    } else {
      throw new IllegalArgumentException(
          "Unsupported holder type for UuidReader: " + holder.getClass());
    }
  }

  @Override
  public void copyAsValue(AbstractExtensionTypeWriter writer) {
    UuidWriterImpl impl = (UuidWriterImpl) writer;
    impl.vector.copyFromSafe(idx(), impl.idx(), vector);
  }

  @Override
  public Object readObject() {
    return vector.getObject(idx());
  }
}
