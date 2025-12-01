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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.holders.NullableUuidHolder;
import org.apache.arrow.vector.holders.UuidHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Writer implementation for {@link UuidVector}.
 *
 * <p>Supports writing UUID values in multiple formats: {@link java.util.UUID}, byte arrays, and
 * {@link ArrowBuf}. Also handles {@link UuidHolder} and {@link NullableUuidHolder}.
 *
 * @see UuidVector
 * @see org.apache.arrow.vector.extension.UuidType
 */
public class UuidWriterImpl extends AbstractExtensionTypeWriter<UuidVector> {

  /**
   * Constructs a writer for the given UUID vector.
   *
   * @param vector the UUID vector to write to
   */
  public UuidWriterImpl(UuidVector vector) {
    super(vector);
  }

  @Override
  public void writeExtension(Object value) {
    if (value instanceof byte[]) {
      vector.setSafe(getPosition(), (byte[]) value);
    } else if (value instanceof ArrowBuf) {
      vector.setSafe(getPosition(), (ArrowBuf) value);
    } else if (value instanceof java.util.UUID) {
      vector.setSafe(getPosition(), (java.util.UUID) value);
    } else {
      throw new IllegalArgumentException("Unsupported value type for UUID: " + value.getClass());
    }
    vector.setValueCount(getPosition() + 1);
  }

  @Override
  public void writeExtension(Object value, ArrowType type) {
    writeExtension(value);
  }

  @Override
  public void write(ExtensionHolder holder) {
    if (holder instanceof UuidHolder) {
      vector.setSafe(getPosition(), (UuidHolder) holder);
    } else if (holder instanceof NullableUuidHolder) {
      vector.setSafe(getPosition(), (NullableUuidHolder) holder);
    }
    vector.setValueCount(getPosition() + 1);
  }
}
