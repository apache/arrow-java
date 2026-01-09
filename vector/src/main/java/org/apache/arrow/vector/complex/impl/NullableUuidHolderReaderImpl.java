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

import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.holders.NullableUuidHolder;
import org.apache.arrow.vector.holders.UuidHolder;
import org.apache.arrow.vector.types.Types;

/**
 * Reader implementation for reading UUID values from a {@link NullableUuidHolder}.
 *
 * <p>This reader wraps a single UUID holder value and provides methods to read from it. Unlike
 * {@link UuidReaderImpl} which reads from a vector, this reader operates on a holder instance.
 *
 * @see NullableUuidHolder
 * @see UuidReaderImpl
 */
public class NullableUuidHolderReaderImpl extends AbstractFieldReader {
  private final NullableUuidHolder holder;

  /**
   * Constructs a reader for the given UUID holder.
   *
   * @param holder the UUID holder to read from
   */
  public NullableUuidHolderReaderImpl(NullableUuidHolder holder) {
    this.holder = holder;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException(
        "size() is not supported on NullableUuidHolderReaderImpl. "
            + "This reader wraps a single UUID holder value, not a collection. "
            + "Use UuidReaderImpl for vector-based UUID reading.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException(
        "next() is not supported on NullableUuidHolderReaderImpl. "
            + "This reader wraps a single UUID holder value, not an iterator. "
            + "Use UuidReaderImpl for vector-based UUID reading.");
  }

  @Override
  public void setPosition(int index) {
    throw new UnsupportedOperationException(
        "setPosition() is not supported on NullableUuidHolderReaderImpl. "
            + "This reader wraps a single UUID holder value, not a vector. "
            + "Use UuidReaderImpl for vector-based UUID reading.");
  }

  @Override
  public Types.MinorType getMinorType() {
    return Types.MinorType.EXTENSIONTYPE;
  }

  @Override
  public boolean isSet() {
    return holder.isSet == 1;
  }

  @Override
  public void read(ExtensionHolder h) {
    if (h instanceof NullableUuidHolder) {
      NullableUuidHolder nullableHolder = (NullableUuidHolder) h;
      nullableHolder.mostSigBits = this.holder.mostSigBits;
      nullableHolder.leastSigBits = this.holder.leastSigBits;
      nullableHolder.isSet = this.holder.isSet;
    } else if (h instanceof UuidHolder) {
      UuidHolder uuidHolder = (UuidHolder) h;
      uuidHolder.mostSigBits = this.holder.mostSigBits;
      uuidHolder.leastSigBits = this.holder.leastSigBits;
    } else {
      throw new IllegalArgumentException(
          "Unsupported holder type: "
              + h.getClass().getName()
              + ". "
              + "Only NullableUuidHolder and UuidHolder are supported for UUID values. "
              + "Provided holder type cannot be used to read UUID data.");
    }
  }

  @Override
  public Object readObject() {
    if (!isSet()) {
      return null;
    }
    // Convert UUID longs to Java UUID object
    return holder.getUuid();
  }
}
