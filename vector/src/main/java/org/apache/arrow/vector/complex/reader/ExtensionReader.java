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
package org.apache.arrow.vector.complex.reader;

import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Interface for reading extension types. Extends the functionality of {@link BaseReader}. */
public interface ExtensionReader extends BaseReader {

  /**
   * Reads to the given extension holder.
   *
   * @param holder the {@link ExtensionHolder} to read
   */
  void read(ExtensionHolder holder);

  /**
   * Reads and returns an object representation of the extension type.
   *
   * @return the object representation of the extension type
   */
  Object readObject();

  /**
   * Checks if the current value is set.
   *
   * @return true if the value is set, false otherwise
   */
  boolean isSet();

  /**
   * Returns the {@link ArrowType} of the extension data this reader exposes.
   *
   * <p>The default derives the type from {@link #getField()}, which works for vector-backed
   * readers. Holder-backed readers (which have no notion of a {@code Field}) must override this to
   * return the type carried by their {@link ExtensionHolder} directly.
   *
   * <p>Callers that need the extension {@link ArrowType} (e.g. to route a value through a union or
   * promotable writer) should prefer this over {@code getField().getType()} so the call is
   * well-defined regardless of how the reader is backed.
   *
   * @return the extension {@link ArrowType}
   */
  default ArrowType getExtensionType() {
    return getField().getType();
  }
}
