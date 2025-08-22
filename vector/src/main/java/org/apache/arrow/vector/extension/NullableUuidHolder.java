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
package org.apache.arrow.vector.extension;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.ExtensionHolder;

/** Nullable holder for UUID extension type values. */
public class NullableUuidHolder extends ExtensionHolder {
  /** Buffer containing the UUID bytes (16 bytes) when isSet = 1, undefined when isSet = 0. */
  public ArrowBuf buffer;

  /** Default constructor initializes the holder as null (isSet = 0). */
  public NullableUuidHolder() {
    this.isSet = 0;
  }

  /**
   * Reason for not supporting the operation is that ValueHolders are potential scalar replacements
   * and hence we don't want any methods to be invoked on them.
   */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /**
   * Reason for not supporting the operation is that ValueHolders are potential scalar replacements
   * and hence we don't want any methods to be invoked on them.
   */
  @Override
  public String toString() {
    throw new UnsupportedOperationException();
  }
}
