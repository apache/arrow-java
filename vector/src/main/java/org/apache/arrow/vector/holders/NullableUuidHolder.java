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
package org.apache.arrow.vector.holders;

import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import java.util.UUID;

/**
 * Value holder for nullable UUID values.
 *
 * <p>The {@code isSet} field controls nullability: when {@code isSet = 1}, the holder contains a
 * valid UUID represented as two longs; when {@code isSet = 0}, the holder represents a null value
 * and the long fields should not be accessed.
 *
 * @see UuidHolder
 * @see org.apache.arrow.vector.UuidVector
 * @see org.apache.arrow.vector.extension.UuidType
 */
public class NullableUuidHolder extends ExtensionHolder {
  /** The most significant 64 bits of the UUID. */
  public long mostSigBits;

  /** The least significant 64 bits of the UUID. */
  public long leastSigBits;

  /**
   * Converts the holder's two longs to a UUID object.
   *
   * @return the UUID represented by this holder, or null if isSet is 0
   */
  public UUID getUuid() {
    if (this.isSet == 0) {
      return null;
    }
    return new UUID(mostSigBits, leastSigBits);
  }

  @Override
  public ArrowType type() {
    return UuidType.INSTANCE;
  }
}
