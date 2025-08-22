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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

public class UuidType extends ExtensionType {
  private static final AtomicBoolean registered = new AtomicBoolean(false);
  public static final UuidType INSTANCE = new UuidType();

  /** Register the extension type so it can be used globally. */
  public static void ensureRegistered() {
    if (!registered.getAndSet(true)) {
      // The values don't matter, we just need an instance
      ExtensionTypeRegistry.register(INSTANCE);
    }
  }

  @Override
  public ArrowType storageType() {
    return new ArrowType.FixedSizeBinary(16);
  }

  @Override
  public String extensionName() {
    return "uuid";
  }

  @Override
  public boolean extensionEquals(ExtensionType other) {
    return other instanceof UuidType;
  }

  @Override
  public ArrowType deserialize(ArrowType storageType, String serializedData) {
    if (!storageType.equals(storageType())) {
      throw new UnsupportedOperationException(
          "Cannot construct UuidType from underlying type " + storageType);
    }
    return INSTANCE;
  }

  @Override
  public String serialize() {
    return "";
  }

  @Override
  public boolean isComplex() {
    return false;
  }

  @Override
  public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
    return new UuidVector(name, allocator, new FixedSizeBinaryVector(name, allocator, 16));
  }
}
