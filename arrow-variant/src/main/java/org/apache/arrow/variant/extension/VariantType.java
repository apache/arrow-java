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
package org.apache.arrow.variant.extension;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.variant.impl.VariantWriterImpl;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Arrow extension type for <a
 * href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Parquet
 * Variant</a> binary encoding. The type itself does not support shredded variant data.
 */
public final class VariantType extends ExtensionType {

  public static final VariantType INSTANCE = new VariantType();

  public static final String EXTENSION_NAME = "parquet.variant";

  static {
    ExtensionTypeRegistry.register(INSTANCE);
  }

  private VariantType() {}

  @Override
  public ArrowType storageType() {
    return ArrowType.Struct.INSTANCE;
  }

  @Override
  public String extensionName() {
    return EXTENSION_NAME;
  }

  @Override
  public boolean extensionEquals(ExtensionType other) {
    return other instanceof VariantType;
  }

  @Override
  public String serialize() {
    return "";
  }

  @Override
  public ArrowType deserialize(ArrowType storageType, String serializedData) {
    if (!storageType.equals(this.storageType())) {
      throw new UnsupportedOperationException(
          "Cannot construct VariantType from underlying type " + storageType);
    }
    return INSTANCE;
  }

  @Override
  public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
    return new VariantVector(name, allocator);
  }

  @Override
  public boolean isComplex() {
    // The type itself is not complex meaning we need separate functions to convert/extract
    // different types.
    // Meanwhile, the containing vector is complex in terms of containing multiple values (metadata
    // and value)
    return false;
  }

  @Override
  public FieldWriter getNewFieldWriter(ValueVector vector) {
    return new VariantWriterImpl((VariantVector) vector);
  }
}
