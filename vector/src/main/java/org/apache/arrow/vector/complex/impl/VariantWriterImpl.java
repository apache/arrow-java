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
import org.apache.arrow.vector.extension.VariantVector;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.holders.NullableVariantHolder;
import org.apache.arrow.vector.holders.VariantHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.variant.Variant;

/**
 * Writer implementation for VARIANT extension type vectors.
 *
 * <p>This writer handles writing variant data to a {@link VariantVector}. It accepts both {@link
 * VariantHolder} and {@link NullableVariantHolder} objects containing metadata and value buffers
 * and writes them to the appropriate position in the vector.
 */
public class VariantWriterImpl extends AbstractExtensionTypeWriter<VariantVector> {

  private static final String UNSUPPORTED_TYPE_TEMPLATE = "Unsupported type for Variant: %s";

  /**
   * Constructs a new VariantWriterImpl for the given vector.
   *
   * @param vector the variant vector to write to
   */
  public VariantWriterImpl(VariantVector vector) {
    super(vector);
  }

  /**
   * Writes an extension type or variant value to the vector.
   *
   * <p>This method handles {@link ExtensionHolder} by delegating to {@link #write(ExtensionHolder)}
   * and {@link Variant} by delegating to {@link #writeVariant(Variant)}.
   *
   * @param object the object to write, must be an {@link ExtensionHolder} or {@link Variant}
   * @throws IllegalArgumentException if the object is not an {@link ExtensionHolder} or {@link
   *     Variant}
   */
  @Override
  public void writeExtension(Object object) {
    if (object instanceof ExtensionHolder) {
      write((ExtensionHolder) object);
    } else if (object instanceof Variant) {
      writeVariant((Variant) object);
    } else {
      throw new IllegalArgumentException(
          String.format(UNSUPPORTED_TYPE_TEMPLATE, object.getClass().getName()));
    }
  }

  private void writeVariant(Variant variant) {
    java.nio.ByteBuffer metadataBuffer = variant.getMetadataBuffer();
    java.nio.ByteBuffer valueBuffer = variant.getValueBuffer();
    int metadataLength = metadataBuffer.remaining();
    int valueLength = valueBuffer.remaining();
    try (ArrowBuf metadataBuf = vector.getAllocator().buffer(metadataLength);
        ArrowBuf valueBuf = vector.getAllocator().buffer(valueLength)) {
      metadataBuf.setBytes(0, metadataBuffer.duplicate());
      valueBuf.setBytes(0, valueBuffer.duplicate());
      NullableVariantHolder holder = new NullableVariantHolder();
      holder.isSet = 1;
      holder.metadataBuffer = metadataBuf;
      holder.metadataStart = 0;
      holder.metadataEnd = metadataLength;
      holder.valueBuffer = valueBuf;
      holder.valueStart = 0;
      holder.valueEnd = valueLength;
      vector.setSafe(getPosition(), holder);
      vector.setValueCount(getPosition() + 1);
    }
  }

  @Override
  public void writeExtension(Object value, ArrowType type) {
    writeExtension(value);
  }

  /**
   * Writes a variant holder to the vector at the current position.
   *
   * <p>The holder can be either a {@link VariantHolder} (non-nullable, always set) or a {@link
   * NullableVariantHolder} (nullable, may be null). The data is written using {@link
   * VariantVector#setSafe(int, NullableVariantHolder)} which handles buffer allocation and copying.
   *
   * @param extensionHolder the variant holder to write, must be a {@link VariantHolder} or {@link
   *     NullableVariantHolder}
   * @throws IllegalArgumentException if the holder is neither a {@link VariantHolder} nor a {@link
   *     NullableVariantHolder}
   */
  @Override
  public void write(ExtensionHolder extensionHolder) {
    if (extensionHolder instanceof VariantHolder) {
      vector.setSafe(getPosition(), (VariantHolder) extensionHolder);
    } else if (extensionHolder instanceof NullableVariantHolder) {
      vector.setSafe(getPosition(), (NullableVariantHolder) extensionHolder);
    } else {
      throw new IllegalArgumentException(
          String.format(UNSUPPORTED_TYPE_TEMPLATE, extensionHolder.getClass().getName()));
    }
    vector.setValueCount(getPosition() + 1);
  }
}
