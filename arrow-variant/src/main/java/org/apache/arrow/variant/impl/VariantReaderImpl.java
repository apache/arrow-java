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
package org.apache.arrow.variant.impl;

import org.apache.arrow.variant.extension.VariantVector;
import org.apache.arrow.variant.holders.NullableVariantHolder;
import org.apache.arrow.variant.holders.VariantHolder;
import org.apache.arrow.vector.complex.impl.AbstractFieldReader;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class VariantReaderImpl extends AbstractFieldReader {
  private final VariantVector vector;

  public VariantReaderImpl(VariantVector vector) {
    this.vector = vector;
  }

  @Override
  public Types.MinorType getMinorType() {
    return this.vector.getMinorType();
  }

  @Override
  public Field getField() {
    return this.vector.getField();
  }

  @Override
  public boolean isSet() {
    return !this.vector.isNull(this.idx());
  }

  @Override
  public void read(ExtensionHolder holder) {
    if (holder instanceof VariantHolder) {
      vector.get(idx(), (VariantHolder) holder);
    } else if (holder instanceof NullableVariantHolder) {
      vector.get(idx(), (NullableVariantHolder) holder);
    } else {
      throw new IllegalArgumentException(
          "Unsupported holder type for VariantReader: " + holder.getClass());
    }
  }

  public void read(VariantHolder h) {
    this.vector.get(this.idx(), h);
  }

  public void read(NullableVariantHolder h) {
    this.vector.get(this.idx(), h);
  }

  @Override
  public Object readObject() {
    return this.vector.getObject(this.idx());
  }
}
