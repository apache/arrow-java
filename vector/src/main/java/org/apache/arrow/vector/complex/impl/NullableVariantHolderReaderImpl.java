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

import org.apache.arrow.vector.holders.NullableVariantHolder;
import org.apache.arrow.vector.types.Types;

public class NullableVariantHolderReaderImpl extends AbstractFieldReader {
  private final NullableVariantHolder holder;

  public NullableVariantHolderReaderImpl(NullableVariantHolder holder) {
    this.holder = holder;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("You can't call size on a Holder value reader.");
  }

  @Override
  public boolean next() {
    throw new UnsupportedOperationException("You can't call next on a single value reader.");
  }

  @Override
  public void setPosition(int index) {
    throw new UnsupportedOperationException("You can't call setPosition on a single value reader.");
  }

  @Override
  public Types.MinorType getMinorType() {
    return Types.MinorType.EXTENSIONTYPE;
  }

  @Override
  public boolean isSet() {
    return holder.isSet == 1;
  }

  /**
   * Reads the variant holder data into the provided holder.
   *
   * @param h the holder to read into
   */
  public void read(NullableVariantHolder h) {
    h.metadataStart = this.holder.metadataStart;
    h.metadataEnd = this.holder.metadataEnd;
    h.metadataBuffer = this.holder.metadataBuffer;
    h.valueStart = this.holder.valueStart;
    h.valueEnd = this.holder.valueEnd;
    h.valueBuffer = this.holder.valueBuffer;
    h.isSet = this.isSet() ? 1 : 0;
  }
}
