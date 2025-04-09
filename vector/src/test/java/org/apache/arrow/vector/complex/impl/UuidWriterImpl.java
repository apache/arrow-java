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

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.holder.UuidHolder;
import org.apache.arrow.vector.holders.ExtensionHolder;

public class UuidWriterImpl extends AbstractExtensionTypeWriter<UuidVector> {

  public UuidWriterImpl(UuidVector vector) {
    super(vector);
  }

  @Override
  public void writeExtension(Object value) {
    UUID uuid = (UUID) value;
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    vector.setSafe(getPosition(), bb.array());
    vector.setValueCount(getPosition() + 1);
  }

  @Override
  public void write(ExtensionHolder holder) {
    UuidHolder uuidHolder = (UuidHolder) holder;
    vector.setSafe(getPosition(), uuidHolder.value);
    vector.setValueCount(getPosition() + 1);
  }
}
