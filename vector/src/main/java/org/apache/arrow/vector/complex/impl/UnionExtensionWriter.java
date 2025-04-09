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

import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.types.pojo.Field;

public class UnionExtensionWriter extends AbstractFieldWriter {
  protected ExtensionTypeVector vector;
  protected FieldWriter writer;

  public UnionExtensionWriter(ExtensionTypeVector vector) {
    this.vector = vector;
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public Field getField() {
    return vector.getField();
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }

  @Override
  public void writeExtension(Object var1) {
    this.writer.writeExtension(var1);
  }

  @Override
  public void addExtensionTypeWriterFactory(ExtensionTypeWriterFactory factory) {
    this.writer = factory.getWriterImpl(vector);
    this.writer.setPosition(idx());
  }

  public void write(ExtensionHolder holder) {
    this.writer.write(holder);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    if (this.writer != null) {
      this.writer.setPosition(index);
    }
  }
}
