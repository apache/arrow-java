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
package org.apache.arrow.adapter.avro.consumers;

import java.io.IOException;
import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer wrapper which consumes nullable type values from avro decoder. Write the data to the
 * underlying {@link FieldVector}.
 *
 * @param <T> The vector within consumer or its delegate.
 */
public class AvroNullableConsumer<T extends FieldVector> extends BaseAvroConsumer<T> {

  private final Consumer<T> delegate;
  private final int nullIndex;

  /** Instantiate a AvroNullableConsumer. */
  @SuppressWarnings("unchecked")
  public AvroNullableConsumer(Consumer<T> delegate, int nullIndex) {
    super((T) delegate.getVector());
    this.delegate = delegate;
    this.nullIndex = nullIndex;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    int typeIndex = decoder.readInt();
    if (typeIndex == nullIndex) {
      decoder.readNull();
      delegate.addNull();
    } else {
      delegate.consume(decoder);
    }
    currentIndex++;
  }

  @Override
  public void addNull() {
    // Can be called by containers of nullable types
    delegate.addNull();
    currentIndex++;
  }

  @Override
  public void setPosition(int index) {
    if (index < 0 || index > vector.getValueCount()) {
      throw new IllegalArgumentException("Index out of bounds");
    }
    delegate.setPosition(index);
    super.setPosition(index);
  }

  @Override
  public boolean resetValueVector(T vector) {
    boolean delegateOk = delegate.resetValueVector(vector);
    boolean thisOk = super.resetValueVector(vector);
    return thisOk && delegateOk;
  }

  @Override
  public void close() throws Exception {
    super.close();
    delegate.close();
  }
}
