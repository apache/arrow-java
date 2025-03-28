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
package org.apache.arrow.adapter.avro.producers;

import java.io.IOException;
import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Encoder;

/**
 * Producer wrapper which producers nullable types to an avro encoder. Write the data to the
 * underlying {@link FieldVector}.
 *
 * @param <T> The vector within producer or its delegate, used for partially produce purpose.
 */
public class AvroNullableProducer<T extends FieldVector> extends BaseAvroProducer<T> {

  private final Producer<T> delegate;

  /** Instantiate a AvroNullableProducer. */
  public AvroNullableProducer(Producer<T> delegate) {
    super(delegate.getVector());
    this.delegate = delegate;
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    if (vector.isNull(currentIndex)) {
      encoder.writeInt(1);
      encoder.writeNull();
      delegate.skipNull();
    } else {
      encoder.writeInt(0);
      delegate.produce(encoder);
    }
    currentIndex++;
  }

  @Override
  public void skipNull() {
    // Can be called by containers of nullable types
    delegate.skipNull();
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
  public void resetValueVector(T vector) {
    delegate.resetValueVector(vector);
  }

  @Override
  public T getVector() {
    return delegate.getVector();
  }
}
