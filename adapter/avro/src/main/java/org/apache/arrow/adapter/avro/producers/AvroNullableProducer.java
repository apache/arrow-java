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

import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Encoder;

import java.io.IOException;


public class AvroNullableProducer<T extends FieldVector> extends BaseAvroProducer<T> {

  private final Producer<T> delegate;

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
    }
    else {
      encoder.writeInt(0);
      delegate.produce(encoder);
    }
    currentIndex++;
  }

  @Override
  public void skipNull() {
    delegate.skipNull();
    super.skipNull();
  }

  @Override
  public void setPosition(int index) {
    delegate.setPosition(index);
    super.setPosition(index);
  }

  @Override
  public boolean resetValueVector(T vector) {
    return delegate.resetValueVector(vector);
  }

  @Override
  public T getVector() {
    return delegate.getVector();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
