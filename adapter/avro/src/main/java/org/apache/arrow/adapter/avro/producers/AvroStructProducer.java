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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.avro.io.Encoder;

/**
 * Producer which produces nested record type values to avro encoder. Read the data from {@link
 * org.apache.arrow.vector.complex.StructVector}.
 */
public class AvroStructProducer extends BaseAvroProducer<StructVector> {

  private final Producer<? extends FieldVector>[] delegates;

  /** Instantiate a AvroStructProducer. */
  public AvroStructProducer(StructVector vector, Producer<? extends FieldVector>[] delegates) {
    super(vector);
    this.delegates = delegates;
  }

  @Override
  public void produce(Encoder encoder) throws IOException {

    for (Producer<?> delegate : delegates) {
      delegate.produce(encoder);
    }

    currentIndex++;
  }

  @Override
  public void skipNull() {
    for (Producer<?> delegate : delegates) {
      delegate.skipNull();
    }
    super.skipNull();
  }

  @Override
  public void setPosition(int index) {
    if (index < 0 || index > vector.getValueCount()) {
      throw new IllegalArgumentException("Index out of bounds: " + index);
    }
    for (Producer<?> delegate : delegates) {
      delegate.setPosition(index);
    }
    super.setPosition(index);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void resetValueVector(StructVector vector) {
    for (int i = 0; i < delegates.length; i++) {
      Producer<FieldVector> delegate = (Producer<FieldVector>) delegates[i];
      delegate.resetValueVector(vector.getChildrenFromFields().get(i));
    }
  }
}
