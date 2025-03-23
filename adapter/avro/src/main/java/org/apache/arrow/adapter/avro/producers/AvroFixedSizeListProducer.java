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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces array values from a {@link FixedSizeListVector}, writes data to an avro
 * encoder.
 */
public class AvroFixedSizeListProducer extends BaseAvroProducer<FixedSizeListVector> {

  private final Producer<? extends FieldVector> delegate;

  /** Instantiate an AvroFixedSizeListProducer. */
  public AvroFixedSizeListProducer(
      FixedSizeListVector vector, Producer<? extends FieldVector> delegate) {
    super(vector);
    this.delegate = delegate;
  }

  @Override
  public void produce(Encoder encoder) throws IOException {

    encoder.writeArrayStart();
    encoder.setItemCount(vector.getListSize());

    for (int i = 0; i < vector.getListSize(); i++) {
      encoder.startItem();
      delegate.produce(encoder);
    }

    encoder.writeArrayEnd();
    currentIndex++;
  }

  // Do not override skipNull(), the delegate delegate vector will not hold data

  @Override
  public void setPosition(int index) {
    int delegateOffset = vector.getOffsetBuffer().getInt(index * (long) Integer.BYTES);
    delegate.setPosition(delegateOffset);
    super.setPosition(index);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void resetValueVector(FixedSizeListVector vector) {
    ((Producer<FieldVector>) delegate).resetValueVector(vector.getDataVector());
  }
}
