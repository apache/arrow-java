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
import org.apache.arrow.vector.complex.MapVector;
import org.apache.avro.io.Encoder;

/** Producer which produces map type values to avro encoder. Write the data to {@link MapVector}. */
public class AvroMapProducer extends BaseAvroProducer<MapVector> {

  private final Producer<? extends FieldVector> delegate;

  /** Instantiate a AvroMapProducer. */
  public AvroMapProducer(MapVector vector, Producer<? extends FieldVector> delegate) {
    super(vector);
    this.delegate = delegate;
  }

  @Override
  public void produce(Encoder encoder) throws IOException {

    int startOffset = vector.getOffsetBuffer().getInt(currentIndex * (long) Integer.BYTES);
    int endOffset = vector.getOffsetBuffer().getInt((currentIndex + 1) * (long) Integer.BYTES);
    int nEntries = endOffset - startOffset;

    encoder.writeMapStart();
    encoder.setItemCount(nEntries);

    for (int i = 0; i < nEntries; i++) {
      encoder.startItem();
      delegate.produce(encoder);
    }

    encoder.writeMapEnd();
    currentIndex++;
  }

  // Do not override skipNull(), delegate will not have an entry if the map is null

  @Override
  public void setPosition(int index) {
    if (index < 0 || index > vector.getValueCount()) {
      throw new IllegalArgumentException("Index out of bounds");
    }
    int delegateOffset = vector.getOffsetBuffer().getInt(index * (long) Integer.BYTES);
    delegate.setPosition(delegateOffset);
    super.setPosition(index);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void resetValueVector(MapVector vector) {
    ((Producer<FieldVector>) delegate).resetValueVector(vector.getDataVector());
  }
}
