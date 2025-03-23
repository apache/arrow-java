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
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.io.Encoder;

/** Composite producer which holds all producers. It manages the produce and cleanup process. */
public class CompositeAvroProducer {

  private final List<Producer<? extends FieldVector>> producers;

  public CompositeAvroProducer(List<Producer<? extends FieldVector>> producers) {
    this.producers = producers;
  }

  public List<Producer<?>> getProducers() {
    return producers;
  }

  /** Produce encoder data. */
  public void produce(Encoder encoder) throws IOException {
    for (Producer<? extends FieldVector> producer : producers) {
      producer.produce(encoder);
    }
  }

  /** Reset vector of consumers with the given {@link VectorSchemaRoot}. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void resetProducerVectors(VectorSchemaRoot root) {
    // This method assumes that the VSR matches the constructed set of producers
    int index = 0;
    for (Producer producer : producers) {
      producer.resetValueVector(root.getFieldVectors().get(index));
    }
  }
}
