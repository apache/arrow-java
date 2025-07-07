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
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that decodes values from a dictionary-encoded {@link FieldVector}, writes the resulting
 * values to an Avro encoder.
 *
 * @param <T> Type of the underlying dictionary vector
 */
public class DictionaryDecodingProducer<T extends FieldVector>
    extends BaseAvroProducer<BaseIntVector> {

  private final Producer<T> dictProducer;

  /** Instantiate a DictionaryDecodingProducer. */
  public DictionaryDecodingProducer(BaseIntVector indexVector, Producer<T> dictProducer) {
    super(indexVector);
    this.dictProducer = dictProducer;
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    int dicIndex = (int) vector.getValueAsLong(currentIndex++);
    dictProducer.setPosition(dicIndex);
    dictProducer.produce(encoder);
  }
}
