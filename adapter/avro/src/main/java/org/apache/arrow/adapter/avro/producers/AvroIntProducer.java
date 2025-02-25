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

import org.apache.arrow.vector.IntVector;
import org.apache.avro.io.Encoder;

import java.io.IOException;

/**
 * Producer that produces int values from an {@link IntVector},
 * writes data to an avro encoder.
 */
public class AvroIntProducer extends BaseAvroProducer<IntVector> {

  /**
   * Instantiate an AvroIntConsumer.
   */
  public AvroIntProducer(IntVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    encoder.writeInt(vector.get(currentIndex++));
  }
}
