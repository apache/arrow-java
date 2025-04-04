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
package org.apache.arrow.adapter.avro.producers.logical;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.arrow.adapter.avro.producers.BaseAvroProducer;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces decimal values from a {@link Decimal256Vector}, writes data to an Avro
 * encoder.
 */
public class AvroDecimal256Producer extends BaseAvroProducer<Decimal256Vector> {

  // Logic is the same as for DecimalVector (128 bit)

  byte[] encodedBytes = new byte[Decimal256Vector.TYPE_WIDTH];

  /** Instantiate an AvroDecimalProducer. */
  public AvroDecimal256Producer(Decimal256Vector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    BigDecimal value = vector.getObject(currentIndex++);
    AvroDecimalProducer.encodeDecimal(value, encodedBytes);
    encoder.writeFixed(encodedBytes);
  }
}
