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
import org.apache.arrow.adapter.avro.producers.BaseAvroProducer;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that converts seconds from a {@link TimeSecVector} and produces time (microseconds)
 * values, writes data to an Avro encoder.
 */
public class AvroTimeSecProducer extends BaseAvroProducer<TimeSecVector> {

  // Convert seconds to milliseconds for Avro time-millis (INT) type
  // INT is enough to cover the number of milliseconds in a day
  // So overflows should not happen if values are valid times of day

  private static final int MILLIS_PER_SECOND = 1000;
  private static final long OVERFLOW_LIMIT = Integer.MAX_VALUE / 1000;

  /** Instantiate an AvroTimeSecProducer. */
  public AvroTimeSecProducer(TimeSecVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    int seconds = vector.getDataBuffer().getInt(currentIndex * (long) TimeSecVector.TYPE_WIDTH);
    if (Math.abs(seconds) > OVERFLOW_LIMIT) {
      throw new ArithmeticException("Time value is too large for Avro encoding");
    }
    int millis = seconds * MILLIS_PER_SECOND;
    encoder.writeInt(millis);
    currentIndex++;
  }
}
