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
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that converts epoch seconds from a {@link TimeStampSecTZVector} and produces UTC
 * timestamp (milliseconds) values, writes data to an Avro encoder.
 */
public class AvroTimestampSecTzProducer extends BaseAvroProducer<TimeStampSecTZVector> {

  // Avro does not support timestamps in seconds, so convert to timestamp-millis type
  // Check for overflow and raise an exception

  // Both Arrow and Avro store zone-aware times in UTC so zone conversion is not needed

  private static final long MILLIS_PER_SECOND = 1000;
  private static final long OVERFLOW_LIMIT = Long.MAX_VALUE / MILLIS_PER_SECOND;

  /** Instantiate an AvroTimestampSecTzProducer. */
  public AvroTimestampSecTzProducer(TimeStampSecTZVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long utcSeconds =
        vector.getDataBuffer().getLong(currentIndex * (long) TimeStampVector.TYPE_WIDTH);
    if (Math.abs(utcSeconds) > OVERFLOW_LIMIT) {
      throw new ArithmeticException("Timestamp value is too large for Avro encoding");
    }
    long utcMillis = utcSeconds * MILLIS_PER_SECOND;
    encoder.writeLong(utcMillis);
    currentIndex++;
  }
}
