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
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that converts epoch seconds from a {@link TimeStampSecVector} and produces local
 * timestamp (milliseconds) values, writes data to an Avro encoder.
 */
public class AvroTimestampSecProducer extends BaseAvroProducer<TimeStampSecVector> {

  // Avro does not support timestamps in seconds, so convert to local-timestamp-millis type
  // Check for overflow and raise an exception

  private static final long MILLIS_PER_SECOND = 1000;
  private static final long OVERFLOW_LIMIT = Long.MAX_VALUE / MILLIS_PER_SECOND;

  /** Instantiate an AvroTimestampSecProducer. */
  public AvroTimestampSecProducer(TimeStampSecVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long seconds =
        vector.getDataBuffer().getLong(currentIndex * (long) TimeStampSecVector.TYPE_WIDTH);
    if (Math.abs(seconds) > OVERFLOW_LIMIT) {
      throw new ArithmeticException("Timestamp value is too large for Avro encoding");
    }
    long millis = seconds * MILLIS_PER_SECOND;
    encoder.writeLong(millis);
    currentIndex++;
  }
}
