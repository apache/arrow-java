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
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that converts nanoseconds from a {@link TimeNanoVector} and produces time (microseconds)
 * values, writes data to an Avro encoder.
 */
public class AvroTimeNanoProducer extends BaseAvroProducer<TimeNanoVector> {

  // Convert nanoseconds to microseconds for Avro time-micros (LONG) type
  // Range is 1000 times less than for microseconds, so the type will fit (with loss of precision)

  private static final long NANOS_PER_MICRO = 1000;

  public AvroTimeNanoProducer(TimeNanoVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long nanos = vector.getDataBuffer().getLong(currentIndex * (long) TimeNanoVector.TYPE_WIDTH);
    long micros = nanos / NANOS_PER_MICRO;
    encoder.writeLong(micros);
    currentIndex++;
  }
}
