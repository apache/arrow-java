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
import org.apache.arrow.vector.DateMilliVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that converts days in milliseconds from a {@link DateMilliVector} and produces date
 * (INT) values, writes data to an Avro encoder.
 */
public class AvroDateMilliProducer extends BaseAvroProducer<DateMilliVector> {

  // Convert milliseconds to days for Avro date type

  private static final long MILLIS_PER_DAY = 86400000;

  /** Instantiate an AvroDateMilliProducer. */
  public AvroDateMilliProducer(DateMilliVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long millis = vector.getDataBuffer().getLong(currentIndex * (long) DateMilliVector.TYPE_WIDTH);
    long days = millis / MILLIS_PER_DAY;
    if (days > (long) Integer.MAX_VALUE || days < (long) Integer.MIN_VALUE) {
      throw new ArithmeticException("Date value is too large for Avro encoding");
    }
    encoder.writeInt((int) days);
    currentIndex++;
  }
}
