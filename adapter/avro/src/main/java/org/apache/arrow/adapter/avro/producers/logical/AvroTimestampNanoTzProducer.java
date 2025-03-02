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

import java.time.Instant;
import java.time.ZoneId;
import org.apache.arrow.vector.TimeStampNanoTZVector;

/**
 * Producer that converts timestamps in zone-aware epoch nanoseconds from a {@link
 * TimeStampNanoTZVector} and produces UTC timestamp (nanoseconds) values, writes data to an Avro
 * encoder.
 */
public class AvroTimestampNanoTzProducer extends BaseTimestampTzProducer<TimeStampNanoTZVector> {

  private static final long NANOS_PER_SECOND = 1000000000;

  /** Instantiate an AvroTimestampNanoTzProducer. */
  public AvroTimestampNanoTzProducer(TimeStampNanoTZVector vector) {
    super(vector, vector.getTimeZone(), NANOS_PER_SECOND);
  }

  @Override
  protected long convertToUtc(long tzValue, ZoneId zoneId) {
    // For negative values, e.g. -.5 seconds = -1 second + .5 in nanos
    long tzSeconds = tzValue >= 0 ? tzValue / NANOS_PER_SECOND : tzValue / NANOS_PER_SECOND - 1;
    long tzNano = tzValue % NANOS_PER_SECOND;
    Instant utcInstant = Instant.ofEpochSecond(tzSeconds, tzNano).atZone(zoneId).toInstant();
    long utcSeconds = utcInstant.getEpochSecond();
    long utcNano = utcInstant.getNano();
    return utcSeconds * NANOS_PER_SECOND + utcNano;
  }
}
