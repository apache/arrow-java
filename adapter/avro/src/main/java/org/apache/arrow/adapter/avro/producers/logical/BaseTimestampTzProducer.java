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
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.arrow.adapter.avro.producers.BaseAvroProducer;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.avro.io.Encoder;

abstract class BaseTimestampTzProducer<T extends TimeStampVector>
    extends BaseAvroProducer<TimeStampVector> {

  // Convert TZ values to UTC to encode Avro timestamp types
  // Where possible, used a fixed offset (zero for UTC or missing zone info)
  // Conversion using named zones is more expensive and depends on the concrete type

  protected final ZoneId zoneId;
  protected final boolean fixedOffsetFlag;
  protected final long fixedOffset;

  protected abstract long convertToUtc(long tzValue, ZoneId zoneId);

  protected BaseTimestampTzProducer(T vector, String zoneName, long offsetMultiplier) {
    super(vector);
    zoneId = zoneName != null ? ZoneId.of(zoneName) : ZoneOffset.UTC;
    if (zoneId instanceof ZoneOffset) {
      ZoneOffset offset = (ZoneOffset) zoneId;
      fixedOffsetFlag = true;
      fixedOffset = (long) offset.getTotalSeconds() * offsetMultiplier;
    } else {
      fixedOffsetFlag = false;
      fixedOffset = 0;
    }
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long tzValue = vector.getDataBuffer().getLong(currentIndex * (long) TimeStampVector.TYPE_WIDTH);
    long utcValue = fixedOffsetFlag ? tzValue + fixedOffset : convertToUtc(tzValue, zoneId);
    encoder.writeLong(utcValue);
  }
}
