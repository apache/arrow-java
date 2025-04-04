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
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces decimal values from a {@link DecimalVector}, writes data to an Avro
 * encoder.
 */
public class AvroDecimalProducer extends BaseAvroProducer<DecimalVector> {

  // Arrow stores decimals with native endianness, but Avro requires big endian
  // Writing the Arrow representation as fixed bytes fails on little-end machines
  // Instead, we replicate the big endian logic explicitly here
  // See DecimalUtility.writeByteArrayToArrowBufHelper

  byte[] encodedBytes = new byte[DecimalVector.TYPE_WIDTH];

  /** Instantiate an AvroDecimalProducer. */
  public AvroDecimalProducer(DecimalVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    // Use getObject() to go back to a BigDecimal then re-encode
    BigDecimal value = vector.getObject(currentIndex++);
    encodeDecimal(value, encodedBytes);
    encoder.writeFixed(encodedBytes);
  }

  static void encodeDecimal(BigDecimal value, byte[] encodedBytes) {
    byte[] valueBytes = value.unscaledValue().toByteArray();
    byte[] padding = valueBytes[0] < 0 ? DecimalUtility.minus_one : DecimalUtility.zeroes;
    System.arraycopy(padding, 0, encodedBytes, 0, encodedBytes.length - valueBytes.length);
    System.arraycopy(
        valueBytes, 0, encodedBytes, encodedBytes.length - valueBytes.length, valueBytes.length);
  }
}
