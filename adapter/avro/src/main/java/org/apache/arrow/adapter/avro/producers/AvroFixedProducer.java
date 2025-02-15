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

import java.io.IOException;

import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces fixed-size binary values from a {@link FixedSizeBinaryVector},
 * writes data to an Avro encoder.
 */
public class AvroFixedProducer extends BaseAvroProducer<FixedSizeBinaryVector> {

  private final byte[] reuseBytes;

  /**
   * Instantiate an AvroFixedProducer.
   */
  public AvroFixedProducer(FixedSizeBinaryVector vector) {
    super(vector);
    reuseBytes = new byte[vector.getByteWidth()];
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long offset = (long) currentIndex * vector.getByteWidth();
    vector.getDataBuffer().getBytes(offset, reuseBytes);
    encoder.writeBytes(reuseBytes);
    currentIndex++;
  }
}