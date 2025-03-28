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
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces fixed-size binary values from a {@link FixedSizeBinaryVector}, writes data
 * to an Avro encoder.
 *
 * <p>Logical types are also supported, for vectors derived from {@link BaseFixedWidthVector} where
 * the internal representation is fixed width bytes and requires no conversion.
 */
public class AvroFixedSizeBinaryProducer extends BaseAvroProducer<BaseFixedWidthVector> {

  private final byte[] reuseBytes;

  /** Instantiate an AvroFixedSizeBinaryProducer. */
  public AvroFixedSizeBinaryProducer(FixedSizeBinaryVector vector) {
    super(vector);
    reuseBytes = new byte[vector.getTypeWidth()];
  }

  /** Protected constructor for logical types with a fixed width representation. */
  protected AvroFixedSizeBinaryProducer(BaseFixedWidthVector vector) {
    super(vector);
    reuseBytes = new byte[vector.getTypeWidth()];
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long offset = (long) currentIndex * vector.getTypeWidth();
    vector.getDataBuffer().getBytes(offset, reuseBytes);
    encoder.writeFixed(reuseBytes);
    currentIndex++;
  }
}
