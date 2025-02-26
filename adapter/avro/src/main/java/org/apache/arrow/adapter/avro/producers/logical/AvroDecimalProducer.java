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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.DecimalVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces decimal values from a {@link DecimalVector}, writes data to an Avro
 * encoder.
 */
public abstract class AvroDecimalProducer extends BaseAvroProducer<DecimalVector> {

  /** Instantiate an AvroDecimalProducer. */
  protected AvroDecimalProducer(DecimalVector vector) {
    super(vector);
  }

  /** Producer for decimal logical type with original bytes type. */
  public static class BytesDecimalProducer extends AvroDecimalProducer {

    private final byte[] reuseBytes;

    /** Instantiate a BytesDecimalConsumer. */
    public BytesDecimalProducer(DecimalVector vector) {
      super(vector);
      Preconditions.checkArgument(
          vector.getTypeWidth() <= 16, "Decimal bytes length should <= 16.");
      reuseBytes = new byte[vector.getTypeWidth()];
    }

    @Override
    public void produce(Encoder encoder) throws IOException {
      long offset = (long) currentIndex * vector.getTypeWidth();
      vector.getDataBuffer().getBytes(offset, reuseBytes);
      encoder.writeBytes(reuseBytes);
      currentIndex++;
    }
  }

  /** Producer for decimal logical type with original fixed type. */
  public static class FixedDecimalProducer extends AvroDecimalProducer {

    private final byte[] reuseBytes;

    /** Instantiate a FixedDecimalConsumer. */
    public FixedDecimalProducer(DecimalVector vector, int size) {
      super(vector);
      Preconditions.checkArgument(size <= 16, "Decimal bytes length should <= 16.");
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
}
