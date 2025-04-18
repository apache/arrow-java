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
package org.apache.arrow.adapter.avro.consumers.logical;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.adapter.avro.consumers.BaseAvroConsumer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume 256-bit decimal type values from avro decoder. Write the data to {@link
 * Decimal256Vector}.
 */
public abstract class AvroDecimal256Consumer extends BaseAvroConsumer<Decimal256Vector> {

  protected AvroDecimal256Consumer(Decimal256Vector vector) {
    super(vector);
  }

  /** Consumer for decimal logical type with 256 bit width and original bytes type. */
  public static class BytesDecimal256Consumer extends AvroDecimal256Consumer {

    private ByteBuffer cacheBuffer;

    /** Instantiate a BytesDecimal256Consumer. */
    public BytesDecimal256Consumer(Decimal256Vector vector) {
      super(vector);
    }

    @Override
    public void consume(Decoder decoder) throws IOException {
      cacheBuffer = decoder.readBytes(cacheBuffer);
      byte[] bytes = new byte[cacheBuffer.limit()];
      Preconditions.checkArgument(bytes.length <= 32, "Decimal bytes length should <= 32.");
      cacheBuffer.get(bytes);
      vector.setBigEndian(currentIndex++, bytes);
    }
  }

  /** Consumer for decimal logical type with 256 bit width and original fixed type. */
  public static class FixedDecimal256Consumer extends AvroDecimal256Consumer {

    private final byte[] reuseBytes;

    /** Instantiate a FixedDecimal256Consumer. */
    public FixedDecimal256Consumer(Decimal256Vector vector, int size) {
      super(vector);
      Preconditions.checkArgument(size <= 32, "Decimal bytes length should <= 32.");
      reuseBytes = new byte[size];
    }

    @Override
    public void consume(Decoder decoder) throws IOException {
      decoder.readFixed(reuseBytes);
      vector.setBigEndian(currentIndex++, reuseBytes);
    }
  }
}
