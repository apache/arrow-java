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
import org.apache.arrow.vector.BigIntVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces long values from a {@link BigIntVector}, writes data to an Avro encoder.
 *
 * <p>Logical types are also supported, for vectors derived from {@link BaseFixedWidthVector} where
 * the internal representation matches BigIntVector and requires no conversion.
 */
public class AvroBigIntProducer extends BaseAvroProducer<BaseFixedWidthVector> {

  /** Instantiate an AvroBigIntProducer. */
  public AvroBigIntProducer(BigIntVector vector) {
    super(vector);
  }

  /** Protected constructor for logical types with a long representation. */
  protected AvroBigIntProducer(BaseFixedWidthVector vector) {
    super(vector);
    if (vector.getTypeWidth() != BigIntVector.TYPE_WIDTH) {
      throw new IllegalArgumentException(
          "AvroBigIntProducer requires type width = " + BigIntVector.TYPE_WIDTH);
    }
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    long value = vector.getDataBuffer().getLong(currentIndex * (long) BigIntVector.TYPE_WIDTH);
    encoder.writeLong(value);
    currentIndex++;
  }
}
