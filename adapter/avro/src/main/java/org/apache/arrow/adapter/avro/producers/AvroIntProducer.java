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
import org.apache.arrow.vector.IntVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces int values from an {@link IntVector}, writes data to an avro encoder.
 *
 * <p>Logical types are also supported, for vectors derived from {@link BaseFixedWidthVector} where
 * the internal representation matches IntVector and requires no conversion.
 */
public class AvroIntProducer extends BaseAvroProducer<BaseFixedWidthVector> {

  /** Instantiate an AvroIntConsumer. */
  public AvroIntProducer(IntVector vector) {
    super(vector);
  }

  /** Protected constructor for a logical types with an integer representation. */
  protected AvroIntProducer(BaseFixedWidthVector vector) {
    super(vector);
    if (vector.getTypeWidth() != IntVector.TYPE_WIDTH) {
      throw new IllegalArgumentException(
          "AvroIntProducer requires type width = " + IntVector.TYPE_WIDTH);
    }
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    int value = vector.getDataBuffer().getInt(currentIndex * (long) IntVector.TYPE_WIDTH);
    encoder.writeInt(value);
    currentIndex++;
  }
}
