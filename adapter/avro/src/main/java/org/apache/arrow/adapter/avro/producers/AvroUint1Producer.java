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
import org.apache.arrow.vector.UInt1Vector;
import org.apache.avro.io.Encoder;

/** Producer that produces int values from a {@link UInt1Vector}, writes data to an avro encoder. */
public class AvroUint1Producer extends BaseAvroProducer<UInt1Vector> {

  /** Instantiate an AvroUint1Producer. */
  public AvroUint1Producer(UInt1Vector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    byte unsigned = vector.getDataBuffer().getByte(currentIndex * (long) UInt1Vector.TYPE_WIDTH);
    int unsignedInt = Byte.toUnsignedInt(unsigned);
    encoder.writeInt(unsignedInt);
    currentIndex++;
  }
}
