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
import java.nio.ByteBuffer;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.avro.io.Encoder;

/**
 * Producer that produces byte array values from a {@link VarBinaryVector}, writes data to an Avro
 * encoder.
 */
public class AvroBytesProducer extends BaseAvroProducer<VarBinaryVector> {

  /** Instantiate an AvroBytesProducer. */
  public AvroBytesProducer(VarBinaryVector vector) {
    super(vector);
  }

  @Override
  public void produce(Encoder encoder) throws IOException {
    // The nio ByteBuffer is created once per call, but underlying data is not copied
    long offset = vector.getStartOffset(currentIndex);
    long endOffset = vector.getEndOffset(currentIndex);
    int length = (int) (endOffset - offset);
    ByteBuffer nioBuffer = vector.getDataBuffer().nioBuffer(offset, length);
    encoder.writeBytes(nioBuffer);
    currentIndex++;
  }
}
