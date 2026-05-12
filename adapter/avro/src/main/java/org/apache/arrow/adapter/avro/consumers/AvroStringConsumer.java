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
package org.apache.arrow.adapter.avro.consumers;

import java.io.IOException;
import org.apache.arrow.vector.VarCharVector;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

/**
 * Consumer which consume string type values from avro decoder. Write the data to {@link
 * VarCharVector}.
 */
public class AvroStringConsumer extends BaseAvroConsumer<VarCharVector> {

  private Utf8 cachedUtf8;

  /** Instantiate a AvroStringConsumer. */
  public AvroStringConsumer(VarCharVector vector) {
    super(vector);
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    cachedUtf8 = decoder.readString(cachedUtf8);
    vector.setSafe(currentIndex++, cachedUtf8.getBytes(), 0, cachedUtf8.getByteLength());
  }
}
