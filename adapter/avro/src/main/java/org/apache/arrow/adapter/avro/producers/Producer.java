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
import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Encoder;

/**
 * Interface that is used to produce values to avro encoder.
 *
 * @param <T> The vector within producer or its delegate, used for partially produce purpose.
 */
public interface Producer<T extends FieldVector> {

  /**
   * Produce a specific type value from the vector and write it to avro encoder.
   *
   * @param encoder avro encoder to write data
   * @throws IOException on error
   */
  void produce(Encoder encoder) throws IOException;

  /** Skip null value in the vector by setting reader position + 1. */
  void skipNull();

  /** Set the position to read value from vector. */
  void setPosition(int index);

  /** Reset the vector within producer. */
  void resetValueVector(T vector);

  /** Get the vector within the producer. */
  T getVector();
}
