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

import org.apache.arrow.adapter.avro.producers.AvroIntProducer;
import org.apache.arrow.vector.TimeMilliVector;

/**
 * Producer that produces time (milliseconds) values from a {@link TimeMilliVector}, writes data to
 * an Avro encoder.
 */
public class AvroTimeMilliProducer extends AvroIntProducer {

  // Time in milliseconds stored as integer, matches Avro time-millis type

  /** Instantiate an AvroTimeMilliProducer. */
  public AvroTimeMilliProducer(TimeMilliVector vector) {
    super(vector);
  }
}
