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

import org.apache.arrow.vector.FieldVector;

/**
 * Base class for avro producers.
 *
 * @param <T> vector type.
 */
public abstract class BaseAvroProducer<T extends FieldVector> implements Producer<T> {

  protected T vector;
  protected int currentIndex;

  /**
   * Constructs a base avro consumer.
   *
   * @param vector the vector to consume.
   */
  protected BaseAvroProducer(T vector) {
    this.vector = vector;
  }

  @Override
  public void skipNull() {
    currentIndex++;
  }

  @Override
  public void setPosition(int index) {
    currentIndex = index;
  }

  @Override
  public boolean resetValueVector(T vector) {
    this.vector = vector;
    this.currentIndex = 0;
    return true;
  }

  @Override
  public T getVector() {
    return vector;
  }
}
