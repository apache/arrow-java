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
package org.apache.arrow.vector.complex.impl;

import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.complex.writer.FieldWriter;

/**
 * A factory interface for creating instances of {@link ExtensionTypeWriter}. This factory allows
 * configuring writer implementations for specific {@link ExtensionTypeVector}.
 *
 * @param <T> the type of writer implementation for a specific {@link ExtensionTypeVector}.
 */
public interface ExtensionTypeWriterFactory<T extends FieldWriter> {

  /**
   * Returns an instance of the writer implementation for the given {@link ExtensionTypeVector}.
   *
   * @param vector the {@link ExtensionTypeVector} for which the writer implementation is to be
   *     returned.
   * @return an instance of the writer implementation for the given {@link ExtensionTypeVector}.
   */
  T getWriterImpl(ExtensionTypeVector vector);
}
