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
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;

/**
 * A factory interface that allows configuring writer implementations for specific {@link
 * ExtensionTypeVector}, get the vector class for a given {@link ExtensionType}, and get the reader
 * implementation for a given {@link ExtensionTypeVector}.
 */
public interface ExtensionTypeFactory {

  /**
   * Returns an instance of the writer implementation for the given {@link ExtensionTypeVector}.
   *
   * @param vector the {@link ExtensionTypeVector} for which the writer implementation is to be
   *     returned.
   * @return an instance of the writer implementation for the given {@link ExtensionTypeVector}.
   */
  FieldWriter getWriterImpl(ExtensionTypeVector vector);

  /**
   * Returns the vector class for the given {@link ExtensionType}.
   *
   * @param extensionType the {@link ExtensionType} for which the vector class is to be returned.
   * @return the vector class for the given {@link ExtensionType}.
   */
  Class<? extends ExtensionTypeVector> getVectorClass(ExtensionType extensionType);

  /**
   * Returns an instance of the reader implementation for the given {@link ExtensionTypeVector}.
   *
   * @param vector the {@link ExtensionTypeVector} for which the reader implementation is to be
   *     returned.
   * @return an instance of the reader implementation for the given {@link ExtensionTypeVector}.
   */
  FieldReader getReaderImpl(ExtensionTypeVector vector);

  /**
   * Returns the {@link ExtensionType} for the given {@link ExtensionHolder}.
   *
   * @param holder the {@link ExtensionHolder} for which the {@link ExtensionType} is to be
   *     returned.
   * @return the {@link ExtensionType} for the given {@link ExtensionHolder}.
   */
  ExtensionType getExtensionTypeByHolder(ExtensionHolder holder);
}
