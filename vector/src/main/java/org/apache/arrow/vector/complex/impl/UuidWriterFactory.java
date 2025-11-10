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
import org.apache.arrow.vector.UuidVector;

/**
 * Factory for creating {@link UuidWriterImpl} instances.
 *
 * <p>This factory is used to create writers for UUID extension type vectors.
 *
 * @see UuidWriterImpl
 * @see org.apache.arrow.vector.extension.UuidType
 */
public class UuidWriterFactory implements ExtensionTypeWriterFactory {

  /**
   * Creates a writer implementation for the given extension type vector.
   *
   * @param extensionTypeVector the vector to create a writer for
   * @return a {@link UuidWriterImpl} if the vector is a {@link UuidVector}, null otherwise
   */
  @Override
  public AbstractFieldWriter getWriterImpl(ExtensionTypeVector extensionTypeVector) {
    if (extensionTypeVector instanceof UuidVector) {
      return new UuidWriterImpl((UuidVector) extensionTypeVector);
    }
    return null;
  }
}
