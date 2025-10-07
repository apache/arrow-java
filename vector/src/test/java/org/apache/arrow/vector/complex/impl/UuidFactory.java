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
import org.apache.arrow.vector.holder.UuidHolder;
import org.apache.arrow.vector.holders.ExtensionHolder;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.UuidType;

public class UuidFactory implements ExtensionTypeFactory {

  @Override
  public AbstractFieldWriter getWriterImpl(ExtensionTypeVector extensionTypeVector) {
    if (extensionTypeVector instanceof UuidVector) {
      return new UuidWriterImpl((UuidVector) extensionTypeVector);
    }
    return null;
  }

  @Override
  public Class<? extends ExtensionTypeVector> getVectorClass(ExtensionType extensionType) {
    if (extensionType instanceof UuidType) {
      return UuidVector.class;
    }
    throw new UnsupportedOperationException("Unsupported extension type " + extensionType);
  }

  @Override
  public ExtensionType getExtensionTypeByHolder(ExtensionHolder holder) {
    if (holder instanceof UuidHolder) {
      return new UuidType();
    }
    return null;
  }

  @Override
  public AbstractFieldReader getReaderImpl(ExtensionTypeVector extensionTypeVector) {
    if (extensionTypeVector instanceof UuidVector) {
      return new UuidReaderImpl((UuidVector) extensionTypeVector);
    }
    return null;
  }
}
