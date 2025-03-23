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
import java.util.List;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.avro.io.Encoder;

abstract class BaseUnionProducer<T extends FieldVector> extends BaseAvroProducer<T> {

  // Logic is substantially the same for union and dense union, just dense union resolves offsets
  // For methods not available on FieldVector some calls are delegate to the  child class

  private final Producer<?>[] delegates;
  private final UnionMode unionMode;
  private final int nullTypeIndex;

  protected abstract int getCurrentTypeIndex();

  public BaseUnionProducer(T vector, Producer<?>[] delegates) {
    super(vector);
    this.delegates = delegates;
    if (vector.getMinorType() == Types.MinorType.DENSEUNION) {
      this.unionMode = UnionMode.Dense;
    } else {
      this.unionMode = UnionMode.Sparse;
    }
    this.nullTypeIndex = findNullTypeIndex(vector.getChildrenFromFields());
  }

  protected int findNullTypeIndex(List<FieldVector> childVectors) {
    for (int i = 0; i < childVectors.size(); i++) {
      if (childVectors.get(i).getMinorType() == Types.MinorType.NULL) {
        return i;
      }
    }
    // For nullable unions with no explicit null type, a null type is appended to the schema
    return childVectors.size();
  }

  @Override
  public void produce(Encoder encoder) throws IOException {

    if (vector.isNull(currentIndex)) {
      encoder.writeInt(nullTypeIndex);
      encoder.writeNull();
    } else {

      int typeIndex = getCurrentTypeIndex();
      int typeVectorIndex;

      if (unionMode == UnionMode.Dense) {
        typeVectorIndex = vector.getOffsetBuffer().getInt(currentIndex * (long) Integer.BYTES);
      } else {
        typeVectorIndex = currentIndex;
      }

      FieldVector typeVector = vector.getChildrenFromFields().get(typeIndex);

      if (typeVector.isNull(typeVectorIndex)) {
        encoder.writeInt(nullTypeIndex);
        encoder.writeNull();
      } else {
        Producer<?> delegate = delegates[typeIndex];
        encoder.writeInt(typeIndex);
        delegate.setPosition(typeVectorIndex);
        delegate.produce(encoder);
      }
    }

    currentIndex++;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void resetValueVector(T vector) {
    for (int i = 0; i < delegates.length; i++) {
      Producer<FieldVector> delegate = (Producer<FieldVector>) delegates[i];
      delegate.resetValueVector(vector.getChildrenFromFields().get(i));
    }
  }
}
