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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDenseUnionWriterNPE {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }

  @Test
  public void testListOfDenseUnionWriterNPE() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.addOrGetVector(FieldType.nullable(Types.MinorType.DENSEUNION.getType()));
      UnionListWriter listWriter = listVector.getWriter();

      listWriter.startList();
      listWriter.endList();
    }
  }

  @Test
  public void testListOfDenseUnionWriterWithData() {
    try (ListVector listVector = ListVector.empty("list", allocator)) {
      listVector.addOrGetVector(FieldType.nullable(Types.MinorType.DENSEUNION.getType()));

      UnionListWriter listWriter = listVector.getWriter();
      listWriter.startList();
      listWriter.writeInt(100);
      listWriter.writeBigInt(200L);
      listWriter.endList();

      listWriter.startList();
      listWriter.writeFloat4(3.14f);
      listWriter.endList();

      listVector.setValueCount(2);

      assertEquals(2, listVector.getValueCount());

      List<?> value0 = (List<?>) listVector.getObject(0);
      List<?> value1 = (List<?>) listVector.getObject(1);

      assertEquals(2, value0.size());
      assertEquals(100, value0.get(0));
      assertEquals(200L, value0.get(1));

      assertEquals(1, value1.size());
      assertEquals(3.14f, value1.get(0));
    }
  }
}
