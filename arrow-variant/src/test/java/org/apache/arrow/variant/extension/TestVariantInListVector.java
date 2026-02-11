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
package org.apache.arrow.variant.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.variant.TestVariant;
import org.apache.arrow.variant.Variant;
import org.apache.arrow.variant.holders.NullableVariantHolder;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ExtensionWriter;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVariantInListVector {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testListVectorWithVariantExtensionType() {
    final FieldType type = FieldType.nullable(VariantType.INSTANCE);
    try (ListVector inVector = new ListVector("input", allocator, type, null)) {
      Variant variant1 = TestVariant.variantString("hello");
      Variant variant2 = TestVariant.variantString("bye");

      UnionListWriter writer = inVector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startList();
      ExtensionWriter extensionWriter = writer.extension(VariantType.INSTANCE);
      extensionWriter.writeExtension(variant1);
      extensionWriter.writeExtension(variant2);
      writer.endList();
      inVector.setValueCount(1);

      ArrayList<Variant> resultSet = (ArrayList<Variant>) inVector.getObject(0);
      assertEquals(2, resultSet.size());
      assertEquals(variant1, resultSet.get(0));
      assertEquals(variant2, resultSet.get(1));
    }
  }

  @Test
  public void testListVectorReaderForVariantExtensionType() {
    try (ListVector inVector = ListVector.empty("input", allocator)) {
      Variant variant1 = TestVariant.variantString("hello");
      Variant variant2 = TestVariant.variantString("bye");

      UnionListWriter writer = inVector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startList();
      ExtensionWriter extensionWriter = writer.extension(VariantType.INSTANCE);
      extensionWriter.writeExtension(variant1);
      writer.endList();

      writer.setPosition(1);
      writer.startList();
      extensionWriter.writeExtension(variant2);
      extensionWriter.writeExtension(variant2);
      writer.endList();

      inVector.setValueCount(2);

      UnionListReader reader = inVector.getReader();
      reader.setPosition(0);
      assertTrue(reader.next());
      FieldReader variantReader = reader.reader();
      NullableVariantHolder resultHolder = new NullableVariantHolder();
      variantReader.read(resultHolder);
      assertEquals(variant1, new Variant(resultHolder));

      reader.setPosition(1);
      assertTrue(reader.next());
      variantReader = reader.reader();
      variantReader.read(resultHolder);
      assertEquals(variant2, new Variant(resultHolder));

      assertTrue(reader.next());
      variantReader = reader.reader();
      variantReader.read(resultHolder);
      assertEquals(variant2, new Variant(resultHolder));
    }
  }

  @Test
  public void testCopyFromForVariantExtensionType() {
    try (ListVector inVector = ListVector.empty("input", allocator);
        ListVector outVector = ListVector.empty("output", allocator)) {
      Variant variant1 = TestVariant.variantString("hello");
      Variant variant2 = TestVariant.variantString("bye");

      UnionListWriter writer = inVector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startList();
      ExtensionWriter extensionWriter = writer.extension(VariantType.INSTANCE);
      extensionWriter.writeExtension(variant1);
      writer.endList();

      writer.setPosition(1);
      writer.startList();
      extensionWriter.writeExtension(variant2);
      extensionWriter.writeExtension(variant2);
      writer.endList();

      inVector.setValueCount(2);

      outVector.allocateNew();
      outVector.copyFrom(0, 0, inVector);
      outVector.copyFrom(1, 1, inVector);
      outVector.setValueCount(2);

      ArrayList<Variant> resultSet0 = (ArrayList<Variant>) outVector.getObject(0);
      assertEquals(1, resultSet0.size());
      assertEquals(variant1, resultSet0.get(0));

      ArrayList<Variant> resultSet1 = (ArrayList<Variant>) outVector.getObject(1);
      assertEquals(2, resultSet1.size());
      assertEquals(variant2, resultSet1.get(0));
      assertEquals(variant2, resultSet1.get(1));
    }
  }

  @Test
  public void testCopyValueSafeForVariantExtensionType() {
    try (ListVector inVector = ListVector.empty("input", allocator)) {
      Variant variant1 = TestVariant.variantString("hello");
      Variant variant2 = TestVariant.variantString("bye");

      UnionListWriter writer = inVector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startList();
      ExtensionWriter extensionWriter = writer.extension(VariantType.INSTANCE);
      extensionWriter.writeExtension(variant1);
      writer.endList();

      writer.setPosition(1);
      writer.startList();
      extensionWriter.writeExtension(variant2);
      extensionWriter.writeExtension(variant2);
      writer.endList();

      inVector.setValueCount(2);

      try (ListVector outVector = (ListVector) inVector.getTransferPair(allocator).getTo()) {
        TransferPair tp = inVector.makeTransferPair(outVector);
        tp.copyValueSafe(0, 0);
        tp.copyValueSafe(1, 1);
        outVector.setValueCount(2);

        ArrayList<Variant> resultSet0 = (ArrayList<Variant>) outVector.getObject(0);
        assertEquals(1, resultSet0.size());
        assertEquals(variant1, resultSet0.get(0));

        ArrayList<Variant> resultSet1 = (ArrayList<Variant>) outVector.getObject(1);
        assertEquals(2, resultSet1.size());
        assertEquals(variant2, resultSet1.get(0));
        assertEquals(variant2, resultSet1.get(1));
      }
    }
  }
}
