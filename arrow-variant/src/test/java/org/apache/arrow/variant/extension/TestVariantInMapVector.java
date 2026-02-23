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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.variant.TestVariant;
import org.apache.arrow.variant.Variant;
import org.apache.arrow.variant.holders.NullableVariantHolder;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVariantInMapVector {

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
  public void testMapVectorWithVariantExtensionType() {
    Variant variant1 = TestVariant.variantString("hello");
    Variant variant2 = TestVariant.variantString("world");
    try (final MapVector inVector = MapVector.empty("map", allocator, false)) {
      inVector.allocateNew();
      UnionMapWriter writer = inVector.getWriter();
      writer.setPosition(0);

      writer.startMap();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(0);
      writer.value().extension(VariantType.INSTANCE).writeExtension(variant1, VariantType.INSTANCE);
      writer.endEntry();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(1);
      writer.value().extension(VariantType.INSTANCE).writeExtension(variant2, VariantType.INSTANCE);
      writer.endEntry();
      writer.endMap();

      writer.setValueCount(1);

      UnionMapReader mapReader = inVector.getReader();
      mapReader.setPosition(0);
      mapReader.next();
      FieldReader variantReader = mapReader.value();
      NullableVariantHolder holder = new NullableVariantHolder();
      variantReader.read(holder);
      assertEquals(variant1, new Variant(holder));

      mapReader.next();
      variantReader = mapReader.value();
      variantReader.read(holder);
      assertEquals(variant2, new Variant(holder));
    }
  }

  @Test
  public void testCopyFromForVariantExtensionType() {
    Variant variant1 = TestVariant.variantString("hello");
    Variant variant2 = TestVariant.variantString("world");
    try (final MapVector inVector = MapVector.empty("in", allocator, false);
        final MapVector outVector = MapVector.empty("out", allocator, false)) {
      inVector.allocateNew();
      UnionMapWriter writer = inVector.getWriter();
      writer.setPosition(0);

      writer.startMap();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(0);
      writer.value().extension(VariantType.INSTANCE).writeExtension(variant1, VariantType.INSTANCE);
      writer.endEntry();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(1);
      writer.value().extension(VariantType.INSTANCE).writeExtension(variant2, VariantType.INSTANCE);
      writer.endEntry();
      writer.endMap();

      writer.setValueCount(1);
      outVector.allocateNew();
      outVector.copyFrom(0, 0, inVector);
      outVector.setValueCount(1);

      UnionMapReader mapReader = outVector.getReader();
      mapReader.setPosition(0);
      mapReader.next();
      FieldReader variantReader = mapReader.value();
      NullableVariantHolder holder = new NullableVariantHolder();
      variantReader.read(holder);
      assertEquals(variant1, new Variant(holder));

      mapReader.next();
      variantReader = mapReader.value();
      variantReader.read(holder);
      assertEquals(variant2, new Variant(holder));
    }
  }
}
