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
package org.apache.arrow.c;

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.UuidVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.extension.JsonType;
import org.apache.arrow.vector.extension.OpaqueType;
import org.apache.arrow.vector.extension.OpaqueVector;
import org.apache.arrow.vector.extension.UuidType;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableLargeVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class RoundtripTest {
  private static final String EMPTY_SCHEMA_PATH = "";
  private RootAllocator allocator = null;
  private BufferAllocator childAllocator = null;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    childAllocator = allocator.newChildAllocator("child", 0, Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    childAllocator.close();
    allocator.close();
  }

  FieldVector vectorRoundtrip(FieldVector vector) {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

      // Producer creates structures from existing memory pointers
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
          ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
        // Producer exports vector into the C Data Interface structures
        Data.exportVector(allocator, vector, null, arrowArray, arrowSchema);
      }

      // Consumer imports vector
      FieldVector imported =
          Data.importVector(childAllocator, consumerArrowArray, consumerArrowSchema, null);
      if (!(imported instanceof NullVector)) {
        assertEquals(childAllocator, imported.getAllocator());
      }

      // Check that transfers work
      TransferPair pair = imported.getTransferPair(allocator);
      pair.transfer();
      return (FieldVector) pair.getTo();
    }
  }

  VectorSchemaRoot vectorSchemaRootRoundtrip(VectorSchemaRoot root) {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

      // Producer creates structures from existing memory pointers
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
          ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
        // Producer exports vector into the C Data Interface structures
        Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);
      }

      // Consumer imports vector
      return Data.importVectorSchemaRoot(allocator, consumerArrowArray, consumerArrowSchema, null);
    }
  }

  boolean roundtrip(FieldVector vector, Class<?> clazz) {
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    List<Integer> orgRefCnts =
        fieldBuffers.stream().map(buf -> buf.refCnt()).collect(Collectors.toList());
    long orgMemorySize = allocator.getAllocatedMemory();

    boolean result = false;
    try (ValueVector imported = vectorRoundtrip(vector)) {
      assertTrue(
          clazz.isInstance(imported),
          String.format("expected %s but was %s", clazz, imported.getClass()));
      result = VectorEqualsVisitor.vectorEquals(vector, imported);

      if (imported instanceof BaseVariableWidthVector
          || imported instanceof BaseLargeVariableWidthVector) {
        ArrowBuf offsetBuffer = imported.getOffsetBuffer();
        assertTrue(offsetBuffer.capacity() > 0);
        assertEquals(0, offsetBuffer.getInt(0));
      }
    }

    // Check that the ref counts of the buffers are the same after the roundtrip
    IntStream.range(0, orgRefCnts.size())
        .forEach(
            i -> {
              ArrowBuf buf = fieldBuffers.get(i);
              assertEquals(buf.refCnt(), orgRefCnts.get(i));
            });

    assertEquals(orgMemorySize, allocator.getAllocatedMemory());

    return result;
  }

  @Test
  public void testBitVector() {
    BitVector imported;

    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024);
      vector.setValueCount(1024);

      // Put and set a few values
      vector.set(0, 1);
      vector.set(1, 0);
      vector.set(100, 0);
      vector.set(1022, 1);

      vector.setValueCount(1024);

      imported = (BitVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }

    assertEquals(1, imported.get(0));
    assertEquals(0, imported.get(1));
    assertEquals(0, imported.get(100));
    assertEquals(1, imported.get(1022));
    assertEquals(1020, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testIntVector() {
    IntVector imported;
    try (final IntVector vector = new IntVector("v", allocator)) {
      setVector(vector, 1, 2, 3, null);
      imported = (IntVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testBigIntVector() {
    BigIntVector imported;
    try (final BigIntVector vector = new BigIntVector("v", allocator)) {
      setVector(vector, 1L, 2L, 3L, null);
      imported = (BigIntVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testDateDayVector() {
    DateDayVector imported;
    try (final DateDayVector vector = new DateDayVector("v", allocator)) {
      setVector(vector, 1, 2, 3, null);
      imported = (DateDayVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testDateMilliVector() {
    DateMilliVector imported;
    try (final DateMilliVector vector = new DateMilliVector("v", allocator)) {
      setVector(vector, 1L, 2L, 3L, null);
      imported = (DateMilliVector) vectorRoundtrip(vector);
      assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported));
    }
    assertEquals(1, imported.get(0));
    assertEquals(2, imported.get(1));
    assertEquals(3, imported.get(2));
    assertEquals(4, imported.getValueCount());
    assertEquals(1, imported.getNullCount());
    imported.close();
  }

  @Test
  public void testDecimalVector() {
    try (final DecimalVector vector = new DecimalVector("v", allocator, 1, 1)) {
      setVector(vector, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, DecimalVector.class));
    }
  }

  @Test
  public void testDurationVector() {
    for (TimeUnit unit : TimeUnit.values()) {
      final FieldType fieldType = FieldType.nullable(new ArrowType.Duration(unit));
      try (final DurationVector vector = new DurationVector("v", fieldType, allocator)) {
        setVector(vector, 1L, 2L, 3L, null);
        assertTrue(roundtrip(vector, DurationVector.class));
      }
    }
  }

  @Test
  public void testZeroVectorEquals() {
    try (final ZeroVector vector = new ZeroVector()) {
      // A ZeroVector is imported as a NullVector
      assertTrue(roundtrip(vector, NullVector.class));
    }
  }

  @Test
  public void testFixedSizeBinaryVector() {
    try (final FixedSizeBinaryVector vector = new FixedSizeBinaryVector("v", allocator, 2)) {
      setVector(vector, new byte[] {0b0000, 0b0001}, new byte[] {0b0010, 0b0011});
      assertTrue(roundtrip(vector, FixedSizeBinaryVector.class));
    }
  }

  @Test
  public void testFloat2Vector() {
    try (final Float2Vector vector = new Float2Vector("v", allocator)) {
      setVector(vector, 0.1f, 0.2f, 0.3f, null);
      assertTrue(roundtrip(vector, Float2Vector.class));
    }
  }

  @Test
  public void testFloat4Vector() {
    try (final Float4Vector vector = new Float4Vector("v", allocator)) {
      setVector(vector, 0.1f, 0.2f, 0.3f, null);
      assertTrue(roundtrip(vector, Float4Vector.class));
    }
  }

  @Test
  public void testFloat8Vector() {
    try (final Float8Vector vector = new Float8Vector("v", allocator)) {
      setVector(vector, 0.1d, 0.2d, 0.3d, null);
      assertTrue(roundtrip(vector, Float8Vector.class));
    }
  }

  @Test
  public void testIntervalDayVector() {
    try (final IntervalDayVector vector = new IntervalDayVector("v", allocator)) {
      IntervalDayHolder value = new IntervalDayHolder();
      value.days = 5;
      value.milliseconds = 100;
      setVector(vector, value, null);
      assertTrue(roundtrip(vector, IntervalDayVector.class));
    }
  }

  @Test
  public void testIntervalYearVector() {
    try (final IntervalYearVector vector = new IntervalYearVector("v", allocator)) {
      setVector(vector, 1990, 2000, 2010, 2020, null);
      assertTrue(roundtrip(vector, IntervalYearVector.class));
    }
  }

  @Test
  public void testSmallIntVector() {
    try (final SmallIntVector vector = new SmallIntVector("v", allocator)) {
      setVector(vector, (short) 0, (short) 256, null);
      assertTrue(roundtrip(vector, SmallIntVector.class));
    }
  }

  @Test
  public void testTimeMicroVector() {
    try (final TimeMicroVector vector = new TimeMicroVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeMicroVector.class));
    }
  }

  @Test
  public void testTimeMilliVector() {
    try (final TimeMilliVector vector = new TimeMilliVector("v", allocator)) {
      setVector(vector, 0, 1, 2, 3, null);
      assertTrue(roundtrip(vector, TimeMilliVector.class));
    }
  }

  @Test
  public void testTimeNanoVector() {
    try (final TimeNanoVector vector = new TimeNanoVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeNanoVector.class));
    }
  }

  @Test
  public void testTimeSecVector() {
    try (final TimeSecVector vector = new TimeSecVector("v", allocator)) {
      setVector(vector, 0, 1, 2, 3, null);
      assertTrue(roundtrip(vector, TimeSecVector.class));
    }
  }

  @Test
  public void testTimeStampMicroTZVector() {
    try (final TimeStampMicroTZVector vector = new TimeStampMicroTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMicroTZVector.class));
    }
  }

  @Test
  public void testTimeStampMicroVector() {
    try (final TimeStampMicroVector vector = new TimeStampMicroVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMicroVector.class));
    }
  }

  @Test
  public void testTimeStampMilliTZVector() {
    try (final TimeStampMilliTZVector vector = new TimeStampMilliTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMilliTZVector.class));
    }
  }

  @Test
  public void testTimeStampMilliVector() {
    try (final TimeStampMilliVector vector = new TimeStampMilliVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampMilliVector.class));
    }
  }

  @Test
  public void testTimeTimeStampNanoTZVector() {
    try (final TimeStampNanoTZVector vector = new TimeStampNanoTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampNanoTZVector.class));
    }
  }

  @Test
  public void testTimeStampNanoVector() {
    try (final TimeStampNanoVector vector = new TimeStampNanoVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampNanoVector.class));
    }
  }

  @Test
  public void testTimeStampSecTZVector() {
    try (final TimeStampSecTZVector vector = new TimeStampSecTZVector("v", allocator, "UTC")) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampSecTZVector.class));
    }
  }

  @Test
  public void testTimeStampSecVector() {
    try (final TimeStampSecVector vector = new TimeStampSecVector("v", allocator)) {
      setVector(vector, 0L, 1L, 2L, 3L, null);
      assertTrue(roundtrip(vector, TimeStampSecVector.class));
    }
  }

  @Test
  public void testTinyIntVector() {
    try (final TinyIntVector vector = new TinyIntVector("v", allocator)) {
      setVector(vector, (byte) 0, (byte) 1, null);
      assertTrue(roundtrip(vector, TinyIntVector.class));
    }
  }

  @Test
  public void testUInt1Vector() {
    try (final UInt1Vector vector = new UInt1Vector("v", allocator)) {
      setVector(vector, (byte) 0, (byte) 1, null);
      assertTrue(roundtrip(vector, UInt1Vector.class));
    }
  }

  @Test
  public void testUInt2Vector() {
    try (final UInt2Vector vector = new UInt2Vector("v", allocator)) {
      setVector(vector, '0', '1', null);
      assertTrue(roundtrip(vector, UInt2Vector.class));
    }
  }

  @Test
  public void testUInt4Vector() {
    try (final UInt4Vector vector = new UInt4Vector("v", allocator)) {
      setVector(vector, 0, 1, null);
      assertTrue(roundtrip(vector, UInt4Vector.class));
    }
  }

  @Test
  public void testUInt8Vector() {
    try (final UInt8Vector vector = new UInt8Vector("v", allocator)) {
      setVector(vector, 0L, 1L, null);
      assertTrue(roundtrip(vector, UInt8Vector.class));
    }
  }

  @Test
  public void testNullVector() {
    try (final NullVector vector = new NullVector("v", 1024)) {
      assertTrue(roundtrip(vector, NullVector.class));
    }
  }

  @Test
  public void testVarBinaryVector() {
    try (final VarBinaryVector vector = new VarBinaryVector("v", allocator)) {
      setVector(
          vector,
          "abc".getBytes(StandardCharsets.UTF_8),
          "def".getBytes(StandardCharsets.UTF_8),
          null);
      assertTrue(roundtrip(vector, VarBinaryVector.class));
    }
  }

  @Test
  public void testViewVector() {
    // ViewVarCharVector with short strings
    try (final ViewVarCharVector vector = new ViewVarCharVector("v1", allocator)) {
      setVector(
          vector,
          "abc".getBytes(StandardCharsets.UTF_8),
          "def".getBytes(StandardCharsets.UTF_8),
          null);
      assertTrue(roundtrip(vector, ViewVarCharVector.class));
    }

    // ViewVarCharVector with long strings
    try (final ViewVarCharVector vector = new ViewVarCharVector("v2", allocator)) {
      setVector(
          vector,
          "01234567890123".getBytes(StandardCharsets.UTF_8),
          "01234567890123567".getBytes(StandardCharsets.UTF_8),
          null);
      assertTrue(roundtrip(vector, ViewVarCharVector.class));
    }

    // ViewVarBinaryVector with short values
    try (final ViewVarBinaryVector vector = new ViewVarBinaryVector("v3", allocator)) {
      setVector(
          vector,
          "abc".getBytes(StandardCharsets.UTF_8),
          "def".getBytes(StandardCharsets.UTF_8),
          null);
      assertTrue(roundtrip(vector, ViewVarBinaryVector.class));
    }

    // ViewVarBinaryVector with long values
    try (final ViewVarBinaryVector vector = new ViewVarBinaryVector("v4", allocator)) {
      setVector(
          vector,
          "01234567890123".getBytes(StandardCharsets.UTF_8),
          "01234567890123567".getBytes(StandardCharsets.UTF_8),
          null);
      assertTrue(roundtrip(vector, ViewVarBinaryVector.class));
    }

    List<byte[]> byteArrayList = new ArrayList<>();
    for (int i = 1; i <= 500; i++) {
      StringBuilder sb = new StringBuilder(i);
      for (int j = 0; j < i; j++) {
        sb.append(j); // or any other character
      }
      byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
      byteArrayList.add(bytes);
    }

    // ViewVarCharVector with short long strings with multiple data buffers
    try (final ViewVarCharVector vector = new ViewVarCharVector("v5", allocator)) {
      setVector(vector, byteArrayList.toArray(new byte[0][]));
      assertTrue(roundtrip(vector, ViewVarCharVector.class));
    }

    // ViewVarBinaryVector with short long strings with multiple data buffers
    try (final ViewVarBinaryVector vector = new ViewVarBinaryVector("v6", allocator)) {
      setVector(vector, byteArrayList.toArray(new byte[0][]));
      assertTrue(roundtrip(vector, ViewVarBinaryVector.class));
    }
  }

  @Test
  public void testVarCharVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      setVector(vector, "abc", "def", null);
      assertTrue(roundtrip(vector, VarCharVector.class));
    }
  }

  @Test
  public void testEmptyVarCharVector() {
    try (final VarCharVector vector = new VarCharVector("v", allocator)) {
      assertTrue(roundtrip(vector, VarCharVector.class));
    }
  }

  @Test
  public void testLargeVarBinaryVector() {
    try (final LargeVarBinaryVector vector = new LargeVarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableLargeVarBinaryHolder nullHolder = new NullableLargeVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableLargeVarBinaryHolder binHolder = new NullableLargeVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello world";
      try (ArrowBuf buf = allocator.buffer(16)) {
        buf.setBytes(0, str.getBytes(StandardCharsets.UTF_8));
        binHolder.start = 0;
        binHolder.end = str.length();
        binHolder.buffer = buf;
        vector.setSafe(0, binHolder);
        vector.setSafe(1, nullHolder);

        assertTrue(roundtrip(vector, LargeVarBinaryVector.class));
      }
    }
  }

  @Test
  public void testLargeVarCharVector() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      setVector(vector, "abc", "def", null);
      assertTrue(roundtrip(vector, LargeVarCharVector.class));
    }
  }

  @Test
  public void testEmptyLargeVarCharVector() {
    try (final LargeVarCharVector vector = new LargeVarCharVector("v", allocator)) {
      assertTrue(roundtrip(vector, LargeVarCharVector.class));
    }
  }

  @Test
  public void testListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      setVector(
          vector,
          Arrays.stream(new int[] {1, 2}).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] {3, 4}).boxed().collect(Collectors.toList()),
          new ArrayList<Integer>());
      assertTrue(roundtrip(vector, ListVector.class));
    }
  }

  @Test
  public void testEmptyListVector() {
    try (final ListVector vector = ListVector.empty("v", allocator)) {
      setVector(vector, new ArrayList<Integer>());
      assertTrue(roundtrip(vector, ListVector.class));
    }
  }

  @Test
  public void testLargeListVector() {
    try (final LargeListVector vector = LargeListVector.empty("v", allocator)) {
      setVector(
          vector,
          Arrays.stream(new int[] {1, 2}).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] {3, 4}).boxed().collect(Collectors.toList()),
          new ArrayList<Integer>());
      assertTrue(roundtrip(vector, LargeListVector.class));
    }
  }

  @Test
  public void testFixedSizeListVector() {
    try (final FixedSizeListVector vector = FixedSizeListVector.empty("v", 2, allocator)) {
      setVector(
          vector,
          Arrays.stream(new int[] {1, 2}).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] {3, 4}).boxed().collect(Collectors.toList()));
      assertTrue(roundtrip(vector, FixedSizeListVector.class));
    }
  }

  @Test
  public void testListViewVector() {
    try (final ListViewVector vector = ListViewVector.empty("v", allocator)) {
      setVector(
          vector,
          Arrays.stream(new int[] {1, 2}).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] {3, 4}).boxed().collect(Collectors.toList()),
          new ArrayList<Integer>());
      assertTrue(roundtrip(vector, ListViewVector.class));
    }
  }

  @Test
  public void testEmptyListViewVector() {
    try (final ListViewVector vector = ListViewVector.empty("v", allocator)) {
      setVector(vector, new ArrayList<Integer>());
      assertTrue(roundtrip(vector, ListViewVector.class));
    }
  }

  @Test
  public void testLargeListViewVector() {
    try (final LargeListViewVector vector = LargeListViewVector.empty("v", allocator)) {
      setVector(
          vector,
          Arrays.stream(new int[] {1, 2}).boxed().collect(Collectors.toList()),
          Arrays.stream(new int[] {3, 4}).boxed().collect(Collectors.toList()),
          new ArrayList<Integer>());
      assertTrue(roundtrip(vector, LargeListViewVector.class));
    }
  }

  @Test
  public void testEmptyLargeListViewVector() {
    try (final LargeListViewVector vector = LargeListViewVector.empty("v", allocator)) {
      setVector(vector, new ArrayList<Integer>());
      assertTrue(roundtrip(vector, LargeListViewVector.class));
    }
  }

  @Test
  public void testMapVector() {
    try (final MapVector vector = MapVector.empty("v", allocator, false)) {
      writeTestMaps(vector);
      assertTrue(roundtrip(vector, MapVector.class));
    }
  }

  @Test
  public void testUnionVector() {
    try (UnionVector vector = UnionVector.empty("v", allocator)) {
      writeTestUnion(vector);
      assertTrue(roundtrip(vector, UnionVector.class));
    }
  }

  @Test
  public void testStructVector() {
    try (final StructVector vector = StructVector.empty("v", allocator)) {
      Map<String, List<Integer>> data = new HashMap<>();
      data.put("col_1", Arrays.stream(new int[] {1, 2}).boxed().collect(Collectors.toList()));
      data.put("col_2", Arrays.stream(new int[] {3, 4}).boxed().collect(Collectors.toList()));
      setVector(vector, data);
      assertTrue(roundtrip(vector, StructVector.class));
    }
  }

  @Test
  public void testRunEndEncodedVector() {
    try (final RunEndEncodedVector vector = RunEndEncodedVector.empty("v", allocator)) {
      setVector(vector, List.of(1, 3), List.of(1, 2));
      assertTrue(roundtrip(vector, RunEndEncodedVector.class));
    }
  }

  @Test
  public void testEmptyRunEndEncodedVector() {
    try (final RunEndEncodedVector vector = RunEndEncodedVector.empty("v", allocator)) {
      setVector(vector, List.of(), List.of());
      assertTrue(roundtrip(vector, RunEndEncodedVector.class));
    }
  }

  @Test
  public void testExtensionTypeVector() {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("a", UuidType.INSTANCE)));
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      // Fill with data
      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();
      UuidVector vector = (UuidVector) root.getVector("a");
      vector.setValueCount(2);
      vector.set(0, u1);
      vector.set(1, u2);
      root.setRowCount(2);

      // Roundtrip (export + import)
      VectorSchemaRoot importedRoot = vectorSchemaRootRoundtrip(root);

      // Verify correctness
      assertEquals(root.getSchema(), importedRoot.getSchema());

      final Field field = importedRoot.getSchema().getFields().get(0);
      assertEquals(
          field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_NAME),
          UuidType.INSTANCE.extensionName());
      assertEquals(
          field.getMetadata().get(ExtensionType.EXTENSION_METADATA_KEY_METADATA),
          UuidType.INSTANCE.serialize());

      final UuidVector deserialized = (UuidVector) importedRoot.getFieldVectors().get(0);
      assertEquals(vector.getValueCount(), deserialized.getValueCount());
      for (int i = 0; i < vector.getValueCount(); i++) {
        assertEquals(vector.isNull(i), deserialized.isNull(i));
        if (!vector.isNull(i)) {
          assertEquals(vector.getObject(i), deserialized.getObject(i));
        }
      }

      importedRoot.close();
    }
  }

  static Stream<Arguments> viewExtensionCases() {
    byte[][] inlined = {
      "{}".getBytes(StandardCharsets.UTF_8), null, "null".getBytes(StandardCharsets.UTF_8)
    };
    byte[][] external = {
      "{\"message\":\"a JSON value longer than twelve bytes\"}".getBytes(StandardCharsets.UTF_8),
      null
    };
    return Stream.of(
            new JsonType(ArrowType.Utf8View.INSTANCE),
            new OpaqueType(ArrowType.Utf8View.INSTANCE, "json", "test"),
            new OpaqueType(ArrowType.BinaryView.INSTANCE, "binary", "test"))
        .flatMap(
            type ->
                Stream.of(
                    Arguments.of(type, new byte[0][], 3),
                    Arguments.of(type, inlined, 3),
                    Arguments.of(type, external, 4)));
  }

  @ParameterizedTest
  @MethodSource("viewExtensionCases")
  public void testViewExtensionTypeVector(ExtensionType type, byte[][] values, int bufferCount) {
    ExtensionType previous = ExtensionTypeRegistry.lookup(type.extensionName());
    ExtensionTypeRegistry.register(type);
    Schema schema = new Schema(Collections.singletonList(Field.nullable("a", type)));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      ExtensionTypeVector<?> vector = (ExtensionTypeVector<?>) root.getVector("a");
      setVector((VariableWidthFieldVector) vector.getUnderlyingVector(), values);
      root.setRowCount(values.length);

      try (ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray arrowArray = ArrowArray.allocateNew(allocator)) {
        Data.exportVector(allocator, vector, null, arrowArray, arrowSchema);
        assertEquals(bufferCount, arrowArray.snapshot().n_buffers);
        try (FieldVector imported =
            Data.importVector(childAllocator, arrowArray, arrowSchema, null)) {
          assertViewExtensionVector(vector, imported);
        }
      }

      try (VectorSchemaRoot imported = vectorSchemaRootRoundtrip(root)) {
        assertEquals(root.getSchema(), imported.getSchema());
        assertEquals(root.getRowCount(), imported.getRowCount());
        assertViewExtensionVector(vector, imported.getVector("a"));
      }
    } finally {
      ExtensionTypeRegistry.unregister(type);
      if (previous != null) {
        ExtensionTypeRegistry.register(previous);
      }
    }
  }

  private void assertViewExtensionVector(ExtensionTypeVector<?> expected, FieldVector actual) {
    assertEquals(expected.getField(), actual.getField());
    ExtensionTypeVector<?> imported = assertInstanceOf(ExtensionTypeVector.class, actual);
    assertTrue(
        VectorEqualsVisitor.vectorEquals(
            expected.getUnderlyingVector(), imported.getUnderlyingVector()));
  }

  @Test
  public void testVectorSchemaRoot() {
    VectorSchemaRoot imported;

    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {
      try (VectorSchemaRoot vsr = createTestVSR()) {
        // Producer creates structures from existing memory pointers
        try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
            ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
          // Producer exports vector into the C Data Interface structures
          Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        }
      }
      // Consumer imports vector
      imported =
          Data.importVectorSchemaRoot(allocator, consumerArrowArray, consumerArrowSchema, null);
    }

    // Ensure that imported VectorSchemaRoot is valid even after C Data Interface
    // structures are closed
    try (VectorSchemaRoot original = createTestVSR()) {
      assertTrue(imported.equals(original));
    }
    imported.close();
  }

  /**
   * Tests exporting Table and importing back to VSR. Importing back to Table is not supported at
   * present.
   */
  @Test
  public void testTable() {
    VectorSchemaRoot imported;

    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {
      try (VectorSchemaRoot vsr = createTestVSR();
          Table table = new Table(vsr)) {
        // Producer creates structures from existing memory pointers
        try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
            ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
          // Producer exports vector into the C Data Interface structures
          Data.exportTable(allocator, table, null, arrowArray, arrowSchema);
        }
      }
      // Consumer imports vector
      imported =
          Data.importVectorSchemaRoot(allocator, consumerArrowArray, consumerArrowSchema, null);
    }

    // Ensure that imported VectorSchemaRoot is valid even after C Data Interface
    // structures are closed
    try (VectorSchemaRoot original = createTestVSR()) {
      assertTrue(imported.equals(original));
    }
    imported.close();
  }

  @Test
  public void testVectorSchemaRootWithDuplicatedFieldNames() {
    VectorSchemaRoot imported;

    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

      try (VectorSchemaRoot testVSR1 = createTestVSR();
          VectorSchemaRoot testVSR2 = createTestVSR()) {
        // Merge two VSRs to produce duplicated field names
        final VectorSchemaRoot vsr =
            new VectorSchemaRoot(
                Stream.concat(
                        testVSR1.getFieldVectors().stream(), testVSR2.getFieldVectors().stream())
                    .collect(Collectors.toList()));
        // Producer creates structures from existing memory pointers
        try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
            ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
          // Producer exports vector into the C Data Interface structures
          Data.exportVectorSchemaRoot(allocator, vsr, null, arrowArray, arrowSchema);
        }
      }
      // Consumer imports vector
      imported =
          Data.importVectorSchemaRoot(allocator, consumerArrowArray, consumerArrowSchema, null);
    }

    // Ensure that imported VectorSchemaRoot is valid even after C Data Interface
    // structures are closed
    try (VectorSchemaRoot testVSR1 = createTestVSR();
        VectorSchemaRoot testVSR2 = createTestVSR()) {
      final VectorSchemaRoot original =
          new VectorSchemaRoot(
              Stream.concat(
                      testVSR1.getFieldVectors().stream(), testVSR2.getFieldVectors().stream())
                  .collect(Collectors.toList()));
      assertTrue(imported.equals(original));
    }
    imported.close();
  }

  @Test
  public void testSchema() {
    Schema schema = createSchema();
    // Consumer allocates empty ArrowSchema
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator)) {
      // Producer fills the schema with data
      exportSchema(schema, consumerArrowSchema);

      // Consumer imports schema
      Schema importedSchema = Data.importSchema(allocator, consumerArrowSchema, null);
      assertEquals(schema.toJson(), importedSchema.toJson());
    }
  }

  @Test
  public void testSchemaStructReuse() {
    Schema schema = createSchema();
    // Consumer allocates empty ArrowSchema
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator)) {
      // Producer fills the schema with data
      exportSchema(schema, consumerArrowSchema);

      // Consumer imports schema
      Schema importedSchema = Data.importSchema(allocator, consumerArrowSchema, null, false);
      assertEquals(schema.toJson(), importedSchema.toJson());

      // Imported struct should be released but not closed
      assertEquals(0, consumerArrowSchema.snapshot().release);
      assertNotEquals(0, consumerArrowSchema.memoryAddress());

      // Export and import again
      exportSchema(schema, consumerArrowSchema);
      importedSchema = Data.importSchema(allocator, consumerArrowSchema, null, false);
      assertEquals(schema.toJson(), importedSchema.toJson());
      assertEquals(0, consumerArrowSchema.snapshot().release);
      assertNotEquals(0, consumerArrowSchema.memoryAddress());
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, Long.MIN_VALUE, Integer.MAX_VALUE + 1L, (1L << 32) - 100})
  public void testImportRejectsInvalidLength(long length) {
    try (IntVector vector = new IntVector("ints", allocator)) {
      setVector(vector, 1, 2, 3);
      assertLengthRejected(vector, "ints", length, array -> setLength(array, length));
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, Long.MIN_VALUE, Integer.MAX_VALUE + 1L, (1L << 32) - 100})
  public void testImportRejectsInvalidStructLength(long length) {
    try (StructVector vector = createStructWithTwoChildren()) {
      assertLengthRejected(vector, "struct", length, array -> setLength(array, length));
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, Long.MIN_VALUE, Integer.MAX_VALUE + 1L, (1L << 32) - 100})
  public void testImportRejectsInvalidChildLength(long length) {
    try (StructVector vector = createStructWithTwoChildren()) {
      assertLengthRejected(
          vector,
          "second",
          length,
          array -> {
            long[] children = NativeUtil.toJavaArray(array.snapshot().children, 2);
            setLength(ArrowArray.wrap(children[1]), length);
          });
    }
  }

  private StructVector createStructWithTwoChildren() {
    StructVector vector = StructVector.empty("struct", allocator);
    FieldType type = FieldType.nullable(new ArrowType.Int(32, true));
    IntVector first = vector.addOrGet("first", type, IntVector.class);
    IntVector second = vector.addOrGet("second", type, IntVector.class);
    vector.allocateNew();
    for (int i = 0; i < 3; i++) {
      first.setSafe(i, i);
      second.setSafe(i, i);
      vector.setIndexDefined(i);
    }
    vector.setValueCount(3);
    return vector;
  }

  private static void setLength(ArrowArray array, long length) {
    ArrowArray.Snapshot snapshot = array.snapshot();
    snapshot.length = length;
    array.save(snapshot);
  }

  /**
   * Checks that an import with an invalid length fails cleanly. Negative lengths are rejected as
   * such before anything is imported, other invalid lengths name the field they belong to.
   */
  private void assertLengthRejected(
      FieldVector vector, String expectedField, long length, Consumer<ArrowArray> corrupt) {
    if (length < 0) {
      assertImportRejected(
          vector,
          IllegalStateException.class,
          "ArrowArray struct has negative length (" + length + ")",
          corrupt);
    } else {
      assertImportRejected(vector, expectedField, corrupt);
    }
  }

  /** Exports the vector, corrupts the export and checks that the import fails cleanly. */
  private void assertImportRejected(
      FieldVector vector, String expectedField, Consumer<ArrowArray> corrupt) {
    assertImportRejected(vector, IllegalArgumentException.class, expectedField, corrupt);
  }

  private void assertImportRejected(
      FieldVector vector,
      Class<? extends RuntimeException> expectedType,
      String expectedMessage,
      Consumer<ArrowArray> corrupt) {
    try (ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator)) {
      Data.exportVector(allocator, vector, null, arrowArray, arrowSchema);
      corrupt.accept(arrowArray);
      RuntimeException e =
          assertThrows(
              expectedType, () -> Data.importVector(childAllocator, arrowArray, arrowSchema, null));
      assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
    }
    assertEquals(0, childAllocator.getAllocatedMemory());
  }

  private void exportSchema(Schema schema, ArrowSchema targetArrowSchema) {
    try (ArrowSchema arrowSchema = ArrowSchema.wrap(targetArrowSchema.memoryAddress())) {
      Data.exportSchema(allocator, schema, null, arrowSchema);
    }
  }

  private static Schema createSchema() {
    Field decimalField =
        new Field("inner1", FieldType.nullable(new ArrowType.Decimal(19, 4, 128)), null);
    Field strField = new Field("inner2", FieldType.nullable(new ArrowType.Utf8()), null);
    Field itemField =
        new Field(
            "col1",
            FieldType.nullable(new ArrowType.Struct()),
            Arrays.asList(decimalField, strField));
    Field intField = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schema = new Schema(Arrays.asList(itemField, intField));
    return schema;
  }

  @Test
  public void testImportedBufferAsNioBuffer() {
    IntVector imported;
    try (final IntVector vector = new IntVector("v", allocator)) {
      setVector(vector, 1, 2, 3, null);
      imported = (IntVector) vectorRoundtrip(vector);
    }
    ArrowBuf dataBuffer = imported.getDataBuffer();
    ByteBuffer nioBuffer = dataBuffer.nioBuffer().asReadOnlyBuffer();
    nioBuffer.order(ByteOrder.nativeOrder());
    assertEquals(1, nioBuffer.getInt(0));
    assertEquals(2, nioBuffer.getInt(1 << 2));
    assertEquals(3, nioBuffer.getInt(2 << 2));
    imported.close();
  }

  @Test
  public void testImportReleasedArray() {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {
      // Producer creates structures from existing memory pointers
      try (final NullVector vector = new NullVector()) {
        exportFieldVector(vector, consumerArrowSchema, consumerArrowArray);
      }

      // Release array structure
      consumerArrowArray.markReleased();

      // Consumer tried to imports vector but fails
      Exception e =
          assertThrows(
              IllegalStateException.class,
              () -> {
                Data.importVector(allocator, consumerArrowArray, consumerArrowSchema, null);
              });

      assertEquals("Cannot import released ArrowArray", e.getMessage());
    }
  }

  @Test
  public void testImportArrayWithNonZeroOffset() {
    try (IntVector source = new IntVector("source", allocator);
        IntVector destination = new IntVector("destination", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      setVector(source, 1, 2, null, 4, 5);
      Data.exportVector(allocator, source, null, array);
      slice(array, 1, 3);

      Data.importIntoVector(allocator, array, destination, null);

      assertEquals(3, destination.getValueCount());
      assertEquals(1, destination.getNullCount());
      assertEquals(2, destination.get(0));
      assertTrue(destination.isNull(1));
      assertEquals(4, destination.get(2));
    }
  }

  @Test
  public void testImportIntVectorWithNonZeroOffset() {
    try (IntVector source = new IntVector("source", allocator)) {
      setVector(source, rows(i -> i % 3 == 0 ? null : i, Integer[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportVarCharWithNonZeroOffset() {
    try (VarCharVector source = new VarCharVector("source", allocator)) {
      setVector(source, rows(i -> i % 4 == 1 ? null : "v".repeat(i % 5), String[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportListWithNonZeroOffset() {
    try (ListVector source = ListVector.empty("source", allocator)) {
      setVector(source, testLists());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportStructWithNonZeroOffset() {
    // The offset of a struct applies to its children, which are exported at full length
    try (StructVector source = StructVector.empty("source", allocator);
        StructVector destination = StructVector.empty("destination", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      Map<String, List<Integer>> values = new HashMap<>();
      values.put("a", Arrays.asList(1, 2, 3, 4));
      values.put("b", Arrays.asList(10, null, 30, 40));
      setVector(source, values);
      destination.initializeChildrenFromFields(source.getField().getChildren());
      Data.exportVector(allocator, source, null, array);
      slice(array, 1, 2);

      Data.importIntoVector(allocator, array, destination, null);

      assertEquals(2, destination.getValueCount());
      IntVector a = (IntVector) destination.getChild("a");
      IntVector b = (IntVector) destination.getChild("b");
      assertEquals(2, a.getValueCount());
      assertEquals(2, a.get(0));
      assertEquals(3, a.get(1));
      assertTrue(b.isNull(0));
      assertEquals(30, b.get(1));
    }
  }

  @Test
  public void testImportStructWithNullsWithNonZeroOffset() {
    try (StructVector source = StructVector.empty("source", allocator)) {
      setVector(source, testStructValues());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportStructWithChildOffsetsWithNonZeroOffset() {
    // The offset of a child adds to the offset of the struct
    try (StructVector source = StructVector.empty("source", allocator)) {
      setVector(source, testStructValues());
      for (int childOffset : new int[] {3, 8, 11}) {
        for (int offset = 0; offset < ROW_COUNT - childOffset; offset++) {
          int length = ROW_COUNT - childOffset - offset;
          try (ArrowArray array = ArrowArray.allocateNew(allocator);
              ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVector(allocator, source, null, array, schema);
            ArrowArray.Snapshot snapshot = array.snapshot();
            long[] children = NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children);
            for (long child : children) {
              slice(ArrowArray.wrap(child), childOffset, ROW_COUNT - childOffset);
            }
            slice(array, offset, length);

            try (StructVector imported =
                (StructVector) Data.importVector(allocator, array, schema, null)) {
              assertEquals(length, imported.getValueCount());
              for (int i = 0; i < length; i++) {
                String row = "child offset " + childOffset + ", offset " + offset + ", index " + i;
                assertEquals(source.isNull(offset + i), imported.isNull(i), row);
                for (String name : source.getChildFieldNames()) {
                  assertEquals(
                      source.getChild(name).getObject(childOffset + offset + i),
                      imported.getChild(name).getObject(i),
                      row + ", child " + name);
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void testImportStructWithShortChildWithNonZeroOffset() {
    try (StructVector source = StructVector.empty("source", allocator);
        StructVector destination = StructVector.empty("destination", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      setVector(source, testStructValues());
      destination.initializeChildrenFromFields(source.getField().getChildren());
      Data.exportVector(allocator, source, null, array);
      ArrowArray.Snapshot snapshot = array.snapshot();
      for (long child : NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children)) {
        slice(ArrowArray.wrap(child), 0, 13);
      }
      // The slice needs 14 rows of each child
      slice(array, 9, 5);

      Exception e =
          assertThrows(
              IllegalStateException.class,
              () -> Data.importIntoVector(allocator, array, destination, null));
      assertEquals(
          "ArrowArray struct child at position 0 has length 13, but its parent needs 14 rows",
          e.getMessage());
    }
  }

  @Test
  public void testImportVectorSchemaRootWithSlicedChildren() {
    // Arrow C++ exports record_batch->Slice(8, 2) as a struct whose children carry the offset
    try (VectorSchemaRoot source = createTestVSR();
        VectorSchemaRoot destination = VectorSchemaRoot.create(source.getSchema(), allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, source, null, array);
      ArrowArray.Snapshot snapshot = array.snapshot();
      slice(array, 0, 2);
      for (long child : NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children)) {
        slice(ArrowArray.wrap(child), 8, 2);
      }

      Data.importIntoVectorSchemaRoot(allocator, array, destination, null);

      assertEquals(2, destination.getRowCount());
      BitVector bits = (BitVector) destination.getVector("boolean");
      VarCharVector strings = (VarCharVector) destination.getVector("varchar");
      assertEquals(0, bits.get(0));
      assertEquals(1, bits.get(1));
      assertEquals("test8", new String(strings.get(0), StandardCharsets.UTF_8));
      assertEquals("test9", new String(strings.get(1), StandardCharsets.UTF_8));
    }
  }

  @Test
  public void testImportVectorSchemaRootWithDuplicatedFieldNamesWithNonZeroOffset() {
    // Transferring a struct matches its children by name, which loses children with the same name
    try (IntVector a1 = new IntVector("a", allocator);
        IntVector a2 = new IntVector("a", allocator);
        VarCharVector b = new VarCharVector("b", allocator)) {
      setVector(a1, rows(i -> i % 5 == 0 ? null : i, Integer[]::new));
      setVector(a2, rows(i -> i % 3 == 0 ? null : 100 + i, Integer[]::new));
      setTestStrings(b);
      VectorSchemaRoot source = new VectorSchemaRoot(Arrays.asList(a1, a2, b));
      for (int offset : new int[] {3, 8, 13}) {
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
            VectorSchemaRoot imported = VectorSchemaRoot.create(source.getSchema(), allocator)) {
          Data.exportVectorSchemaRoot(allocator, source, null, array);
          slice(array, offset, ROW_COUNT - offset);

          Data.importIntoVectorSchemaRoot(allocator, array, imported, null);

          assertEquals(ROW_COUNT - offset, imported.getRowCount());
          for (int column = 0; column < 3; column++) {
            for (int i = 0; i < imported.getRowCount(); i++) {
              assertEquals(
                  source.getVector(column).getObject(offset + i),
                  imported.getVector(column).getObject(i),
                  "column " + column + ", offset " + offset + ", index " + i);
            }
          }
        }
      }
    }
  }

  @Test
  public void testImportVectorSchemaRootWithSlicedStructOfPlainExtension() {
    // The schema of the root has the extension type that the vector's getField() does not report
    Field field =
        new Field(
            "s",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(Field.nullable("p", PlainType.INSTANCE)));
    Schema schema = new Schema(Collections.singletonList(field));
    withExtensionType(
        PlainType.INSTANCE,
        () -> {
          try (VectorSchemaRoot source = VectorSchemaRoot.create(schema, allocator);
              VectorSchemaRoot destination = VectorSchemaRoot.create(schema, allocator);
              ArrowArray array = ArrowArray.allocateNew(allocator)) {
            StructVector struct = (StructVector) source.getVector("s");
            setTestStrings(((PlainVector) struct.getChild("p")).getUnderlyingVector());
            for (int i = 0; i < ROW_COUNT; i++) {
              struct.setIndexDefined(i);
            }
            source.setRowCount(ROW_COUNT);
            Data.exportVectorSchemaRoot(allocator, source, null, array);
            ArrowArray.Snapshot snapshot = array.snapshot();
            slice(array, 0, 5);
            long[] children = NativeUtil.toJavaArray(snapshot.children, (int) snapshot.n_children);
            for (long child : children) {
              slice(ArrowArray.wrap(child), 9, 5);
            }

            Data.importIntoVectorSchemaRoot(allocator, array, destination, null);

            assertEquals(5, destination.getRowCount());
            StructVector imported = (StructVector) destination.getVector("s");
            assertInstanceOf(PlainVector.class, imported.getChild("p"));
            for (int i = 0; i < 5; i++) {
              assertEquals(struct.getObject(9 + i), imported.getObject(i), "index " + i);
            }
          }
        });
  }

  @Test
  public void testImportExtensionTypeVectorWithNonZeroOffset() {
    try (UuidVector source = new UuidVector("source", allocator)) {
      setTestUuids(source);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportStructWithExtensionTypeChildWithNonZeroOffset() {
    try (StructVector source = StructVector.empty("source", allocator)) {
      UuidVector child =
          source.addOrGet("u", FieldType.nullable(UuidType.INSTANCE), UuidVector.class);
      setTestUuids(child);
      setStructRowsDefined(source);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportBitVectorWithNonZeroOffset() {
    try (BitVector source = new BitVector("source", allocator)) {
      setVector(source, rows(i -> i % 5 == 0 ? null : i % 2, Integer[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportLargeListWithNonZeroOffset() {
    try (LargeListVector source = LargeListVector.empty("source", allocator)) {
      setVector(source, testLists());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testImportFixedSizeListWithNonZeroOffset() {
    try (FixedSizeListVector source = FixedSizeListVector.empty("source", 2, allocator)) {
      setVector(source, rows(i -> Arrays.asList(2 * i, 2 * i + 1), List[]::new));
      for (int i = 1; i < ROW_COUNT; i += 6) {
        source.setNull(i);
      }
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportMapWithNonZeroOffset() {
    try (MapVector source = MapVector.empty("source", allocator, false)) {
      writeTestMaps(source);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportUnionWithNonZeroOffset() {
    try (UnionVector source = UnionVector.empty("source", allocator)) {
      writeTestUnion(source);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportDenseUnionWithNonZeroOffset() {
    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.isSet = 1;
    final NullableIntHolder intHolder = new NullableIntHolder();
    try (DenseUnionVector source = DenseUnionVector.empty("source", allocator)) {
      source.allocateNew();
      byte uint4TypeId = source.registerNewTypeId(Field.nullable("u", MinorType.UINT4.getType()));
      byte intTypeId = source.registerNewTypeId(Field.nullable("i", MinorType.INT.getType()));
      for (int i = 0; i < ROW_COUNT; i++) {
        if (i % 3 == 0) {
          uInt4Holder.value = 100 + i;
          source.setTypeId(i, uint4TypeId);
          source.setSafe(i, uInt4Holder);
        } else {
          intHolder.isSet = i % 5 == 0 ? 0 : 1;
          intHolder.value = -i;
          source.setTypeId(i, intTypeId);
          source.setSafe(i, intHolder);
        }
      }
      source.setValueCount(ROW_COUNT);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportDenseUnionWithTypeIdsWithNonZeroOffset() {
    // The type ids differ from the positions of the children they select
    Field field =
        new Field(
            "source",
            FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {5, 10})),
            Arrays.asList(
                Field.nullable("i", MinorType.INT.getType()),
                Field.nullable("s", MinorType.VARCHAR.getType())));
    try (DenseUnionVector source = (DenseUnionVector) field.createVector(allocator)) {
      source.allocateNew();
      IntVector ints = (IntVector) source.getVectorByType((byte) 5);
      VarCharVector strings = (VarCharVector) source.getVectorByType((byte) 10);
      int intCount = 0;
      int stringCount = 0;
      for (int i = 0; i < ROW_COUNT; i++) {
        if (i % 3 == 0) {
          source.setTypeId(i, (byte) 10);
          source.setOffset(i, stringCount);
          strings.setSafe(stringCount++, ("s" + i).getBytes(StandardCharsets.UTF_8));
        } else {
          source.setTypeId(i, (byte) 5);
          source.setOffset(i, intCount);
          ints.setSafe(intCount++, i);
        }
      }
      source.setValueCount(ROW_COUNT);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportDenseUnionWithDescendingTypeIdsWithNonZeroOffset() {
    // The children of a dense union need not be in the order of their type ids
    try (DenseUnionVector source =
        (DenseUnionVector) denseUnionWithDescendingTypeIds("source").createVector(allocator)) {
      setValuesWithDescendingTypeIds(source, ROW_COUNT);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportIntoDenseUnionWithDescendingTypeIdsWithNonZeroOffset() {
    // Without a field, the importer creates its temporary vector from the target's getField()
    Field field = denseUnionWithDescendingTypeIds("u");
    try (DenseUnionVector source = (DenseUnionVector) field.createVector(allocator);
        DenseUnionVector target = (DenseUnionVector) field.createVector(allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      setValuesWithDescendingTypeIds(source, 4);
      Data.exportVector(allocator, source, null, array);
      slice(array, 1, 3);

      Data.importIntoVector(allocator, array, target, null);

      assertEquals(3, target.getValueCount());
      assertEquals(201, target.getObject(0));
      assertEquals(102, target.getObject(1));
      assertEquals(203, target.getObject(2));
      assertEquals(field, target.getField());
    }
  }

  /** A dense union of two int children, {@code a} with type id 10 and {@code b} with 5. */
  private static Field denseUnionWithDescendingTypeIds(String name) {
    return new Field(
        name,
        FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {10, 5})),
        Arrays.asList(
            Field.nullable("a", MinorType.INT.getType()),
            Field.nullable("b", MinorType.INT.getType())));
  }

  /** Sets rows that alternate between a = 100 + row and b = 200 + row, starting with a. */
  private static void setValuesWithDescendingTypeIds(DenseUnionVector vector, int rowCount) {
    vector.allocateNew();
    IntVector a = (IntVector) vector.getVectorByType((byte) 10);
    IntVector b = (IntVector) vector.getVectorByType((byte) 5);
    for (int i = 0; i < rowCount; i++) {
      vector.setTypeId(i, (byte) (i % 2 == 0 ? 10 : 5));
      vector.setOffset(i, i / 2);
      (i % 2 == 0 ? a : b).setSafe(i / 2, (i % 2 == 0 ? 100 : 200) + i);
    }
    vector.setValueCount(rowCount);
  }

  @Test
  public void testImportDenseUnionWithOffsetGapsWithNonZeroOffset() {
    // The offsets of a dense union only have to increase, so they may skip values of a child
    try (DenseUnionVector source =
        (DenseUnionVector) denseUnionWithIntChild("source").createVector(allocator)) {
      setTestValuesWithOffsetGaps(source);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportStructWithDenseUnionWithOffsetGapsWithNonZeroOffset() {
    // The struct sets the value count of the dense union after transferring it
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(denseUnionWithIntChild("u")));
    try (StructVector source = (StructVector) field.createVector(allocator)) {
      for (int i = 0; i < OFFSETS_WITH_GAPS.length; i++) {
        if (i % 6 != 5) {
          source.setIndexDefined(i);
        }
      }
      source.setValueCount(OFFSETS_WITH_GAPS.length);
      setTestValuesWithOffsetGaps((DenseUnionVector) source.getChild("u"));
      assertSlicedImportsMatchSource(source);
    }
  }

  private static final int[] OFFSETS_WITH_GAPS = {0, 1, 3, 4, 5, 8, 9, 12, 13, 14, 17, 19};

  private static Field denseUnionWithIntChild(String name) {
    return new Field(
        name,
        FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {0})),
        Collections.singletonList(Field.nullable("i", MinorType.INT.getType())));
  }

  /** Selects the values at {@link #OFFSETS_WITH_GAPS} from the child of a dense union. */
  private static void setTestValuesWithOffsetGaps(DenseUnionVector vector) {
    vector.allocateNew();
    IntVector child = (IntVector) vector.getVectorByType((byte) 0);
    setVector(child, rows(i -> 100 + i, Integer[]::new));
    for (int i = 0; i < OFFSETS_WITH_GAPS.length; i++) {
      vector.setTypeId(i, (byte) 0);
      vector.setOffset(i, OFFSETS_WITH_GAPS[i]);
    }
    vector.setValueCount(OFFSETS_WITH_GAPS.length);
    // setValueCount() sets the child's value count to the number of rows selecting it
    child.setValueCount(ROW_COUNT);
  }

  @Test
  public void testImportRunEndEncodedWithNonZeroOffset() {
    try (RunEndEncodedVector source = RunEndEncodedVector.empty("source", allocator)) {
      setVector(source, List.of(2, 3, 6, 10, 11, 15, 20), List.of(1, 2, 3, 4, 5, 6, 7));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportViewVarCharWithNonZeroOffset() {
    try (ViewVarCharVector source = new ViewVarCharVector("source", allocator)) {
      setVector(source, testViewStrings());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportViewVarBinaryWithNonZeroOffset() {
    try (ViewVarBinaryVector source = new ViewVarBinaryVector("source", allocator)) {
      setVector(source, testViewBytes());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportVarBinaryWithNonZeroOffset() {
    try (VarBinaryVector source = new VarBinaryVector("source", allocator)) {
      setVector(source, testViewBytes());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportLargeVarCharWithNonZeroOffset() {
    try (LargeVarCharVector source = new LargeVarCharVector("source", allocator)) {
      setVector(source, testViewStrings());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportLargeVarBinaryWithNonZeroOffset() {
    try (LargeVarBinaryVector source = new LargeVarBinaryVector("source", allocator)) {
      setVector(source, testViewBytes());
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportDecimalWithNonZeroOffset() {
    try (DecimalVector source = new DecimalVector("source", allocator, 10, 2)) {
      setVector(source, rows(i -> i % 3 == 0 ? null : 1001L * i - 5000, Long[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportDecimal256WithNonZeroOffset() {
    try (Decimal256Vector source = new Decimal256Vector("source", allocator, 40, 2)) {
      setVector(source, rows(i -> i % 3 == 0 ? null : 1001L * i - 5000, Long[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportFixedSizeBinaryWithNonZeroOffset() {
    try (FixedSizeBinaryVector source = new FixedSizeBinaryVector("source", allocator, 3)) {
      setVector(
          source,
          rows(
              i -> i % 3 == 0 ? null : new byte[] {(byte) i, (byte) -i, (byte) (2 * i)},
              byte[][]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportFloat16WithNonZeroOffset() {
    try (Float2Vector source = new Float2Vector("source", allocator)) {
      setVector(source, rows(i -> i % 3 == 0 ? null : i / 4f - 2, Float[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportTimeStampWithTimeZoneWithNonZeroOffset() {
    try (TimeStampMicroTZVector source =
        new TimeStampMicroTZVector("source", allocator, "Asia/Tokyo")) {
      setVector(
          source,
          rows(i -> i % 3 == 0 ? null : 1_700_000_000_000_000L + 1_000_003L * i, Long[]::new));
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportListViewWithNonZeroOffset() {
    try (ListViewVector source = ListViewVector.empty("source", allocator)) {
      setVector(source, testListsWithoutNulls());
      setTestListNulls(source::setNull);
      assertSlicedImportsMatchSource(source);
    }
  }

  @Test
  public void testImportLargeListViewWithNonZeroOffset() {
    try (LargeListViewVector source = LargeListViewVector.empty("source", allocator)) {
      setVector(source, testListsWithoutNulls());
      setTestListNulls(source::setNull);
      assertSlicedImportsMatchSource(source);
    }
  }

  /**
   * Returns {@link #testLists()} with empty lists in place of the nulls, which the populators of
   * list views do not take.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Integer>[] testListsWithoutNulls() {
    List[] lists =
        Arrays.stream(testLists())
            .map(list -> list == null ? List.of() : list)
            .toArray(List[]::new);
    return lists;
  }

  /** Sets the rows that are null in {@link #testLists()} to null. */
  private static void setTestListNulls(IntConsumer setNull) {
    List<Integer>[] lists = testLists();
    for (int i = 0; i < lists.length; i++) {
      if (lists[i] == null) {
        setNull.accept(i);
      }
    }
  }

  /**
   * Strings of up to and over the 12 bytes that a view holds inline, some of which are not ASCII,
   * and some nulls.
   */
  private static String[] testViewStrings() {
    return rows(
        i ->
            i % 4 == 1
                ? null
                : i % 2 == 0 ? "é" + i + "日" : "a string longer than twelve bytes 日本語 😀 " + i,
        String[]::new);
  }

  /** The UTF-8 bytes of {@link #testViewStrings()}. */
  private static byte[][] testViewBytes() {
    return Arrays.stream(testViewStrings())
        .map(s -> s == null ? null : s.getBytes(StandardCharsets.UTF_8))
        .toArray(byte[][]::new);
  }

  @ParameterizedTest
  @ValueSource(ints = {1 << 16, (1 << 16) + 3})
  public void testImportSliceWithoutValidityBitmapAllocatesOnlyTheSlice(int offset) {
    // Producers commonly omit the validity bitmap of a column without nulls. Neither the bitmap
    // synthesized for the import nor the imported buffers may grow with the offset.
    final int length = 10;
    try (IntVector source = new IntVector("source", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      source.allocateNew(offset + length);
      for (int i = 0; i < offset + length; i++) {
        source.set(i, i);
      }
      source.setValueCount(offset + length);
      Data.exportVector(allocator, source, null, array, schema);
      slice(array, offset, length);
      dropValidityBitmap(array);

      try (BufferAllocator importAllocator =
              allocator.newChildAllocator("import", 0, Long.MAX_VALUE);
          IntVector imported =
              (IntVector) Data.importVector(importAllocator, array, schema, null)) {
        assertEquals(length, imported.getValueCount());
        assertEquals(0, imported.getNullCount());
        for (int i = 0; i < length; i++) {
          assertEquals(offset + i, imported.get(i));
        }
        assertTrue(
            importAllocator.getAllocatedMemory() < 1024,
            "allocated " + importAllocator.getAllocatedMemory() + " bytes");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {10, 1 << 16, (1 << 16) + 3})
  public void testImportStructSliceWithoutValidityBitmapsAllocatesOnlyTheSlice(int offset) {
    // The children of a sliced struct are exported at full length, and only the rows of the
    // slice may be imported from them
    final int rowCount = (1 << 16) + 20;
    final int length = 10;
    try (StructVector source = StructVector.empty("source", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      IntVector child =
          source.addOrGet("a", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
      child.allocateNew(rowCount);
      for (int i = 0; i < rowCount; i++) {
        child.set(i, i);
        source.setIndexDefined(i);
      }
      source.setValueCount(rowCount);
      Data.exportVector(allocator, source, null, array, schema);
      ArrowArray.Snapshot snapshot = array.snapshot();
      dropValidityBitmap(array);
      dropValidityBitmap(ArrowArray.wrap(NativeUtil.toJavaArray(snapshot.children, 1)[0]));
      slice(array, offset, length);

      try (BufferAllocator importAllocator =
              allocator.newChildAllocator("import", 0, Long.MAX_VALUE);
          StructVector imported =
              (StructVector) Data.importVector(importAllocator, array, schema, null)) {
        assertEquals(length, imported.getValueCount());
        IntVector importedChild = (IntVector) imported.getChild("a");
        for (int i = 0; i < length; i++) {
          assertFalse(imported.isNull(i));
          assertEquals(offset + i, importedChild.get(i));
        }
        assertTrue(
            importAllocator.getAllocatedMemory() < 1024,
            "allocated " + importAllocator.getAllocatedMemory() + " bytes");
      }
    }
  }

  @Test
  public void testImportOpaqueVectorWithNonZeroOffset() {
    // OpaqueVector overrides getField() but not makeTransferPair()
    withExtensionType(
        OPAQUE_TYPE,
        () -> {
          try (OpaqueVector source =
              (OpaqueVector) Field.nullable("source", OPAQUE_TYPE).createVector(allocator)) {
            setTestStrings((VarCharVector) source.getUnderlyingVector());
            assertSlicedImportsMatchSource(source);
          }
        });
  }

  @Test
  public void testImportStructWithOpaqueChildWithNonZeroOffset() {
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(Field.nullable("o", OPAQUE_TYPE)));
    withExtensionType(
        OPAQUE_TYPE,
        () -> {
          try (StructVector source = (StructVector) field.createVector(allocator)) {
            OpaqueVector child = (OpaqueVector) source.getChild("o");
            setTestStrings((VarCharVector) child.getUnderlyingVector());
            setStructRowsDefined(source);
            assertSlicedImportsMatchSource(source);
          }
        });
  }

  @Test
  public void testImportListOfOpaqueWithNonZeroOffset() {
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(Field.nullable("item", OPAQUE_TYPE)));
    withExtensionType(
        OPAQUE_TYPE,
        () -> {
          try (ListVector source = (ListVector) field.createVector(allocator)) {
            OpaqueVector items = (OpaqueVector) source.getDataVector();
            writeTestStringLists(source, (VarCharVector) items.getUnderlyingVector());
            assertSlicedImportsMatchSource(source);
          }
        });
  }

  @Test
  public void testImportPlainExtensionVectorWithNonZeroOffset() {
    // PlainVector overrides neither getField() nor makeTransferPair()
    Field field = Field.nullable("source", PlainType.INSTANCE);
    withExtensionType(
        PlainType.INSTANCE,
        () -> {
          try (PlainVector source = (PlainVector) field.createVector(allocator)) {
            setTestStrings(source.getUnderlyingVector());
            assertSlicedImportsMatchSource(field, source);
          }
        });
  }

  @Test
  public void testImportIntoPlainExtensionVectorWithNonZeroOffset() {
    // The target reports only its storage type, so the temporary vector is a plain VarCharVector
    Field field = Field.nullable("source", PlainType.INSTANCE);
    withExtensionType(
        PlainType.INSTANCE,
        () -> {
          try (PlainVector source = (PlainVector) field.createVector(allocator);
              PlainVector destination = (PlainVector) field.createVector(allocator);
              ArrowArray array = ArrowArray.allocateNew(allocator)) {
            setTestStrings(source.getUnderlyingVector());
            Data.exportVector(allocator, source, null, array);
            slice(array, 9, 5);

            Data.importIntoVector(allocator, array, destination, null);

            assertEquals(5, destination.getValueCount());
            for (int i = 0; i < 5; i++) {
              assertEquals(source.getObject(9 + i), destination.getObject(i), "index " + i);
            }
          }
        });
  }

  @Test
  public void testImportStructWithPlainExtensionChildWithNonZeroOffset() {
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(Field.nullable("p", PlainType.INSTANCE)));
    withExtensionType(
        PlainType.INSTANCE,
        () -> {
          try (StructVector source = (StructVector) field.createVector(allocator)) {
            setTestStrings(((PlainVector) source.getChild("p")).getUnderlyingVector());
            setStructRowsDefined(source);
            assertSlicedImportsMatchSource(field, source);
          }
        });
  }

  @Test
  public void testImportListOfPlainExtensionWithNonZeroOffset() {
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(Field.nullable("item", PlainType.INSTANCE)));
    withExtensionType(
        PlainType.INSTANCE,
        () -> {
          try (ListVector source = (ListVector) field.createVector(allocator)) {
            PlainVector items = (PlainVector) source.getDataVector();
            writeTestStringLists(source, items.getUnderlyingVector());
            assertSlicedImportsMatchSource(field, source);
          }
        });
  }

  @Test
  public void testImportIntoStructWithPlainExtensionChildWithNonZeroOffset() {
    // The struct's child is imported into the destination's PlainVector without a field that
    // reports its extension type, because structs are imported without a temporary vector
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Collections.singletonList(Field.nullable("p", PlainType.INSTANCE)));
    withExtensionType(
        PlainType.INSTANCE,
        () -> {
          try (StructVector source = (StructVector) field.createVector(allocator);
              StructVector destination = (StructVector) field.createVector(allocator);
              ArrowArray array = ArrowArray.allocateNew(allocator)) {
            setTestStrings(((PlainVector) source.getChild("p")).getUnderlyingVector());
            setStructRowsDefined(source);
            Data.exportVector(allocator, source, null, array);
            slice(array, 9, 5);

            Data.importIntoVector(allocator, array, destination, null);

            assertInstanceOf(PlainVector.class, destination.getChild("p"));
            assertEquals(5, destination.getValueCount());
            for (int i = 0; i < 5; i++) {
              assertEquals(source.getObject(9 + i), destination.getObject(i), "index " + i);
            }
          }
        });
  }

  private static final int ROW_COUNT = 20;

  /** Returns the values of the {@link #ROW_COUNT} rows of a test vector. */
  private static <T> T[] rows(IntFunction<T> value, IntFunction<T[]> newArray) {
    return IntStream.range(0, ROW_COUNT).mapToObj(value).toArray(newArray);
  }

  /** Lists of up to 3 values, and some nulls. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Integer>[] testLists() {
    List[] lists =
        rows(
            i ->
                i % 5 == 2
                    ? null
                    : IntStream.range(0, i % 4)
                        .mapToObj(j -> 10 * i + j)
                        .collect(Collectors.toList()),
            List[]::new);
    return lists;
  }

  /** Two int children, which are both null in the rows where the struct is null. */
  private static Map<String, List<Integer>> testStructValues() {
    Map<String, List<Integer>> values = new HashMap<>();
    values.put("a", Arrays.asList(rows(i -> i % 7 == 0 ? null : i, Integer[]::new)));
    values.put(
        "b", Arrays.asList(rows(i -> i % 7 == 0 || i % 3 == 0 ? null : 10 * i, Integer[]::new)));
    return values;
  }

  /** Strings with characters that are not ASCII, and some nulls. */
  private static void setTestStrings(VarCharVector vector) {
    String[] suffixes = {"é", "日本語", "😀"};
    setVector(vector, rows(i -> i % 4 == 1 ? null : "s" + i + suffixes[i % 3], String[]::new));
  }

  private static void setTestUuids(UuidVector vector) {
    vector.setValueCount(ROW_COUNT);
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 4 == 0) {
        vector.setNull(i);
      } else {
        vector.set(i, new UUID(i, -i));
      }
    }
  }

  /** Defines all but some rows of a struct whose children are set already. */
  private static void setStructRowsDefined(StructVector vector) {
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 6 != 5) {
        vector.setIndexDefined(i);
      }
    }
    vector.setValueCount(ROW_COUNT);
  }

  /**
   * Imports the slices of {@code source} that start at each offset and either have one row or end
   * at the end of the source, which covers offsets before and after the first byte of a bitmap,
   * aligned to a byte or not.
   */
  private void assertSlicedImportsMatchSource(FieldVector source) {
    assertSlicedImportsMatchSource(source.getField(), source);
  }

  /** Like {@link #assertSlicedImportsMatchSource(FieldVector)}, exporting the type of field. */
  private void assertSlicedImportsMatchSource(Field field, FieldVector source) {
    int valueCount = source.getValueCount();
    for (int offset = 1; offset < valueCount; offset++) {
      assertSlicedImportMatchesSource(field, source, offset, 1);
      assertSlicedImportMatchesSource(field, source, offset, valueCount - offset);
    }
  }

  /**
   * Imports a slice of {@code source}, exported with the type of {@code field}, and compares it
   * with the same range of the source.
   */
  private void assertSlicedImportMatchesSource(
      Field field, FieldVector source, int offset, int length) {
    try (ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      Data.exportField(allocator, field, null, schema);
      Data.exportVector(allocator, source, null, array);
      slice(array, offset, length);
      try (FieldVector imported = Data.importVector(allocator, array, schema, null)) {
        if (canValidate(imported)) {
          ValueVectorUtility.validateFull(imported);
        }
        assertSameVectorClasses(source, imported);
        assertEquals(length, imported.getValueCount());
        String range = "offset " + offset + ", length " + length;
        assertEquals(expectedNullCount(source, offset, length), imported.getNullCount(), range);
        for (int i = 0; i < length; i++) {
          Object expected = source.getObject(offset + i);
          Object actual = imported.getObject(i);
          if (expected instanceof byte[]) {
            assertArrayEquals((byte[]) expected, (byte[]) actual, range + ", index " + i);
          } else {
            assertEquals(expected, actual, range + ", index " + i);
          }
        }
      }
    }
  }

  /**
   * Returns how many of the {@code length} rows of {@code vector} from {@code offset} are null, or
   * 0 for unions and run-end encoded vectors, which have no validity bitmap of their own and always
   * report a null count of 0.
   */
  private static int expectedNullCount(FieldVector vector, int offset, int length) {
    if (vector instanceof UnionVector
        || vector instanceof DenseUnionVector
        || vector instanceof RunEndEncodedVector) {
      return 0;
    }
    return (int) IntStream.range(offset, offset + length).filter(vector::isNull).count();
  }

  /**
   * Returns whether {@link ValueVectorUtility#validateFull} supports {@code vector}, which it does
   * not for fixed-size lists, maps, list views, run-end encoded vectors, float16 vectors, or
   * extension vectors that report their storage type.
   */
  private static boolean canValidate(FieldVector vector) {
    if (vector instanceof FixedSizeListVector
        || vector instanceof MapVector
        || vector instanceof ListViewVector
        || vector instanceof LargeListViewVector
        || vector instanceof Float2Vector
        || vector instanceof RunEndEncodedVector
        || (vector instanceof ExtensionTypeVector
            && !(vector.getField().getType() instanceof ExtensionType))) {
      return false;
    }
    return vector.getChildrenFromFields().stream().allMatch(RoundtripTest::canValidate);
  }

  private static void assertSameVectorClasses(FieldVector expected, FieldVector actual) {
    assertEquals(expected.getClass(), actual.getClass());
    List<FieldVector> expectedChildren = expected.getChildrenFromFields();
    List<FieldVector> actualChildren = actual.getChildrenFromFields();
    assertEquals(expectedChildren.size(), actualChildren.size());
    for (int i = 0; i < expectedChildren.size(); i++) {
      assertSameVectorClasses(expectedChildren.get(i), actualChildren.get(i));
    }
  }

  private static final OpaqueType OPAQUE_TYPE =
      new OpaqueType(ArrowType.Utf8.INSTANCE, "test.type", "test.vendor");

  /** Runs {@code test} with {@code type} registered, so that importing it creates its vector. */
  private static void withExtensionType(ExtensionType type, Runnable test) {
    ExtensionType previous = ExtensionTypeRegistry.lookup(type.extensionName());
    ExtensionTypeRegistry.register(type);
    try {
      test.run();
    } finally {
      ExtensionTypeRegistry.unregister(type);
      if (previous != null) {
        ExtensionTypeRegistry.register(previous);
      }
    }
  }

  /** An extension type whose vector overrides neither getField() nor makeTransferPair(). */
  private static final class PlainType extends ExtensionType {
    static final PlainType INSTANCE = new PlainType();

    @Override
    public ArrowType storageType() {
      return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public String extensionName() {
      return "test.plain";
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
      return other instanceof PlainType;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
      return INSTANCE;
    }

    @Override
    public String serialize() {
      return "";
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
      return new PlainVector(name, allocator);
    }
  }

  private static final class PlainVector extends ExtensionTypeVector<VarCharVector> {
    PlainVector(String name, BufferAllocator allocator) {
      super(name, allocator, new VarCharVector(name, allocator));
    }

    @Override
    public Object getObject(int index) {
      return getUnderlyingVector().getObject(index);
    }

    @Override
    public int hashCode(int index) {
      return getUnderlyingVector().hashCode(index);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      return getUnderlyingVector().hashCode(index, hasher);
    }
  }

  /** Writes lists of up to 3 strings and some nulls, whose items are stored in values. */
  private static void writeTestStringLists(ListVector vector, VarCharVector values) {
    vector.allocateNew();
    values.allocateNew();
    int valueCount = 0;
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 5 == 2) {
        vector.setNull(i);
        continue;
      }
      vector.startNewValue(i);
      for (int j = 0; j < i % 4; j++) {
        if (j == 1) {
          values.setNull(valueCount);
        } else {
          values.setSafe(valueCount, ("s" + i + j).getBytes(StandardCharsets.UTF_8));
        }
        valueCount++;
      }
      vector.endValue(i, i % 4);
    }
    vector.setValueCount(ROW_COUNT);
  }

  /** Rewrite an exported array in place as one whose producer omitted the validity bitmap. */
  private static void dropValidityBitmap(ArrowArray array) {
    ArrowArray.Snapshot snapshot = array.snapshot();
    assertEquals(0, snapshot.null_count);
    MemoryUtil.putLong(snapshot.buffers, NativeUtil.NULL);
  }

  /** Writes maps of up to 3 entries and some nulls. */
  private static void writeTestMaps(MapVector vector) {
    vector.allocateNew();
    UnionMapWriter mapWriter = vector.getWriter();
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 5 == 3) {
        continue;
      }
      mapWriter.setPosition(i);
      mapWriter.startMap();
      for (int j = 0; j < i % 4; j++) {
        mapWriter.startEntry();
        mapWriter.key().bigInt().writeBigInt(j);
        mapWriter.value().integer().writeInt(10 * i + j);
        mapWriter.endEntry();
      }
      mapWriter.endMap();
    }
    mapWriter.setValueCount(ROW_COUNT);
  }

  /** Writes uint4 and int values, and some nulls. */
  private static void writeTestUnion(UnionVector vector) {
    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.isSet = 1;
    final NullableIntHolder intHolder = new NullableIntHolder();
    intHolder.isSet = 1;
    vector.allocateNew();
    for (int i = 0; i < ROW_COUNT; i++) {
      if (i % 3 == 0) {
        uInt4Holder.value = 100 + i;
        vector.setType(i, MinorType.UINT4);
        vector.setSafe(i, uInt4Holder);
      } else if (i % 3 == 1) {
        intHolder.value = -i;
        vector.setType(i, MinorType.INT);
        vector.setSafe(i, intHolder);
      }
    }
    vector.setValueCount(ROW_COUNT);
  }

  /** Rewrite an exported array in place as a slice, as a C producer would. */
  private static void slice(ArrowArray array, long offset, long length) {
    ArrowArray.Snapshot snapshot = array.snapshot();
    snapshot.offset = offset;
    snapshot.length = length;
    array.save(snapshot);
  }

  @Test
  public void testImportEmptyArrayWithNonZeroOffset() {
    try (IntVector source = new IntVector("source", allocator);
        IntVector destination = new IntVector("destination", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      setVector(source, 1, 2, 3);
      Data.exportVector(allocator, source, null, array);
      slice(array, source.getValueCount(), 0);

      Data.importIntoVector(allocator, array, destination, null);
      assertEquals(0, destination.getValueCount());
    }
  }

  @Test
  public void testImportNullArrayWithNonZeroOffset() {
    try (NullVector source = new NullVector("source", 10);
        NullVector destination = new NullVector("destination");
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      Data.exportVector(allocator, source, null, array);
      slice(array, 2, 8);

      Data.importIntoVector(allocator, array, destination, null);
      assertEquals(8, destination.getValueCount());
    }
  }

  @Test
  public void testImportStructOfMapAndListOfStructsWithNonZeroOffset() {
    // m and the structs in ls are nullable, while ls and the y of its structs are not
    Field entries =
        new Field(
            "entries",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                Field.notNullable("key", MinorType.BIGINT.getType()),
                Field.nullable("value", MinorType.INT.getType())));
    Field item =
        new Field(
            "item",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                Field.nullable("x", MinorType.INT.getType()),
                Field.notNullable("y", MinorType.INT.getType())));
    Field field =
        new Field(
            "source",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(
                new Field(
                    "m",
                    FieldType.nullable(new ArrowType.Map(false)),
                    Collections.singletonList(entries)),
                new Field(
                    "ls",
                    FieldType.notNullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(item))));
    try (StructVector source = (StructVector) field.createVector(allocator)) {
      writeTestMaps((MapVector) source.getChild("m"));
      writeTestListsOfStructs((ListVector) source.getChild("ls"));
      setStructRowsDefined(source);
      assertSlicedImportsMatchSource(source);
    }
  }

  /** Writes lists of up to 2 structs, whose x is null in some of them. */
  private static void writeTestListsOfStructs(ListVector vector) {
    vector.allocateNew();
    UnionListWriter writer = vector.getWriter();
    for (int i = 0; i < ROW_COUNT; i++) {
      writer.setPosition(i);
      writer.startList();
      for (int j = 0; j < i % 3; j++) {
        writer.struct().start();
        if ((i + j) % 4 != 0) {
          writer.struct().integer("x").writeInt(10 * i + j);
        }
        writer.struct().integer("y").writeInt(-10 * i - j);
        writer.struct().end();
      }
      writer.endList();
    }
    writer.setValueCount(ROW_COUNT);
  }

  @Test
  public void testImportListWithChildOffsetWithNonZeroOffset() {
    // Both the list and its child have an offset, and the list's offsets count from the child's
    try (ListVector source = ListVector.empty("source", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      setVector(source, List.of(0, 1), List.of(2), List.of(3, 4, 5), List.of(), List.of(6, 7));
      // Values that only the offset of the child makes reachable
      IntVector values = (IntVector) source.getDataVector();
      values.setSafe(8, 8);
      values.setSafe(9, 9);
      values.setValueCount(10);
      Data.exportField(allocator, source.getField(), null, schema);
      Data.exportVector(allocator, source, null, array);
      slice(array, 1, 3);
      ArrowArray.Snapshot snapshot = array.snapshot();
      slice(ArrowArray.wrap(NativeUtil.toJavaArray(snapshot.children, 1)[0]), 2, 8);

      try (FieldVector imported = Data.importVector(allocator, array, schema, null)) {
        assertEquals(3, imported.getValueCount());
        assertEquals(List.of(4), imported.getObject(0));
        assertEquals(List.of(5, 6, 7), imported.getObject(1));
        assertEquals(List.of(), imported.getObject(2));
      }
    }
  }

  @Test
  public void testImportArrayWithNegativeOffsetFails() {
    assertSliceRejected(-1, 2, "ArrowArray struct has negative offset (-1)");
  }

  @Test
  public void testImportArrayWithNegativeLengthFails() {
    assertSliceRejected(1, -2, "ArrowArray struct has negative length (-2)");
  }

  private void assertSliceRejected(long offset, long length, String message) {
    try (IntVector source = new IntVector("source", allocator);
        IntVector destination = new IntVector("destination", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      setVector(source, 1, 2, 3);
      Data.exportVector(allocator, source, null, array);
      slice(array, offset, length);

      IllegalStateException e =
          assertThrows(
              IllegalStateException.class,
              () -> Data.importIntoVector(allocator, array, destination, null));
      assertEquals(message, e.getMessage());
    }
  }

  @Test
  public void testImportSliceWithOffsetBeyondIntRange() {
    // The importer skips the rows before the slice by advancing the buffer pointers, so the array
    // the slice belongs to can have more rows than a vector. Moving the pointers back by the rows
    // that are skipped makes the buffers of the source look like those of such an array.
    long skippedRows = 1L << 31;
    try (IntVector source = new IntVector("source", allocator);
        IntVector destination = new IntVector("destination", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator)) {
      setVector(source, rows(i -> i % 3 == 0 ? null : i, Integer[]::new));
      Data.exportVector(allocator, source, null, array);
      ArrowArray.Snapshot snapshot = array.snapshot();
      long validity = MemoryUtil.getLong(snapshot.buffers);
      long data = MemoryUtil.getLong(snapshot.buffers + 8);
      MemoryUtil.putLong(snapshot.buffers, validity - skippedRows / 8);
      MemoryUtil.putLong(snapshot.buffers + 8, data - skippedRows * 4);
      slice(array, skippedRows + 3, 5);

      Data.importIntoVector(allocator, array, destination, null);

      assertEquals(5, destination.getValueCount());
      for (int i = 0; i < 5; i++) {
        assertEquals(source.getObject(3 + i), destination.getObject(i), "index " + i);
      }
    }
  }

  @Test
  public void testImportRunEndEncodedSliceBeyondIntRangeFails() {
    // Run ends count from the start of the array, so a vector has to hold the rows before the slice
    try (RunEndEncodedVector source = RunEndEncodedVector.empty("source", allocator);
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      setVector(source, List.of(2, 3, 6), List.of(1, 2, 3));
      Data.exportField(allocator, source.getField(), null, schema);
      Data.exportVector(allocator, source, null, array);
      slice(array, Integer.MAX_VALUE - 2, 5);

      IllegalStateException e =
          assertThrows(
              IllegalStateException.class, () -> Data.importVector(allocator, array, schema, null));
      assertEquals(
          "Cannot import an ArrowArray with offset 2147483645 and length 5, because a vector"
              + " cannot hold the 2147483650 rows it has to import",
          e.getMessage());
    }
  }

  @Test
  public void testArrayStructReuse() {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {
      // Producer creates structures from existing memory pointers
      try (final NullVector vector = new NullVector()) {
        exportFieldVector(vector, consumerArrowSchema, consumerArrowArray);
      }
      Data.importVector(allocator, consumerArrowArray, consumerArrowSchema, null, false);

      // Imported structs should be released but not closed
      assertEquals(0, consumerArrowSchema.snapshot().release);
      assertNotEquals(0, consumerArrowSchema.memoryAddress());
      assertEquals(0, consumerArrowArray.snapshot().release);
      assertNotEquals(0, consumerArrowArray.memoryAddress());

      try (final NullVector vector = new NullVector()) {
        exportFieldVector(vector, consumerArrowSchema, consumerArrowArray);
      }
      Data.importVector(allocator, consumerArrowArray, consumerArrowSchema, null, false);

      // Imported structs should be released but not closed
      assertEquals(0, consumerArrowSchema.snapshot().release);
      assertNotEquals(0, consumerArrowSchema.memoryAddress());
      assertEquals(0, consumerArrowArray.snapshot().release);
      assertNotEquals(0, consumerArrowArray.memoryAddress());
    }
  }

  private void exportFieldVector(
      FieldVector vector, ArrowSchema consumerArrowSchema, ArrowArray consumerArrowArray) {
    try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
        ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
      // Producer exports vector into the C Data Interface structures
      Data.exportVector(allocator, vector, null, arrowArray, arrowSchema);
    }
  }

  private VectorSchemaRoot createTestVSR() {
    BitVector bitVector = new BitVector("boolean", allocator);

    Map<String, String> metadata = new HashMap<>();
    metadata.put("key", "value");
    FieldType fieldType = new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata);
    VarCharVector varCharVector = new VarCharVector("varchar", fieldType, allocator);

    bitVector.allocateNew();
    varCharVector.allocateNew();
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);

    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);

    return new VectorSchemaRoot(fields, vectors);
  }
}
