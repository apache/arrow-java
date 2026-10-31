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
package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A vector imported through the C Data interface from a sliced array keeps the offsets of the
 * slice, which start past zero, and the bytes or child elements before them. {@link VectorUnloader}
 * must write only what the offsets cover.
 */
public class TestVectorUnloadNonZeroStartOffset {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }

  private ArrowBuf ints(int... values) {
    ArrowBuf buf = allocator.buffer(4L * values.length);
    for (int i = 0; i < values.length; i++) {
      buf.setInt(4L * i, values[i]);
    }
    buf.writerIndex(4L * values.length);
    return buf;
  }

  private ArrowBuf longs(long... values) {
    ArrowBuf buf = allocator.buffer(8L * values.length);
    for (int i = 0; i < values.length; i++) {
      buf.setLong(8L * i, values[i]);
    }
    buf.writerIndex(8L * values.length);
    return buf;
  }

  private ArrowBuf bytes(String values) {
    byte[] data = values.getBytes(StandardCharsets.UTF_8);
    ArrowBuf buf = allocator.buffer(data.length);
    buf.setBytes(0, data);
    buf.writerIndex(data.length);
    return buf;
  }

  private ArrowBuf allValid() {
    return longs(-1L);
  }

  private static void load(FieldVector vector, int length, ArrowBuf... buffers) {
    vector.loadFieldBuffers(new ArrowFieldNode(length, 0), Arrays.asList(buffers));
    for (ArrowBuf buf : buffers) {
      buf.close();
    }
  }

  /** Rows 2..3 of ["aa", "bb", "cc", "dd"]: the data buffer still holds "aabb" in front. */
  private VarCharVector varCharSlice() {
    VarCharVector vector = new VarCharVector("s", allocator);
    load(vector, 2, allValid(), ints(4, 6, 8), bytes("aabbccdd"));
    return vector;
  }

  private LargeVarCharVector largeVarCharSlice() {
    LargeVarCharVector vector = new LargeVarCharVector("s", allocator);
    load(vector, 2, allValid(), longs(4, 6, 8), bytes("aabbccdd"));
    return vector;
  }

  private void loadEightInts(FieldVector child) {
    load(child, 8, allValid(), ints(0, 1, 2, 3, 4, 5, 6, 7));
  }

  /** Two rows of [[0, 1], [2, 3], [4, 5], [6, 7]] whose child still holds all eight elements. */
  private ListVector listSlice(int... offsets) {
    ListVector vector = ListVector.empty("l", allocator);
    vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
    load(vector, 2, allValid(), ints(offsets));
    loadEightInts(vector.getDataVector());
    return vector;
  }

  private LargeListVector largeListSlice(long... offsets) {
    LargeListVector vector = LargeListVector.empty("l", allocator);
    vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
    load(vector, 2, allValid(), longs(offsets));
    loadEightInts(vector.getDataVector());
    return vector;
  }

  /** Rows 1..2 of [{0: 10, 1: 11}, {2: 12, 3: 13}, {4: 14, 5: 15}, {6: 16, 7: 17}]. */
  private MapVector mapSlice() {
    MapVector vector = MapVector.empty("m", allocator, false);
    Field key = new Field("key", FieldType.notNullable(MinorType.INT.getType()), null);
    Field value = new Field("value", FieldType.nullable(MinorType.INT.getType()), null);
    Field entries =
        new Field(
            MapVector.DATA_VECTOR_NAME,
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            Arrays.asList(key, value));
    vector.initializeChildrenFromFields(Collections.singletonList(entries));
    load(vector, 2, allValid(), ints(2, 4, 6));
    StructVector struct = (StructVector) vector.getDataVector();
    load(struct, 8, allValid());
    loadEightInts(struct.getChild("key"));
    load(struct.getChild("value"), 8, allValid(), ints(10, 11, 12, 13, 14, 15, 16, 17));
    return vector;
  }

  /**
   * The first slice of [["aa"], ["bb"], ["cc"]]: list offsets start at zero, its child's do not.
   */
  private ListVector listOfVarCharSlice() {
    ListVector vector = ListVector.empty("l", allocator);
    vector.addOrGetVector(FieldType.nullable(MinorType.VARCHAR.getType()));
    load(vector, 2, allValid(), ints(0, 1, 2));
    load(vector.getDataVector(), 2, allValid(), ints(2, 4, 6), bytes("aabbcc"));
    return vector;
  }

  private static List<Long> sizes(ArrowRecordBatch batch) {
    return batch.getBuffersLayout().stream().map(ArrowBuffer::getSize).collect(Collectors.toList());
  }

  private static List<Integer> lengths(ArrowRecordBatch batch) {
    return batch.getNodes().stream().map(ArrowFieldNode::getLength).collect(Collectors.toList());
  }

  private VectorSchemaRoot reload(VectorSchemaRoot root, ArrowRecordBatch batch) {
    VectorSchemaRoot loaded = VectorSchemaRoot.create(root.getSchema(), allocator);
    new VectorLoader(loaded).load(batch);
    return loaded;
  }

  @Test
  public void varCharIsWrittenFromItsFirstOffset() {
    try (VarCharVector vector = varCharSlice();
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(1L, 12L, 4L), sizes(batch));
      assertEquals(0, batch.getBuffers().get(1).getInt(0));
      assertEquals(4, batch.getBuffers().get(1).getInt(8));
      VarCharVector read = (VarCharVector) loaded.getVector(0);
      assertEquals("cc", read.getObject(0).toString());
      assertEquals("dd", read.getObject(1).toString());
    }
  }

  @Test
  public void largeVarCharIsWrittenFromItsFirstOffset() {
    try (LargeVarCharVector vector = largeVarCharSlice();
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(1L, 24L, 4L), sizes(batch));
      assertEquals(0L, batch.getBuffers().get(1).getLong(0));
      LargeVarCharVector read = (LargeVarCharVector) loaded.getVector(0);
      assertEquals("cc", read.getObject(0).toString());
      assertEquals("dd", read.getObject(1).toString());
    }
  }

  @Test
  public void listChildIsWrittenBetweenItsOffsets() {
    try (ListVector vector = listSlice(2, 4, 6);
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(2, 4), lengths(batch));
      assertEquals(Arrays.asList(1L, 12L, 1L, 16L), sizes(batch));
      assertEquals(0, batch.getBuffers().get(1).getInt(0));
      ListVector read = (ListVector) loaded.getVector(0);
      assertEquals(Arrays.asList(2, 3), read.getObject(0));
      assertEquals(Arrays.asList(4, 5), read.getObject(1));
    }
  }

  @Test
  public void largeListChildIsWrittenBetweenItsOffsets() {
    try (LargeListVector vector = largeListSlice(2, 4, 6);
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(2, 4), lengths(batch));
      assertEquals(Arrays.asList(1L, 24L, 1L, 16L), sizes(batch));
      assertEquals(0L, batch.getBuffers().get(1).getLong(0));
      LargeListVector read = (LargeListVector) loaded.getVector(0);
      assertEquals(Arrays.asList(2, 3), read.getObject(0));
      assertEquals(Arrays.asList(4, 5), read.getObject(1));
    }
  }

  @Test
  public void mapEntriesAreWrittenBetweenItsOffsets() {
    try (MapVector vector = mapSlice();
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(2, 4, 4, 4), lengths(batch));
      assertEquals(Arrays.asList(1L, 12L, 1L, 1L, 16L, 1L, 16L), sizes(batch));
      MapVector read = (MapVector) loaded.getVector(0);
      assertEquals(vector.getObject(0), read.getObject(0));
      assertEquals(vector.getObject(1), read.getObject(1));
      StructVector entries = (StructVector) read.getDataVector();
      assertEquals(2, ((IntVector) entries.getChild("key")).get(0));
      assertEquals(15, ((IntVector) entries.getChild("value")).get(3));
    }
  }

  @Test
  public void listChildIsWrittenUpToItsLastOffset() {
    try (ListVector vector = listSlice(0, 2, 4);
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(2, 4), lengths(batch));
      assertEquals(Arrays.asList(1L, 12L, 1L, 16L), sizes(batch));
      // the list's own offsets start at zero, so they are written as they are
      assertEquals(
          vector.getOffsetBuffer().memoryAddress(), batch.getBuffers().get(1).memoryAddress());
      ListVector read = (ListVector) loaded.getVector(0);
      assertEquals(Arrays.asList(0, 1), read.getObject(0));
      assertEquals(Arrays.asList(2, 3), read.getObject(1));
    }
  }

  @Test
  public void nestedVarCharIsWrittenFromItsFirstOffset() {
    try (ListVector vector = listOfVarCharSlice();
        VectorSchemaRoot root = VectorSchemaRoot.of(vector);
        ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch();
        VectorSchemaRoot loaded = reload(root, batch)) {
      assertEquals(Arrays.asList(2, 2), lengths(batch));
      assertEquals(Arrays.asList(1L, 12L, 1L, 12L, 4L), sizes(batch));
      assertEquals(0, batch.getBuffers().get(3).getInt(0));
      ListVector read = (ListVector) loaded.getVector(0);
      assertEquals(vector.getObject(0), read.getObject(0));
      assertEquals(vector.getObject(1), read.getObject(1));
      assertEquals("bb", ((VarCharVector) read.getDataVector()).getObject(0).toString());
    }
  }

  @Test
  public void vectorWithZeroStartOffsetIsWrittenWithItsOwnBuffers() {
    try (VarCharVector vector = new VarCharVector("s", allocator);
        VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
      load(vector, 2, allValid(), ints(0, 2, 4), bytes("aabb"));
      try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
        assertEquals(Arrays.asList(1L, 12L, 4L), sizes(batch));
        assertEquals(
            vector.getOffsetBuffer().memoryAddress(), batch.getBuffers().get(1).memoryAddress());
        assertEquals(
            vector.getDataBuffer().memoryAddress(), batch.getBuffers().get(2).memoryAddress());
      }
    }
  }
}
