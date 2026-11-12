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
package org.apache.arrow.vector.ipc;

import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.ValidateUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestArrowReaderValidation {

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
  public void testValidBatchPassesValidation() throws Exception {
    try (ViewVarCharVector views = new ViewVarCharVector("views", allocator);
        ListViewVector listViews = ListViewVector.empty("listViews", allocator);
        LargeListViewVector largeListViews =
            LargeListViewVector.empty("largeListViews", allocator);
        RunEndEncodedVector runs = RunEndEncodedVector.empty("runs", allocator)) {
      setVector(views, "a", "a string longer than the inline size", null);
      setVector(listViews, Arrays.asList(1, 2), Collections.emptyList(), Arrays.asList(3));
      setVector(largeListViews, Arrays.asList(1), Arrays.asList(2, 3), Collections.emptyList());
      setVector(runs, Arrays.asList(1, 3), Arrays.asList(10, 20));
      VectorSchemaRoot root = VectorSchemaRoot.of(views, listViews, largeListViews, runs);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.writeBatch();
      }

      try (ArrowStreamReader reader =
          new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
        reader.setValidateBatches(true);
        assertTrue(reader.loadNextBatch());
        assertEquals(root.contentToTSVString(), reader.getVectorSchemaRoot().contentToTSVString());
        assertFalse(reader.loadNextBatch());
      }
    }
  }

  @Test
  public void testInvalidOffsetsRejected() throws Exception {
    Schema schema =
        new Schema(Collections.singletonList(Field.nullable("s", ArrowType.Utf8.INSTANCE)));
    byte[] stream;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    WriteChannel channel = new WriteChannel(Channels.newChannel(out));
    MessageSerializer.serialize(channel, schema);
    try (ArrowRecordBatch batch = recordBatchWithInvalidOffsets()) {
      MessageSerializer.serialize(channel, batch);
    }
    ArrowStreamWriter.writeEndOfStream(channel, IpcOption.DEFAULT);
    stream = out.toByteArray();

    // Not checked by default
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(stream), allocator)) {
      assertTrue(reader.loadNextBatch());
    }

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(stream), allocator)) {
      reader.setValidateBatches(true);
      assertThrows(ValidateUtil.ValidateException.class, reader::loadNextBatch);
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  public void testInvalidDictionaryRejected() throws Exception {
    DictionaryEncoding encoding = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));
    Schema schema =
        new Schema(
            Collections.singletonList(
                new Field(
                    "s",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, encoding),
                    Collections.emptyList())));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    WriteChannel channel = new WriteChannel(Channels.newChannel(out));
    MessageSerializer.serialize(channel, schema);
    try (ArrowDictionaryBatch batch =
        new ArrowDictionaryBatch(1L, recordBatchWithInvalidOffsets())) {
      MessageSerializer.serialize(channel, batch);
    }
    ArrowStreamWriter.writeEndOfStream(channel, IpcOption.DEFAULT);

    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
      reader.setValidateBatches(true);
      assertThrows(ValidateUtil.ValidateException.class, reader::loadNextBatch);
      assertEquals(0, reader.lookup(1L).getVector().getValueCount());
    }
  }

  /**
   * Builds a batch with three Utf8 values whose second offset is past the end of the 8 byte data
   * buffer. The last offset is within the buffer, so only a full validation finds it.
   */
  private ArrowRecordBatch recordBatchWithInvalidOffsets() throws IOException {
    int[] offsets = {0, 1_000_000, 2, 4};
    try (ArrowBuf validity = allocator.buffer(8);
        ArrowBuf offsetBuffer = allocator.buffer(offsets.length * 4L);
        ArrowBuf data = allocator.buffer(8)) {
      validity.setOne(0, 8);
      validity.writerIndex(8);
      for (int i = 0; i < offsets.length; i++) {
        offsetBuffer.setInt(i * 4L, offsets[i]);
      }
      offsetBuffer.writerIndex(offsets.length * 4L);
      data.setZero(0, 8);
      data.writerIndex(8);
      return new ArrowRecordBatch(
          3,
          Collections.singletonList(new ArrowFieldNode(3, 0)),
          Arrays.asList(validity, offsetBuffer, data));
    }
  }
}
