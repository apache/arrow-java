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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

// Regression test for https://github.com/apache/arrow-java/issues/759
final class ExceptionTest {
  @Test
  public void testException() throws IOException {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("ints", new ArrowType.Int(32, true))));
    final List<Object> batches = new ArrayList<>();

    try (BufferAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      final String exceptionMessage = "This is a message for testing exception.";

      RuntimeException exToThrow = new RuntimeException(exceptionMessage);
      batches.add(exToThrow);

      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      exToThrow.printStackTrace(pw);
      final String expectExceptionMessage = sw.toString();

      ArrowReader source = new ExceptionMemoryArrowReader(allocator, schema, batches);

      try (final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
           final VectorSchemaRoot importRoot = VectorSchemaRoot.create(schema, allocator)) {
        final VectorLoader loader = new VectorLoader(importRoot);
        Data.exportArrayStream(allocator, source, stream);

        try (final ArrowReader reader = Data.importArrayStream(allocator, stream)) {
          IOException jniException = catchThrowableOfType(IOException.class, reader::loadNextBatch);
          final String jniMessage = jniException.getMessage();
          assertThat(jniMessage.endsWith(expectExceptionMessage + "}"));
        }
      }
    }
  }

  static class ExceptionMemoryArrowReader extends ArrowReader {
    private final Schema schema;
    private final List<Object> batches; // set ArrowRecordBatch or Exception
    private final DictionaryProvider provider;
    private int nextBatch;

    ExceptionMemoryArrowReader(
        BufferAllocator allocator,
        Schema schema,
        List<Object> batches) {
      super(allocator);
      this.schema = schema;
      this.batches = batches;
      this.provider = new CDataDictionaryProvider();
      this.nextBatch = 0;
    }

    @Override
    public Dictionary lookup(long id) {
      return provider.lookup(id);
    }

    @Override
    public Set<Long> getDictionaryIds() {
      return provider.getDictionaryIds();
    }

    @Override
    public Map<Long, Dictionary> getDictionaryVectors() {
      return getDictionaryIds().stream()
          .collect(Collectors.toMap(Function.identity(), this::lookup));
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      if (nextBatch < batches.size()) {
        Object object = batches.get(nextBatch++);
        if (object instanceof RuntimeException) {
          throw (RuntimeException) object;
        }
        VectorLoader loader = new VectorLoader(getVectorSchemaRoot());
        loader.load((ArrowRecordBatch) object);
        return true;
      }
      return false;
    }

    @Override
    public long bytesRead() {
      return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
      try {
        for (Object object : batches) {
          if (object instanceof ArrowRecordBatch) {
            ArrowRecordBatch batch = (ArrowRecordBatch) object;
            batch.close();
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    protected Schema readSchema() {
      return schema;
    }
  }
}
