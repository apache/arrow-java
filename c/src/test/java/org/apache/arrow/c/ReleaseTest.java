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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.c.jni.PrivateData;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Release lifecycle of exported C Data Interface structures: running the native release callback
 * closes the Java {@link PrivateData} exactly once, clears the structure's release markers so a
 * second release is a no-op, and frees everything the export retained. Covers both callbacks, the
 * one shared by {@link ArrowSchema}/{@link ArrowArray} and the {@link ArrowArrayStream} one. See
 * https://github.com/apache/arrow-java/issues/1385.
 */
final class ReleaseTest {

  /** Exported structure kinds, one per native release callback. */
  private enum Kind {
    SCHEMA,
    ARRAY,
    STREAM
  }

  /** Release callback and private data pointers of an exported structure. */
  private static final class State {
    final long releaseCallback;
    final long privateData;

    State(long releaseCallback, long privateData) {
      this.releaseCallback = releaseCallback;
      this.privateData = privateData;
    }
  }

  /** Private data that counts how often the native release callback closed it. */
  private static final class CountingPrivateData implements PrivateData {
    private final boolean failOnClose;
    int closeCount;

    CountingPrivateData(boolean failOnClose) {
      this.failOnClose = failOnClose;
    }

    @Override
    public void close() {
      closeCount++;
      if (failOnClose) {
        throw new IllegalStateException("Deliberate failure from PrivateData.close");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void releaseClosesPrivateDataOnce(Kind kind) {
    CountingPrivateData privateData = new CountingPrivateData(false);

    try (BufferAllocator allocator = new RootAllocator()) {
      try (BaseStruct struct = export(allocator, kind, privateData)) {
        final State exported = state(struct);
        assertThat(exported.releaseCallback).isNotZero();
        assertThat(exported.privateData).isNotZero();

        struct.release();

        assertThat(privateData.closeCount).isEqualTo(1);
        final State released = state(struct);
        assertThat(released.releaseCallback).isZero();
        assertThat(released.privateData).isZero();

        // Releasing again must not run the release callback (and thus not close twice)
        struct.release();

        assertThat(privateData.closeCount).isEqualTo(1);
        assertThat(state(struct).releaseCallback).isZero();
      }

      assertThat(allocator.getAllocatedMemory()).isZero();
    }
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void releaseCleansUpWhenCloseThrows(Kind kind) {
    CountingPrivateData privateData = new CountingPrivateData(true);

    try (BufferAllocator allocator = new RootAllocator()) {
      try (BaseStruct struct = export(allocator, kind, privateData)) {
        assertThat(state(struct).releaseCallback).isNotZero();

        // The native side describes and clears the failure: release must not propagate it
        assertDoesNotThrow(struct::release);

        assertThat(privateData.closeCount).isEqualTo(1);
        final State released = state(struct);
        assertThat(released.releaseCallback).isZero();
        assertThat(released.privateData).isZero();
      }

      assertThat(allocator.getAllocatedMemory()).isZero();
    }
  }

  @Test
  void releaseOfExportedVectorClearsNestedState() {
    final Schema schema =
        new Schema(
            Collections.singletonList(
                new Field(
                    "outer",
                    FieldType.nullable(new ArrowType.Struct()),
                    Arrays.asList(
                        Field.nullable("ints", new ArrowType.Int(32, true)),
                        Field.nullable("ints2", new ArrowType.Int(32, true))))));

    try (BufferAllocator allocator = new RootAllocator()) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowArray array = ArrowArray.allocateNew(allocator);
          ArrowSchema exportedSchema = ArrowSchema.allocateNew(allocator)) {
        final FieldVector vector = root.getVector("outer");
        fill(vector, 4);
        root.setRowCount(4);

        Data.exportVector(allocator, vector, null, array, exportedSchema);

        final ArrowArray.Snapshot exportedArray = array.snapshot();
        assertThat(exportedArray.release).isNotZero();
        assertThat(exportedArray.private_data).isNotZero();
        assertThat(exportedArray.n_children).isEqualTo(2);

        final ArrowSchema.Snapshot exportedField = exportedSchema.snapshot();
        assertThat(exportedField.release).isNotZero();
        assertThat(exportedField.private_data).isNotZero();
        assertThat(exportedField.n_children).isEqualTo(2);

        array.release();
        exportedSchema.release();

        assertThat(array.snapshot().release).isZero();
        assertThat(array.snapshot().private_data).isZero();
        assertThat(exportedSchema.snapshot().release).isZero();
        assertThat(exportedSchema.snapshot().private_data).isZero();
      }

      // The nested private data close callbacks free the buffers the export retained.
      assertThat(allocator.getAllocatedMemory()).isZero();
    }
  }

  private static void fill(FieldVector vector, int valueCount) {
    for (FieldVector child : vector.getChildrenFromFields()) {
      final IntVector intVector = (IntVector) child;
      intVector.allocateNew(valueCount);
      for (int i = 0; i < valueCount; i++) {
        intVector.setSafe(i, i);
      }
      intVector.setValueCount(valueCount);
    }
    vector.setValueCount(valueCount);
  }

  /** Exports a fresh structure of the given kind, so that native code owns its release. */
  private static BaseStruct export(BufferAllocator allocator, Kind kind, PrivateData privateData) {
    switch (kind) {
      case SCHEMA:
        {
          final ArrowSchema schema = ArrowSchema.allocateNew(allocator);
          schema.save(new ArrowSchema.Snapshot());
          JniWrapper.get().exportSchema(schema.memoryAddress(), privateData);
          return schema;
        }
      case ARRAY:
        {
          final ArrowArray array = ArrowArray.allocateNew(allocator);
          array.save(new ArrowArray.Snapshot());
          JniWrapper.get().exportArray(array.memoryAddress(), privateData);
          return array;
        }
      case STREAM:
        {
          final ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
          stream.save(new ArrowArrayStream.Snapshot());
          JniWrapper.get().exportArrayStream(stream.memoryAddress(), privateData);
          return stream;
        }
      default:
        throw new IllegalArgumentException("Unsupported structure kind: " + kind);
    }
  }

  /** Reads the native release callback and private data pointer of an exported structure. */
  private static State state(BaseStruct struct) {
    if (struct instanceof ArrowSchema) {
      final ArrowSchema.Snapshot snapshot = ((ArrowSchema) struct).snapshot();
      return new State(snapshot.release, snapshot.private_data);
    }
    if (struct instanceof ArrowArray) {
      final ArrowArray.Snapshot snapshot = ((ArrowArray) struct).snapshot();
      return new State(snapshot.release, snapshot.private_data);
    }
    final ArrowArrayStream.Snapshot snapshot = ((ArrowArrayStream) struct).snapshot();
    return new State(snapshot.release, snapshot.private_data);
  }
}
