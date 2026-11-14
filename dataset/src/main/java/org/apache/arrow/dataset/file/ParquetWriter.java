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
package org.apache.arrow.dataset.file;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/** JNI-based Parquet Writer. Provides methods to write VectorSchemaRoot to Parquet files. */
public class ParquetWriter implements AutoCloseable {

  private static final JniWrapper jni = JniWrapper.get();

  private final BufferAllocator allocator;
  private final ArrowSchema arrowSchema;
  private long nativePtr;
  private boolean closed = false;

  /**
   * Create a new ParquetWriter with default properties.
   *
   * @param outputStream the Java OutputStream to write Parquet data to
   * @param schema the Arrow Schema
   * @throws IOException if initialization fails
   */
  public ParquetWriter(OutputStream outputStream, org.apache.arrow.vector.types.pojo.Schema schema)
      throws IOException {
    this(outputStream, schema, null);
  }

  /**
   * Create a new ParquetWriter with custom properties.
   *
   * @param outputStream the Java OutputStream to write Parquet data to
   * @param schema the Arrow Schema
   * @param properties optional writer properties (can be null for defaults)
   * @throws IOException if initialization fails
   */
  public ParquetWriter(
      OutputStream outputStream,
      org.apache.arrow.vector.types.pojo.Schema schema,
      ParquetWriterProperties properties)
      throws IOException {
    this.allocator = new RootAllocator();
    ArrowSchema arrowSchemaLocal;

    try {
      // Convert Java Schema to Arrow C Data Interface Schema
      arrowSchemaLocal = ArrowSchema.allocateNew(this.allocator);
      this.arrowSchema = arrowSchemaLocal;
      Data.exportSchema(this.allocator, schema, null, arrowSchemaLocal);
    } catch (Exception e) {
      this.close();
      throw new IOException("Failed to convert schema to ArrowSchema: " + e.getMessage(), e);
    }

    long ptr = jni.nativeCreateParquetWriter(outputStream, arrowSchema.memoryAddress(), properties);

    if (ptr == 0) {
      this.close();
      throw new IOException("Failed to create ParquetWriter");
    }
    this.nativePtr = ptr;
  }

  /**
   * Write VectorSchemaRoot to Parquet file.
   *
   * @param root VectorSchemaRoot object (contains data and schema)
   * @throws IOException if write fails
   */
  public void write(VectorSchemaRoot root) throws IOException {
    if (closed) {
      throw new IOException("ParquetWriter is already closed");
    }

    // Use VectorSchemaRoot's allocator to avoid allocator mismatch issues
    // Get allocator from root's vector
    BufferAllocator alloc =
        root.getFieldVectors().isEmpty() ? allocator : root.getFieldVectors().get(0).getAllocator();

    // Check if nativePtr is valid (before calling export)
    if (nativePtr == 0) {
      throw new IOException(
          "ParquetWriter native pointer is invalid (0) - writer may be closed or not initialized");
    }

    try (ArrowArray arrowArray = ArrowArray.allocateNew(alloc)) {
      // Convert VectorSchemaRoot to Arrow C Data Interface Array
      // Must use the same allocator as VectorSchemaRoot for ArrowArray
      // Export VectorSchemaRoot to C Data Interface
      // This creates references to the data in VectorSchemaRoot
      Data.exportVectorSchemaRoot(alloc, root, null, arrowArray, null);

      // Get memory address (must be called after export)
      long arrayPtr = arrowArray.memoryAddress();

      // Check if arrayPtr is valid
      if (arrayPtr == 0) {
        throw new IOException(
            "Failed to get ArrowArray memory address (returned 0). "
                + "This may indicate that exportVectorSchemaRoot failed or ArrowArray is invalid. "
                + "VectorSchemaRoot row count: "
                + root.getRowCount());
      }

      int result = jni.nativeWriteParquetBatch(nativePtr, arrayPtr);
      arrowArray.release();
      // Check for pending exceptions (thrown from native side)
      if (result == 0) {
        throw new IOException("Failed to write RecordBatch (native method returned 0)");
      }
    }
  }

  /**
   * Close ParquetWriter.
   *
   * @throws IOException if close fails
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    if (nativePtr != 0) {
      int result = jni.nativeCloseParquetWriter(nativePtr);
      nativePtr = 0;
      if (result == 0) {
        throw new IOException("Failed to close ParquetWriter");
      }
    }
    if (arrowSchema != null) {
      arrowSchema.release();
      arrowSchema.close();
    }
    if (allocator != null) {
      allocator.close();
    }

    closed = true;
  }
}
