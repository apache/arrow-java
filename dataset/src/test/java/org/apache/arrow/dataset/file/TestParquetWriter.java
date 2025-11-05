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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetWriter extends TestDataset {

  @TempDir public File TMP;

  @Test
  void testParquetWriter() throws Exception {
    String parquetFilePath =
        Paths.get(TMP.getAbsolutePath(), "testParquetWriter.parquet").toString();
    List<Field> fields =
        Arrays.asList(
            Field.nullable("id", new ArrowType.Int(32, true)),
            Field.nullable("name", new ArrowType.Utf8()));
    Schema arrowSchema = new Schema(fields);

    int[] ids = new int[] {1, 2, 3, 4, 5};
    String[] names = new String[] {"Alice", "Bob", "Charlie", "Diana", "Eve"};

    // Write Parquet file
    try (FileOutputStream fos = new FileOutputStream(parquetFilePath);
        ParquetWriter writer = new ParquetWriter(fos, arrowSchema);
        VectorSchemaRoot vectorSchemaRoot = createData(rootAllocator(), arrowSchema, ids, names)) {
      writer.write(vectorSchemaRoot);
    }

    // Verify file exists and is not empty
    File parquetFile = new File(parquetFilePath);
    assertTrue(parquetFile.exists(), "Parquet file should exist");

    // Read and verify Parquet file content
    FileSystemDatasetFactory factory =
        new FileSystemDatasetFactory(
            rootAllocator(),
            NativeMemoryPool.getDefault(),
            FileFormat.PARQUET,
            parquetFile.toURI().toString());
    ScanOptions options = new ScanOptions(100);
    Schema readSchema = factory.inspect();

    // verify schema
    assertEquals(arrowSchema, readSchema);
    List<ArrowRecordBatch> batches = collectResultFromFactory(factory, options);
    assertEquals(1, batches.size());
    ArrowRecordBatch batch = batches.get(0);
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(readSchema, rootAllocator())) {
      VectorLoader loader = new VectorLoader(vsr);
      loader.load(batch);

      IntVector idVector = (IntVector) vsr.getVector("id");
      VarCharVector nameVector = (VarCharVector) vsr.getVector("name");
      for (int rowIndex = 0; rowIndex < batch.getLength(); rowIndex++) {
        int actualId = idVector.get(rowIndex);
        String actualName = nameVector.getObject(rowIndex).toString();
        // Find the corresponding expected values
        int expectedId = ids[rowIndex];
        String expectedName = names[rowIndex];
        assertEquals(expectedId, actualId, "ID mismatch at row " + rowIndex);
        assertEquals(expectedName, actualName, "Name mismatch at row " + rowIndex);
        rowIndex++;
      }
    }

    AutoCloseables.close(factory);
    AutoCloseables.close(batches);
  }

  private static VectorSchemaRoot createData(
      BufferAllocator allocator, Schema schema, int[] ids, String[] names) {
    // Create VectorSchemaRoot from schema
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    // Allocate space for vectors (we'll write 5 rows)
    root.allocateNew();

    // Get field vectors
    IntVector idVector = (IntVector) root.getVector("id");
    VarCharVector nameVector = (VarCharVector) root.getVector("name");

    // Write data to vectors
    for (int i = 0; i < ids.length; i++) {
      idVector.setSafe(i, ids[i]);
      nameVector.setSafe(i, names[i].getBytes());
    }

    // Set the row count
    root.setRowCount(ids.length);

    return root;
  }
}
