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
package org.apache.arrow.dataset.jni;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.lang.reflect.Field;
import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression test for the native {@code FromSchemaByteArray} helper (GH-1205). Passing malformed
 * serialized schema bytes to {@code JniWrapper#createDataset} exercises the former leak branch in
 * {@code FromSchemaByteArray} (where {@code ReleaseByteArrayElements} was skipped on error) and
 * asserts it fails gracefully with a Java exception rather than crashing.
 *
 * <p>This guards the error path; it does not directly assert that the native byte-array elements
 * were released. The leaked bytes are a JVM-internal copy made by {@code GetByteArrayElements},
 * which is not tracked by {@code NativeMemoryPool} nor observable through any portable Java API, so
 * a deterministic leak assertion isn't feasible here. See the PR #1249 discussion for details.
 */
public class TestFromSchemaByteArray extends TestNativeDataset {

  @TempDir public File TMP;

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  private static long factoryId(NativeDatasetFactory factory) throws Exception {
    Field field = NativeDatasetFactory.class.getDeclaredField("datasetFactoryId");
    field.setAccessible(true);
    return field.getLong(factory);
  }

  @Test
  public void testCreateDatasetWithMalformedSchemaBytes() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a");
    FileSystemDatasetFactory factory =
        new FileSystemDatasetFactory(
            rootAllocator(),
            NativeMemoryPool.getDefault(),
            FileFormat.PARQUET,
            writeSupport.getOutputURI());
    try {
      final long datasetFactoryId = factoryId(factory);
      // Bytes that are not a valid serialized Arrow schema, so native ReadSchema fails and the
      // error path of FromSchemaByteArray is taken.
      final byte[] malformedSchemaBytes = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

      // Repeat many times to keep hitting the error path that previously skipped the element
      // release; each iteration must fail gracefully rather than crash.
      for (int i = 0; i < 1000; i++) {
        assertThrows(
            RuntimeException.class,
            () -> JniWrapper.get().createDataset(datasetFactoryId, malformedSchemaBytes));
      }
    } finally {
      factory.close();
    }
  }
}
