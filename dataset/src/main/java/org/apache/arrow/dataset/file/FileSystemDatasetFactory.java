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

import java.lang.reflect.Method;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.arrow.dataset.jni.NativeDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.FragmentScanOptions;
import org.apache.arrow.memory.BufferAllocator;

/** Java binding of the C++ FileSystemDatasetFactory. */
public class FileSystemDatasetFactory extends NativeDatasetFactory {

  private final Set<URI> hdfsFileSystems;

  public FileSystemDatasetFactory(
      BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat format, String uri) {
    super(allocator, memoryPool, createNative(format, uri, Optional.empty()));
    this.hdfsFileSystems = toHdfsFileSystems(uri);
  }

  public FileSystemDatasetFactory(
      BufferAllocator allocator,
      NativeMemoryPool memoryPool,
      FileFormat format,
      String uri,
      Optional<FragmentScanOptions> fragmentScanOptions) {
    super(allocator, memoryPool, createNative(format, uri, fragmentScanOptions));
    this.hdfsFileSystems = toHdfsFileSystems(uri);
  }

  public FileSystemDatasetFactory(
      BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat format, String[] uris) {
    super(allocator, memoryPool, createNative(format, uris, Optional.empty()));
    this.hdfsFileSystems = toHdfsFileSystems(uris);
  }

  public FileSystemDatasetFactory(
      BufferAllocator allocator,
      NativeMemoryPool memoryPool,
      FileFormat format,
      String[] uris,
      Optional<FragmentScanOptions> fragmentScanOptions) {
    super(allocator, memoryPool, createNative(format, uris, fragmentScanOptions));
    this.hdfsFileSystems = toHdfsFileSystems(uris);
  }

  /**
   * Close this factory and release the native instance. For HDFS URIs, also closes the cached
   * Hadoop FileSystem to release non-daemon threads that would otherwise prevent JVM exit. See <a
   * href="https://github.com/apache/arrow-java/issues/1067">#1067</a>.
   */
  @Override
  public synchronized void close() {
    try {
      super.close();
    } finally {
      hdfsFileSystems.forEach(FileSystemDatasetFactory::closeHadoopFileSystem);
    }
  }

  /**
   * For each {@code hdfs://} URI, close the cached Hadoop FileSystem.
   * When Arrow C++ accesses HDFS via libhdfs, the Hadoop Java client creates cached FileSystem
   * instances with non-daemon threads (IPC connections, lease renewers) that prevent JVM exit.
   * Closing the FileSystem terminates these connections. Uses reflection to avoid a compile-time
   * dependency on hadoop-common.
   */
  static void closeHadoopFileSystemsIfHdfs(String... uris) {
    toHdfsFileSystems(uris).forEach(FileSystemDatasetFactory::closeHadoopFileSystem);
  }

  private static Set<URI> toHdfsFileSystems(String... uris) {
    Set<URI> hdfsFileSystems = new LinkedHashSet<>();
    if (uris == null) {
      return hdfsFileSystems;
    }
    for (String uri : uris) {
      try {
        URI parsedUri = new URI(uri);
        if ("hdfs".equalsIgnoreCase(parsedUri.getScheme())) {
          hdfsFileSystems.add(
              new URI(parsedUri.getScheme(), parsedUri.getAuthority(), null, null, null));
        }
      } catch (Exception e) {
        // Ignore here; native factory creation reports invalid user URIs.
      }
    }
    return hdfsFileSystems;
  }

  private static void closeHadoopFileSystem(URI hdfsUri) {
    try {
      Class<?> confClass = Class.forName("org.apache.hadoop.conf.Configuration");
      Object conf = confClass.getDeclaredConstructor().newInstance();
      Class<?> fsClass = Class.forName("org.apache.hadoop.fs.FileSystem");
      Method getMethod = fsClass.getMethod("get", URI.class, confClass);
      Object fs = getMethod.invoke(null, hdfsUri, conf);
      Method closeMethod = fsClass.getMethod("close");
      closeMethod.invoke(fs);
    } catch (Exception e) {
      // Best-effort cleanup; Hadoop may not be on classpath or FileSystem already closed
    }
  }

  private static long createNative(
      FileFormat format, String uri, Optional<FragmentScanOptions> fragmentScanOptions) {
    return JniWrapper.get()
        .makeFileSystemDatasetFactory(
            uri, format.id(), fragmentScanOptions.map(FragmentScanOptions::serialize).orElse(null));
  }

  private static long createNative(
      FileFormat format, String[] uris, Optional<FragmentScanOptions> fragmentScanOptions) {
    return JniWrapper.get()
        .makeFileSystemDatasetFactoryWithFiles(
            uris,
            format.id(),
            fragmentScanOptions.map(FragmentScanOptions::serialize).orElse(null));
  }
}
