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
package org.apache.arrow.memory;

import java.io.IOException;
import java.net.URL;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.util.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static method to ensure we have a RootAllocator on the classpath and report which one is used.
 */
final class CheckAllocator {
  private static final Logger logger = LoggerFactory.getLogger(CheckAllocator.class);

  /**
   * The DefaultAllocationManagerFactory implementations to look for, in order of precedence. Unsafe
   * and Netty match {@link DefaultAllocationManagerOption.AllocationManagerType}, so the logged
   * name can be used to set the type explicitly.
   */
  private enum Allocator {
    // only provided by the memory-core tests
    Core("org.apache.arrow.memory.DefaultAllocationManagerFactory"),
    // unique package names needed by JPMS module naming
    Unsafe("org.apache.arrow.memory.unsafe.DefaultAllocationManagerFactory"),
    Netty("org.apache.arrow.memory.netty.DefaultAllocationManagerFactory");

    private final String factoryClassName;
    private final String resourcePath;

    Allocator(String factoryClassName) {
      this.factoryClassName = factoryClassName;
      this.resourcePath = factoryClassName.replace('.', '/') + ".class";
    }
  }

  private CheckAllocator() {}

  static String check() {
    return check(CheckAllocator.class.getClassLoader());
  }

  @VisibleForTesting
  static String check(@Nullable ClassLoader classLoader) {
    Map<Allocator, Set<URL>> found = scanClasspath(classLoader);
    if (found.isEmpty()) {
      throw new RuntimeException(
          "No DefaultAllocationManager found on classpath. Can't allocate Arrow buffers."
              + " Please consider adding arrow-memory-netty or arrow-memory-unsafe as a dependency.");
    }
    Map.Entry<Allocator, Set<URL>> first = found.entrySet().iterator().next();
    Allocator allocator = first.getKey();
    Set<URL> urls = first.getValue();
    if (found.size() > 1) {
      logger.warn(
          "Found allocation manager types {} on the classpath, using {}. Set the {} system property"
              + " or the {} environment variable to choose one explicitly",
          found.keySet(),
          allocator,
          DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
          DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_ENV_NAME);
    }
    if (urls.size() > 1) {
      logger.warn(
          "More than one {} DefaultAllocationManager on classpath {}. Choosing first found",
          allocator,
          urls);
    }
    logger.info(
        "Allocation manager type not specified, inferred {} from the classpath at {}",
        allocator,
        urls.iterator().next());
    return allocator.factoryClassName;
  }

  @SuppressWarnings("URLEqualsHashCode")
  private static Map<Allocator, Set<URL>> scanClasspath(@Nullable ClassLoader classLoader) {
    // EnumMap iterates in declaration order, which is the order of precedence
    Map<Allocator, Set<URL>> found = new EnumMap<>(Allocator.class);
    for (Allocator allocator : Allocator.values()) {
      try {
        Enumeration<URL> paths =
            classLoader == null
                ? ClassLoader.getSystemResources(allocator.resourcePath)
                : classLoader.getResources(allocator.resourcePath);
        // LinkedHashSet appropriate here because it preserves insertion order
        // during iteration
        Set<URL> urls = new LinkedHashSet<>();
        while (paths.hasMoreElements()) {
          urls.add(paths.nextElement());
        }
        if (!urls.isEmpty()) {
          found.put(allocator, urls);
        }
      } catch (IOException ioe) {
        logger.error("Error getting resources from path", ioe);
      }
    }
    return found;
  }
}
