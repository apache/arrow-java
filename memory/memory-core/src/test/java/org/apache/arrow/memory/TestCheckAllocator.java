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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Test cases for {@link CheckAllocator}. */
public class TestCheckAllocator {

  private static final String CORE = "org.apache.arrow.memory.DefaultAllocationManagerFactory";
  private static final String UNSAFE =
      "org.apache.arrow.memory.unsafe.DefaultAllocationManagerFactory";
  private static final String NETTY =
      "org.apache.arrow.memory.netty.DefaultAllocationManagerFactory";

  @Test
  public void testCheckClasspath() {
    // memory-core tests provide their own DefaultAllocationManagerFactory
    assertEquals(CORE, CheckAllocator.check());
  }

  @Test
  public void testCheckIgnoresLocation() {
    ClassLoader classLoader =
        classLoaderWith(Map.of(NETTY, "file:/work/arrow-memory-core/memory-netty/classes/"));
    assertEquals(NETTY, CheckAllocator.check(classLoader));
  }

  @Test
  public void testCheckUnsafeBeforeNetty() {
    ClassLoader classLoader =
        classLoaderWith(
            Map.of(NETTY, "file:/lib/arrow-memory-netty/", UNSAFE, "file:/lib/arrow-memory-unsafe/"));
    assertEquals(UNSAFE, CheckAllocator.check(classLoader));
  }

  @Test
  public void testCheckNoAllocator() {
    assertThrows(RuntimeException.class, () -> CheckAllocator.check(classLoaderWith(Map.of())));
  }

  /** Returns a class loader that finds each factory class only at the given location. */
  private static ClassLoader classLoaderWith(Map<String, String> locations) {
    return new ClassLoader(null) {
      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        for (Map.Entry<String, String> location : locations.entrySet()) {
          if (name.equals(location.getKey().replace('.', '/') + ".class")) {
            return Collections.enumeration(List.of(URI.create(location.getValue() + name).toURL()));
          }
        }
        return Collections.emptyEnumeration();
      }
    };
  }
}
