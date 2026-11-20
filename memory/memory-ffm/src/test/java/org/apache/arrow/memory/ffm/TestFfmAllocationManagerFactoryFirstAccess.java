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
package org.apache.arrow.memory.ffm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

public class TestFfmAllocationManagerFactoryFirstAccess {

  @Test
  public void factoryIsUsableWhenAccessedBeforeAnyAllocator() {
    // This test runs in its own JVM (see memory-ffm/pom.xml Surefire execution) so that
    // FfmAllocationManager is the first Arrow memory class initialized. Its static init creates an
    // ArrowBuf, which initializes BaseAllocator, whose default config resolves the default factory:
    // with only arrow-memory-ffm on the classpath, that is FfmAllocationManager.FACTORY itself.
    AllocationManager.Factory factory = FfmAllocationManager.FACTORY;

    try (BufferAllocator allocator =
            new RootAllocator(
                RootAllocator.configBuilder().allocationManagerFactory(factory).build());
        ArrowBuf buf = allocator.buffer(64)) {
      assertEquals(64, buf.capacity());
    }
  }
}
