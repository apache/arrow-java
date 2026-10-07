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
package org.apache.arrow.memory.util;

import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.arrow.memory.DefaultAllocationManagerOption.AllocationManagerType;
import org.junit.jupiter.api.Test;

/**
 * When FFM is only inferred from arrow.allocation.manager.type=FFM, a failure to load the FFM
 * accessor must fall back to Unsafe, including the {@link LinkageError}s a JVM throws for it.
 */
public class TestMemoryUtilInferredFfmAccessorFallback {

  @Test
  public void fallsBackToUnsafeWhenFfmAccessorTargetsANewerJdk() {
    // What JDK 21 and earlier throw when loading arrow-memory-ffm, which is compiled for release 22
    MemoryUtilAccessor accessor =
        MemoryUtil.resolveAccessor(
            "",
            AllocationManagerType.FFM,
            () -> {
              throw new UnsupportedClassVersionError(
                  "org/apache/arrow/memory/ffm/FfmMemoryAccessor has been compiled by a more"
                      + " recent version of the Java Runtime (class file version 66.0)");
            });

    assertSame(UnsafeMemoryAccessor.INSTANCE, accessor);
  }

  @Test
  public void fallsBackToUnsafeWhenFfmAccessorInitializationFails() {
    MemoryUtilAccessor accessor =
        MemoryUtil.resolveAccessor(
            "",
            AllocationManagerType.FFM,
            () -> {
              throw new ExceptionInInitializerError(
                  new IllegalCallerException("Illegal native access"));
            });

    assertSame(UnsafeMemoryAccessor.INSTANCE, accessor);
  }
}
