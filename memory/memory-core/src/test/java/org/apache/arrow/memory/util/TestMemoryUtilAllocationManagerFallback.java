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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestMemoryUtilAllocationManagerFallback {

  @Test
  public void allocationManagerTypeFfmFallsBackToUnsafeWithoutFfmOnClasspath() {
    // This test runs in its own JVM (see memory-core/pom.xml Surefire execution added below)
    // with -Darrow.allocation.manager.type=FFM, arrow.memory.accessor.type left unset, and
    // arrow-memory-ffm NOT on the classpath.
    //
    // Unlike an explicit arrow.memory.accessor.type=FFM request, this preference is only inferred
    // from a different property. MemoryUtil must fall back to Unsafe instead of failing inside
    // <clinit>, which would otherwise permanently poison the class for the rest of the JVM's life
    // (JLS 12.4.2) over a signal that may not even be load-bearing.
    assertEquals(
        "org.apache.arrow.memory.util.UnsafeMemoryAccessor", MemoryUtil.getAccessorClassName());
  }
}
