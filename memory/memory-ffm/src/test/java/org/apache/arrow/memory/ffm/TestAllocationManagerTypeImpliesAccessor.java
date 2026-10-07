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

import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.jupiter.api.Test;

/**
 * Verifies that setting only {@code arrow.allocation.manager.type=FFM} (with {@code
 * arrow.memory.accessor.type} left unset) is enough for {@link MemoryUtil} to resolve to {@code
 * FfmMemoryAccessor} on its own, so choosing the FFM allocation manager avoids {@code
 * sun.misc.Unsafe} entirely without a second property. Runs in a dedicated Surefire execution (see
 * memory-ffm/pom.xml) so the property is set before MemoryUtil's static init, and doesn't leak into
 * this module's other tests.
 */
public class TestAllocationManagerTypeImpliesAccessor {

  @Test
  public void allocationManagerTypeAloneSelectsFfmAccessor() {
    assertEquals(
        "org.apache.arrow.memory.ffm.FfmMemoryAccessor", MemoryUtil.getAccessorClassName());

    long address = MemoryUtil.allocateMemory(8);
    try {
      MemoryUtil.putLong(address, 123456789L);
      assertEquals(123456789L, MemoryUtil.getLong(address));
    } finally {
      MemoryUtil.freeMemory(address);
    }
  }
}
