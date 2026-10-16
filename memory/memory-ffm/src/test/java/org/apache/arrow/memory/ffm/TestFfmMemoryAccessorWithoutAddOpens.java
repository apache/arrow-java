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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

/**
 * Verifies the direct-buffer paths of {@link FfmMemoryAccessor} work with no {@code --add-opens}
 * JVM directive at all, i.e. without reflective access into {@code java.nio} internals.
 *
 * <p>This only proves anything when the JVM really is started without {@code
 * --add-opens=java.base/java.nio=...}, so it runs in a dedicated Surefire execution (see
 * memory-ffm/pom.xml) that overrides the inherited argLine to be empty. It is excluded from the
 * default execution, which does inherit that flag.
 */
public class TestFfmMemoryAccessorWithoutAddOpens {

  @Test
  public void directBufferRoundTripsWithoutReflection() {
    long address = FfmMemoryAccessor.INSTANCE.allocateMemory(8);
    try {
      FfmMemoryAccessor.INSTANCE.putLong(address, 42L);

      ByteBuffer buf = FfmMemoryAccessor.INSTANCE.directBuffer(address, 8);
      assertTrue(buf.isDirect());
      assertEquals(42L, buf.order(ByteOrder.nativeOrder()).getLong(0));
      assertEquals(address, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(buf));

      buf.position(4);
      assertEquals(address, FfmMemoryAccessor.INSTANCE.getByteBufferAddress(buf));
    } finally {
      FfmMemoryAccessor.INSTANCE.freeMemory(address);
    }
  }
}
