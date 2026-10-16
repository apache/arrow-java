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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TestMemoryUtilAccessorResolution {

  @Test
  public void unknownFfmAccessorTypeFailsWithActionableMessage() {
    // This test runs in its own JVM (see memory-core/pom.xml Surefire execution added below)
    // with -Darrow.memory.accessor.type=FFM and arrow-memory-ffm NOT on the classpath.
    //
    // MemoryUtil.getByte(0) triggers MemoryUtil class initialization for the first time in this
    // JVM, which resolves the accessor eagerly via a static final field. Since the resolution
    // failure happens inside <clinit>, the JVM wraps the RuntimeException thrown by
    // resolveAccessor()/loadFfmAccessor() in an ExceptionInInitializerError (JLS 12.4.2); the
    // original RuntimeException with the actionable message is available as its cause.
    Throwable thrown = assertThrows(Throwable.class, () -> MemoryUtil.getByte(0));
    Throwable cause = thrown instanceof ExceptionInInitializerError ? thrown.getCause() : thrown;
    assertTrue(cause instanceof RuntimeException);
    assertTrue(cause.getMessage() != null && cause.getMessage().contains("arrow-memory-ffm"));
  }
}
