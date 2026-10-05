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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.util.VisibleForTesting;

/** Utilities for memory related operations. */
public class MemoryUtil {

  /** If the native byte order is little-endian. */
  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  /**
   * The system property used to select the {@link MemoryUtilAccessor} implementation. When unset,
   * this defaults to FFM if {@link
   * DefaultAllocationManagerOption#ALLOCATION_MANAGER_TYPE_PROPERTY_NAME} (or its environment
   * variable) is set to {@code FFM}, so that selecting the FFM allocation manager avoids {@code
   * sun.misc.Unsafe} entirely unless this property overrides it.
   */
  public static final String MEMORY_ACCESSOR_TYPE_PROPERTY_NAME = "arrow.memory.accessor.type";

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MemoryUtil.class);

  private static final MemoryUtilAccessor ACCESSOR = resolveAccessor();

  private MemoryUtil() {}

  /** Returns the fully qualified class name of the {@link MemoryUtilAccessor} in use. */
  @VisibleForTesting
  public static String getAccessorClassName() {
    return ACCESSOR.getClass().getName();
  }

  private static MemoryUtilAccessor resolveAccessor() {
    String type = System.getProperty(MEMORY_ACCESSOR_TYPE_PROPERTY_NAME, "");
    if ("FFM".equals(type)) {
      logger.info(
          "{}=FFM, loading org.apache.arrow.memory.ffm.FfmMemoryAccessor",
          MEMORY_ACCESSOR_TYPE_PROPERTY_NAME);
      return loadFfmAccessor();
    }
    if (type.isEmpty()
        && DefaultAllocationManagerOption.getDefaultAllocationManagerType()
            == DefaultAllocationManagerOption.AllocationManagerType.FFM) {
      try {
        MemoryUtilAccessor accessor = loadFfmAccessor();
        logger.info(
            "{}=FFM, also loading org.apache.arrow.memory.ffm.FfmMemoryAccessor to avoid"
                + " sun.misc.Unsafe (override with {}=Unsafe if this is not wanted)",
            DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
            MEMORY_ACCESSOR_TYPE_PROPERTY_NAME);
        return accessor;
      } catch (RuntimeException e) {
        // Unlike an explicit arrow.memory.accessor.type=FFM request, this preference is only
        // inferred from a different property, which may not even be load-bearing (e.g. a
        // caller-supplied custom AllocationManager.Factory that never reads it). Fall back
        // instead of poisoning MemoryUtil's <clinit> for the rest of the JVM's life (JLS 12.4.2).
        logger.warn(
            "{}=FFM but arrow-memory-ffm is not on the classpath; falling back to Unsafe for {}"
                + " (set {}=Unsafe to silence this warning)",
            DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
            MEMORY_ACCESSOR_TYPE_PROPERTY_NAME,
            MEMORY_ACCESSOR_TYPE_PROPERTY_NAME,
            e);
      }
    }
    if (!"Unsafe".equals(type) && !type.isEmpty()) {
      logger.warn(
          "Unrecognized {}={}, falling back to Unsafe (valid values: Unsafe, FFM)",
          MEMORY_ACCESSOR_TYPE_PROPERTY_NAME,
          type);
    }
    return UnsafeMemoryAccessor.INSTANCE;
  }

  @SuppressWarnings({"nullness:argument", "nullness:return"})
  private static MemoryUtilAccessor loadFfmAccessor() {
    try {
      Field field =
          Class.forName("org.apache.arrow.memory.ffm.FfmMemoryAccessor")
              .getDeclaredField("INSTANCE");
      field.setAccessible(true);
      return (MemoryUtilAccessor) field.get(null);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(
          "Please add arrow-memory-ffm to your classpath,"
              + " no FfmMemoryAccessor found to satisfy "
              + MEMORY_ACCESSOR_TYPE_PROPERTY_NAME
              + "=FFM",
          e);
    }
  }

  /**
   * Given a {@link ByteBuffer}, gets the address the underlying memory space.
   *
   * @param buf the byte buffer.
   * @return address of the underlying memory.
   */
  public static long getByteBufferAddress(ByteBuffer buf) {
    return ACCESSOR.getByteBufferAddress(buf);
  }

  /** Create nio byte buffer. */
  public static ByteBuffer directBuffer(long address, int capacity) {
    return ACCESSOR.directBuffer(address, capacity);
  }

  public static void copyMemory(long srcAddress, long destAddress, long bytes) {
    ACCESSOR.copyMemory(srcAddress, destAddress, bytes);
  }

  public static void copyToMemory(byte[] src, long srcIndex, long destAddress, long bytes) {
    ACCESSOR.copyToMemory(src, srcIndex, destAddress, bytes);
  }

  public static void copyFromMemory(long srcAddress, byte[] dest, long destIndex, long bytes) {
    ACCESSOR.copyFromMemory(srcAddress, dest, destIndex, bytes);
  }

  public static byte getByte(long address) {
    return ACCESSOR.getByte(address);
  }

  public static void putByte(long address, byte value) {
    ACCESSOR.putByte(address, value);
  }

  public static short getShort(long address) {
    return ACCESSOR.getShort(address);
  }

  public static void putShort(long address, short value) {
    ACCESSOR.putShort(address, value);
  }

  public static int getInt(long address) {
    return ACCESSOR.getInt(address);
  }

  public static void putInt(long address, int value) {
    ACCESSOR.putInt(address, value);
  }

  public static long getLong(long address) {
    return ACCESSOR.getLong(address);
  }

  public static void putLong(long address, long value) {
    ACCESSOR.putLong(address, value);
  }

  public static void setMemory(long address, long bytes, byte value) {
    ACCESSOR.setMemory(address, bytes, value);
  }

  public static int getInt(byte[] bytes, int index) {
    return ACCESSOR.getInt(bytes, index);
  }

  public static long getLong(byte[] bytes, int index) {
    return ACCESSOR.getLong(bytes, index);
  }

  public static long allocateMemory(long bytes) {
    return ACCESSOR.allocateMemory(bytes);
  }

  /**
   * Frees native memory at the given address.
   *
   * <p>Behavior depends on the accessor selected by {@value #MEMORY_ACCESSOR_TYPE_PROPERTY_NAME}.
   * The default Unsafe accessor frees any valid native address, whatever allocated it. The FFM
   * accessor ({@code org.apache.arrow.memory.ffm.FfmMemoryAccessor}) only frees addresses that came
   * from its own {@link #allocateMemory}, because it releases the owning {@code Arena} rather than
   * the address; an address from any other source (JNI, a foreign {@code malloc}) is a silent no-op
   * there.
   */
  public static void freeMemory(long address) {
    ACCESSOR.freeMemory(address);
  }
}
