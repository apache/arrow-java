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
package org.apache.arrow.vector.util;

import static org.apache.arrow.vector.extension.UuidType.UUID_BYTE_WIDTH;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Utility class for UUID conversions and operations.
 *
 * <p>Provides methods to convert between {@link UUID} objects and byte representations used in
 * Arrow vectors.
 *
 * @see org.apache.arrow.vector.UuidVector
 * @see org.apache.arrow.vector.extension.UuidType
 */
public class UuidUtility {
  /**
   * Converts a UUID to a 16-byte array.
   *
   * <p>The UUID is stored in big-endian byte order, with the most significant bits first.
   *
   * @param uuid the UUID to convert
   * @return a 16-byte array representing the UUID
   */
  public static byte[] getBytesFromUUID(UUID uuid) {
    byte[] result = new byte[16];
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    for (int i = 15; i >= 8; i--) {
      result[i] = (byte) (lsb & 0xFF);
      lsb >>= 8;
    }
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte) (msb & 0xFF);
      msb >>= 8;
    }
    return result;
  }

  /**
   * Constructs a UUID from bytes stored in an ArrowBuf at the specified index.
   *
   * <p>Reads 16 bytes from the buffer starting at the given index and interprets them as a UUID in
   * big-endian byte order.
   *
   * @param buffer the buffer containing UUID data
   * @param index the byte offset in the buffer where the UUID starts
   * @return the UUID constructed from the buffer data
   */
  public static UUID uuidFromArrowBuf(ArrowBuf buffer, long index) {
    ByteBuffer buf = buffer.nioBuffer(index, UUID_BYTE_WIDTH);

    buf.order(ByteOrder.BIG_ENDIAN);
    long mostSigBits = buf.getLong(0);
    long leastSigBits = buf.getLong(Long.BYTES);
    return new UUID(mostSigBits, leastSigBits);
  }
}
