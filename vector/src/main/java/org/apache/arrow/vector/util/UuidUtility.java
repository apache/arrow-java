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

public class UuidUtility {
  /**
   * Converts UUID to byte array
   *
   * @param uuid UUID object
   * @return uuid as byte array
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

  public static UUID uuidFromArrowBuf(ArrowBuf buffer, long index) {
    ByteBuffer buf = buffer.nioBuffer(index, UUID_BYTE_WIDTH);

    buf.order(ByteOrder.BIG_ENDIAN);
    long mostSigBits = buf.getLong(0);
    long leastSigBits = buf.getLong(Long.BYTES);
    return new UUID(mostSigBits, leastSigBits);
  }
}
