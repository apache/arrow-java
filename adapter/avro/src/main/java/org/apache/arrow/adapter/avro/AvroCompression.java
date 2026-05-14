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
package org.apache.arrow.adapter.avro;

import java.nio.ByteBuffer;
import org.apache.avro.file.*;

public class AvroCompression {

  public static Codec getAvroCodec(String codecName) {

    if (codecName == null || DataFileConstants.NULL_CODEC.equals(codecName)) {
      return new NullCodec();
    }

    switch (codecName) {
      case DataFileConstants.DEFLATE_CODEC:
        return new DeflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
      case DataFileConstants.BZIP2_CODEC:
        return new BZip2Codec();
      case DataFileConstants.XZ_CODEC:
        return new XZCodec(CodecFactory.DEFAULT_XZ_LEVEL);
      case DataFileConstants.ZSTANDARD_CODEC:
        return new ZstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL, false, false);
    }

    throw new IllegalArgumentException("Unsupported codec: " + codecName);
  }

  private static class NullCodec extends Codec {

    @Override
    public String getName() {
      return DataFileConstants.NULL_CODEC;
    }

    @Override
    public ByteBuffer compress(ByteBuffer buffer) {
      return buffer;
    }

    @Override
    public ByteBuffer decompress(ByteBuffer buffer) {
      return buffer;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      return (other != null && other.getClass() == getClass());
    }

    @Override
    public int hashCode() {
      return 2;
    }
  }
}
