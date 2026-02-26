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
package org.apache.arrow.compression;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.xerial.snappy.Snappy;

/** Compression codec for the Snappy algorithm. */
public class SnappyCompressionCodec extends AbstractCompressionCodec {

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    Preconditions.checkArgument(
        uncompressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The uncompressed buffer size exceeds the integer limit %s.",
        Integer.MAX_VALUE);

    byte[] inBytes = new byte[(int) uncompressedBuffer.writerIndex()];
    uncompressedBuffer.getBytes(/* index= */ 0, inBytes);

    final byte[] outBytes;
    try {
      outBytes = Snappy.compress(inBytes);
    } catch (Exception e) {
      throw new RuntimeException("Error compressing with Snappy", e);
    }

    ArrowBuf compressedBuffer =
        allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    compressedBuffer.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes);
    compressedBuffer.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
    return compressedBuffer;
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    Preconditions.checkArgument(
        compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The compressed buffer size exceeds the integer limit %s",
        Integer.MAX_VALUE);

    long decompressedLength = readUncompressedLength(compressedBuffer);

    byte[] inBytes =
        new byte
            [(int) (compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
    compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, inBytes);

    final byte[] outBytes;
    try {
      outBytes = Snappy.uncompress(inBytes);
    } catch (Exception e) {
      throw new RuntimeException("Error decompressing with Snappy", e);
    }

    if (outBytes.length != decompressedLength) {
      throw new RuntimeException(
          "Expected != actual decompressed length: "
              + decompressedLength
              + " != "
              + outBytes.length);
    }

    ArrowBuf decompressedBuffer = allocator.buffer(decompressedLength);
    decompressedBuffer.setBytes(/* index= */ 0, outBytes);
    decompressedBuffer.writerIndex(decompressedLength);
    return decompressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.SNAPPY;
  }
}

