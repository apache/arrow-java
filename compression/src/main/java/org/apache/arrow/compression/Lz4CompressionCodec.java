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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

/**
 * Compression codec for the LZ4 frame format.
 *
 * <p>Each buffer is written as one frame of independent blocks of up to 4 MiB, without checksums,
 * which every LZ4 frame decoder reads. Compression, and the decompression of frames whose blocks
 * are independent, use lz4-java on the buffers' memory directly. A frame whose blocks are linked,
 * as Arrow C++ writes for a buffer larger than 64 KiB, or that uses a dictionary, is decompressed
 * with commons-compress, since lz4-java cannot decode a block that refers to earlier ones.
 */
public class Lz4CompressionCodec extends AbstractCompressionCodec {

  private static final long PREFIX = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;

  private static final int MAGIC = 0x184D2204;

  // The frame descriptor this codec writes: FLG (version 01, independent blocks), BD (4 MiB
  // maximum block size) and HC, the second byte of the xxHash32 of FLG and BD with a zero seed.
  private static final byte WRITE_FLG = 0x60;
  private static final byte WRITE_BD = 0x70;
  private static final byte WRITE_HC = 0x73;
  private static final int WRITE_BLOCK_SIZE = 4 * 1024 * 1024;

  private static final int FLG_VERSION_MASK = 0xC0;
  private static final int FLG_VERSION = 0x40;
  private static final int FLG_INDEPENDENT_BLOCKS = 0x20;
  private static final int FLG_BLOCK_CHECKSUM = 0x10;
  private static final int FLG_CONTENT_SIZE = 0x08;
  private static final int FLG_CONTENT_CHECKSUM = 0x04;
  private static final int FLG_RESERVED = 0x02;
  private static final int FLG_DICTIONARY_ID = 0x01;
  private static final int BD_RESERVED = 0x8F;

  // A block length's high bit marks a block stored uncompressed. A zero length ends the frame.
  private static final int UNCOMPRESSED_BLOCK = 0x80000000;

  // An LZ4 block cannot decompress to more than 255 times its compressed size.
  private static final long MAX_COMPRESSION_RATIO = 255;

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
    LZ4Compressor compressor = Lz4.COMPRESSOR;
    long length = uncompressedBuffer.writerIndex();
    long fullBlocks = length / WRITE_BLOCK_SIZE;
    int lastBlock = (int) (length % WRITE_BLOCK_SIZE);
    long bound =
        PREFIX
            + 7
            + fullBlocks * (Integer.BYTES + compressor.maxCompressedLength(WRITE_BLOCK_SIZE))
            + (lastBlock > 0 ? Integer.BYTES + compressor.maxCompressedLength(lastBlock) : 0)
            + Integer.BYTES;
    ArrowBuf frame = allocator.buffer(bound);
    try {
      setIntLE(frame, PREFIX, MAGIC);
      frame.setByte(PREFIX + 4, WRITE_FLG);
      frame.setByte(PREFIX + 5, WRITE_BD);
      frame.setByte(PREFIX + 6, WRITE_HC);
      long at = PREFIX + 7;
      for (long from = 0; from < length; ) {
        int n = (int) Math.min(WRITE_BLOCK_SIZE, length - from);
        int room = compressor.maxCompressedLength(n);
        int packed =
            compressor.compress(
                uncompressedBuffer.nioBuffer(from, n),
                0,
                n,
                frame.nioBuffer(at + Integer.BYTES, room),
                0,
                room);
        if (packed < n) {
          setIntLE(frame, at, packed);
          at += Integer.BYTES + packed;
        } else {
          setIntLE(frame, at, n | UNCOMPRESSED_BLOCK);
          frame.setBytes(at + Integer.BYTES, uncompressedBuffer, from, n);
          at += Integer.BYTES + n;
        }
        from += n;
      }
      setIntLE(frame, at, 0);
      frame.writerIndex(at + Integer.BYTES);
      return frame;
    } catch (RuntimeException | Error e) {
      frame.close();
      throw e;
    }
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
    long length = readUncompressedLength(compressedBuffer);
    long end = compressedBuffer.writerIndex();
    if (end - PREFIX < 7 || getIntLE(compressedBuffer, PREFIX) != MAGIC) {
      return decompressWithCommonsCompress(allocator, compressedBuffer);
    }
    int flg = compressedBuffer.getByte(PREFIX + 4) & 0xFF;
    int bd = compressedBuffer.getByte(PREFIX + 5) & 0xFF;
    int blockSizeId = (bd >>> 4) & 0x07;
    boolean contentChecksum = (flg & FLG_CONTENT_CHECKSUM) != 0;
    if ((flg & FLG_VERSION_MASK) != FLG_VERSION
        || (flg & (FLG_RESERVED | FLG_DICTIONARY_ID)) != 0
        || (flg & FLG_INDEPENDENT_BLOCKS) == 0
        || (bd & BD_RESERVED) != 0
        || blockSizeId < 4
        || (contentChecksum && length > Integer.MAX_VALUE)) {
      return decompressWithCommonsCompress(allocator, compressedBuffer);
    }
    boolean blockChecksum = (flg & FLG_BLOCK_CHECKSUM) != 0;
    int maxBlockSize = 1 << (8 + 2 * blockSizeId);

    long at = PREFIX + 6;
    if ((flg & FLG_CONTENT_SIZE) != 0) {
      if (end - at < Long.BYTES + 1) {
        throw corrupt("the frame descriptor is cut off");
      }
      long contentSize = getLongLE(compressedBuffer, at);
      if (contentSize != length) {
        throw corrupt(
            "the frame records a content size of "
                + contentSize
                + " bytes, but the buffer's decompressed length is "
                + length);
      }
      at += Long.BYTES;
    }
    int descriptorLength = (int) (at - (PREFIX + 4));
    int headerChecksum =
        (Lz4.XXHASH.hash(
                    compressedBuffer.nioBuffer(PREFIX + 4, descriptorLength), 0, descriptorLength, 0)
                >>> 8)
            & 0xFF;
    if ((compressedBuffer.getByte(at) & 0xFF) != headerChecksum) {
      throw corrupt("the frame descriptor's checksum does not match");
    }
    at += 1;

    // Check the blocks' lengths, and that they can hold the decompressed length, before
    // allocating for it.
    long blocksStart = at;
    long maxDecompressedLength = 0;
    while (true) {
      if (end - at < Integer.BYTES) {
        throw corrupt("the frame ends before its end mark");
      }
      int word = getIntLE(compressedBuffer, at);
      at += Integer.BYTES;
      if (word == 0) {
        break;
      }
      int n = word & ~UNCOMPRESSED_BLOCK;
      if (n > maxBlockSize) {
        throw corrupt("a block of " + n + " bytes is larger than the frame's maximum block size");
      }
      if (n > end - at) {
        throw corrupt("a block of " + n + " bytes runs past the end of the frame");
      }
      maxDecompressedLength +=
          (word & UNCOMPRESSED_BLOCK) != 0
              ? n
              : Math.min(maxBlockSize, n * MAX_COMPRESSION_RATIO);
      at += n;
      if (blockChecksum) {
        if (end - at < Integer.BYTES) {
          throw corrupt("the frame ends before a block's checksum");
        }
        at += Integer.BYTES;
      }
    }
    if (contentChecksum && end - at < Integer.BYTES) {
      throw corrupt("the frame ends before its content checksum");
    }
    if (length < 0 || length > maxDecompressedLength) {
      throw corrupt(
          "the frame decompresses to at most "
              + maxDecompressedLength
              + " bytes, not its recorded decompressed length of "
              + length);
    }

    ArrowBuf decompressedBuffer = allocator.buffer(length);
    try {
      LZ4SafeDecompressor decompressor = Lz4.DECOMPRESSOR;
      at = blocksStart;
      long to = 0;
      while (true) {
        int word = getIntLE(compressedBuffer, at);
        at += Integer.BYTES;
        if (word == 0) {
          break;
        }
        int n = word & ~UNCOMPRESSED_BLOCK;
        if (blockChecksum
            && Lz4.XXHASH.hash(compressedBuffer.nioBuffer(at, n), 0, n, 0)
                != getIntLE(compressedBuffer, at + n)) {
          throw corrupt("a block's checksum does not match");
        }
        if ((word & UNCOMPRESSED_BLOCK) != 0) {
          if (n > length - to) {
            throw corrupt("the frame holds more than its recorded decompressed length of " + length);
          }
          decompressedBuffer.setBytes(to, compressedBuffer, at, n);
          to += n;
        } else {
          int room = (int) Math.min(maxBlockSize, length - to);
          try {
            to +=
                decompressor.decompress(
                    compressedBuffer.nioBuffer(at, n),
                    0,
                    n,
                    decompressedBuffer.nioBuffer(to, room),
                    0,
                    room);
          } catch (LZ4Exception e) {
            throw corrupt("a block does not decompress within " + room + " bytes", e);
          }
        }
        at += n + (blockChecksum ? Integer.BYTES : 0);
      }
      if (to != length) {
        throw wrongLength(length, to);
      }
      if (contentChecksum
          && Lz4.XXHASH.hash(decompressedBuffer.nioBuffer(0, (int) length), 0, (int) length, 0)
              != getIntLE(compressedBuffer, at)) {
        throw corrupt("the frame's content checksum does not match");
      }
      decompressedBuffer.writerIndex(length);
      return decompressedBuffer;
    } catch (RuntimeException | Error e) {
      decompressedBuffer.close();
      throw e;
    }
  }

  private ArrowBuf decompressWithCommonsCompress(
      BufferAllocator allocator, ArrowBuf compressedBuffer) {
    Preconditions.checkArgument(
        compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
        "The compressed buffer size exceeds the integer limit %s",
        Integer.MAX_VALUE);

    long decompressedLength = readUncompressedLength(compressedBuffer);

    byte[] inBytes = new byte[(int) (compressedBuffer.writerIndex() - PREFIX)];
    compressedBuffer.getBytes(PREFIX, inBytes);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int) decompressedLength);
    try (InputStream in = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(inBytes))) {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    byte[] outBytes = out.toByteArray();
    if (outBytes.length != decompressedLength) {
      throw wrongLength(decompressedLength, outBytes.length);
    }
    ArrowBuf decompressedBuffer = allocator.buffer(outBytes.length);
    decompressedBuffer.setBytes(/* index= */ 0, outBytes);
    decompressedBuffer.writerIndex(decompressedLength);
    return decompressedBuffer;
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }

  private static RuntimeException corrupt(String reason) {
    return new IllegalArgumentException("Corrupt LZ4 frame: " + reason);
  }

  private static RuntimeException corrupt(String reason, Throwable cause) {
    return new IllegalArgumentException("Corrupt LZ4 frame: " + reason, cause);
  }

  private static RuntimeException wrongLength(long expected, long actual) {
    return new RuntimeException(
        "Expected != actual decompressed length: " + expected + " != " + actual);
  }

  private static int getIntLE(ArrowBuf buf, long index) {
    int value = buf.getInt(index);
    return MemoryUtil.LITTLE_ENDIAN ? value : Integer.reverseBytes(value);
  }

  private static long getLongLE(ArrowBuf buf, long index) {
    long value = buf.getLong(index);
    return MemoryUtil.LITTLE_ENDIAN ? value : Long.reverseBytes(value);
  }

  private static void setIntLE(ArrowBuf buf, long index, int value) {
    buf.setInt(index, MemoryUtil.LITTLE_ENDIAN ? value : Integer.reverseBytes(value));
  }

  /** lz4-java's implementations, loaded on first use. */
  private static final class Lz4 {
    // fastestInstance() picks the JNI implementations only when lz4-java was loaded by the system
    // class loader, and otherwise settles for the Java ones, so ask for the JNI ones directly.
    static final LZ4Factory FACTORY = lz4Factory();
    static final LZ4Compressor COMPRESSOR = FACTORY.fastCompressor();
    static final LZ4SafeDecompressor DECOMPRESSOR = FACTORY.safeDecompressor();
    static final XXHash32 XXHASH = xxHashFactory().hash32();

    private static LZ4Factory lz4Factory() {
      try {
        return LZ4Factory.nativeInstance();
      } catch (LinkageError | RuntimeException e) {
        return LZ4Factory.fastestJavaInstance();
      }
    }

    private static XXHashFactory xxHashFactory() {
      try {
        return XXHashFactory.nativeInstance();
      } catch (LinkageError | RuntimeException e) {
        return XXHashFactory.fastestJavaInstance();
      }
    }
  }
}
