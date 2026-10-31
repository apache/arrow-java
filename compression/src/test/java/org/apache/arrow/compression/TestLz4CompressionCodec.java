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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Stream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream.BlockSize;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Test cases for {@link Lz4CompressionCodec}'s LZ4 frames. */
class TestLz4CompressionCodec {
  private static final int MIB = 1024 * 1024;

  // Long enough for several 64 KiB blocks.
  private static final int OTHER_WRITER_LENGTH = 200 * 1024;

  private final CompressionCodec codec = new Lz4CompressionCodec();
  private BufferAllocator allocator;

  @BeforeEach
  void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  void terminate() {
    allocator.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 1000, 64 * 1024 + 1, 4 * MIB, 4 * MIB + 1, 9 * MIB})
  void testRoundTrip(int length) throws IOException {
    checkRoundTrip(compressible(length));
  }

  @Test
  void testRoundTripWithStoredBlock() throws IOException {
    // The first 4 MiB block is incompressible, so it is stored, while the rest compresses.
    byte[] data = new byte[9 * MIB];
    byte[] noise = new byte[4 * MIB];
    new Random(42).nextBytes(noise);
    System.arraycopy(noise, 0, data, 0, noise.length);
    checkRoundTrip(data);
  }

  /** Round-trips {@code data}, and checks that other decoders read the frame it compresses to. */
  private void checkRoundTrip(byte[] data) throws IOException {
    ArrowBuf compressed = codec.compress(allocator, toArrowBuf(data));
    // A buffer that does not compress is sent as it is, without a frame.
    if (compressed.getLong(0) != CompressionUtil.NO_COMPRESSION_LENGTH) {
      byte[] frame = new byte[(int) (compressed.writerIndex() - Long.BYTES)];
      compressed.getBytes(Long.BYTES, frame);
      try (InputStream in = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(frame))) {
        assertArrayEquals(data, in.readAllBytes());
      }
      try (InputStream in = new LZ4FrameInputStream(new ByteArrayInputStream(frame))) {
        assertArrayEquals(data, in.readAllBytes());
      }
    }
    assertArrayEquals(data, decompress(compressed));
  }

  static Stream<Arguments> otherWriters() {
    List<Arguments> writers = new ArrayList<>();
    for (BlockSize blockSize : BlockSize.values()) {
      for (boolean linked : new boolean[] {false, true}) {
        for (boolean checksums : new boolean[] {false, true}) {
          FramedLZ4CompressorOutputStream.Parameters parameters =
              new FramedLZ4CompressorOutputStream.Parameters(
                  blockSize, checksums, checksums, linked);
          writers.add(
              Arguments.of(
                  "commons-compress " + parameters,
                  (Writer) out -> new FramedLZ4CompressorOutputStream(out, parameters)));
        }
      }
    }
    writers.add(
        Arguments.of(
            "lz4-java with content size and block checksums",
            (Writer)
                out ->
                    new LZ4FrameOutputStream(
                        out,
                        LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
                        OTHER_WRITER_LENGTH,
                        LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                        LZ4FrameOutputStream.FLG.Bits.BLOCK_CHECKSUM,
                        LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE)));
    return writers.stream();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("otherWriters")
  void testDecompressOtherWriters(String name, Writer writer) throws IOException {
    byte[] data = repetitive(OTHER_WRITER_LENGTH);
    assertArrayEquals(data, decompress(frameOf(data, writer)));
  }

  static Stream<Arguments> corruptions() {
    // Offsets are into the buffer: the 8-byte length prefix, then the frame, whose descriptor
    // is FLG, BD and HC after the 4-byte magic number. The first block's length follows at 15.
    return Stream.of(
        corruption("a wrong descriptor checksum", buf -> buf.setByte(14, buf.getByte(14) ^ 1)),
        corruption("no end mark", buf -> buf.writerIndex(buf.writerIndex() - Integer.BYTES)),
        corruption("a block that runs past the frame", buf -> buf.setInt(15, 1 << 20)),
        corruption("a block larger than the maximum", buf -> buf.setInt(15, 5 * MIB)),
        corruption("compressed bytes that do not decode", buf -> buf.setLong(19, -1L)),
        corruption("a recorded length that the frame cannot hold", buf -> buf.setLong(0, MIB)),
        corruption("a recorded length that is too short", buf -> buf.setLong(0, 1000)));
  }

  private static Arguments corruption(String name, Consumer<ArrowBuf> corrupt) {
    return Arguments.of(name, corrupt);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("corruptions")
  void testDecompressCorruptFrame(String name, Consumer<ArrowBuf> corrupt) {
    ArrowBuf compressed = codec.compress(allocator, toArrowBuf(compressible(4096)));
    corrupt.accept(compressed);
    assertThrows(RuntimeException.class, () -> codec.decompress(allocator, compressed));
    compressed.close();
  }

  @Test
  void testDecompressWrongChecksums() throws IOException {
    byte[] data = compressible(4096);
    Writer writer =
        out ->
            new FramedLZ4CompressorOutputStream(
                out,
                new FramedLZ4CompressorOutputStream.Parameters(BlockSize.K64, true, true, false));
    // The frame ends with a block's checksum, the end mark and the content checksum.
    ArrowBuf block = frameOf(data, writer);
    block.setByte(block.writerIndex() - 12, block.getByte(block.writerIndex() - 12) ^ 1);
    RuntimeException e =
        assertThrows(RuntimeException.class, () -> codec.decompress(allocator, block));
    assertTrue(e.getMessage().contains("block's checksum"), e.getMessage());
    block.close();

    ArrowBuf content = frameOf(data, writer);
    content.setByte(content.writerIndex() - 1, content.getByte(content.writerIndex() - 1) ^ 1);
    e = assertThrows(RuntimeException.class, () -> codec.decompress(allocator, content));
    assertTrue(e.getMessage().contains("content checksum"), e.getMessage());
    content.close();
  }

  @Test
  void testDecompressWrongContentSize() throws IOException {
    byte[] data = compressible(4096);
    ArrowBuf compressed =
        frameOf(
            data,
            out ->
                new LZ4FrameOutputStream(
                    out,
                    LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
                    data.length,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                    LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE));
    compressed.setLong(0, data.length - 1);
    RuntimeException e =
        assertThrows(RuntimeException.class, () -> codec.decompress(allocator, compressed));
    assertTrue(e.getMessage().contains("content size"), e.getMessage());
    compressed.close();
  }

  /** Opens a stream that writes an LZ4 frame. */
  @FunctionalInterface
  interface Writer {
    OutputStream open(OutputStream out) throws IOException;
  }

  /** {@code data} compressed by {@code writer}, behind Arrow's uncompressed-length prefix. */
  private ArrowBuf frameOf(byte[] data, Writer writer) throws IOException {
    ByteArrayOutputStream frame = new ByteArrayOutputStream();
    try (OutputStream out = writer.open(frame)) {
      out.write(data);
    }
    byte[] bytes = frame.toByteArray();
    ArrowBuf buf = allocator.buffer(Long.BYTES + bytes.length);
    buf.setLong(0, data.length);
    buf.setBytes(Long.BYTES, bytes);
    buf.writerIndex(Long.BYTES + bytes.length);
    return buf;
  }

  private byte[] decompress(ArrowBuf compressed) {
    try (ArrowBuf decompressed = codec.decompress(allocator, compressed)) {
      byte[] bytes = new byte[(int) decompressed.writerIndex()];
      decompressed.getBytes(0, bytes);
      return bytes;
    }
  }

  private ArrowBuf toArrowBuf(byte[] data) {
    ArrowBuf buf = allocator.buffer(data.length);
    buf.setBytes(0, data);
    buf.writerIndex(data.length);
    return buf;
  }

  /**
   * Decimal numbers that repeat every thousand, for commons-compress, whose compressor takes
   * seconds per 100 KiB of input with many short matches, such as {@link #compressible}'s.
   */
  private static byte[] repetitive(int length) {
    StringBuilder text = new StringBuilder(length + 8);
    for (int i = 0; text.length() < length; i++) {
      text.append(i % 1000).append(',');
    }
    return Arrays.copyOf(text.toString().getBytes(StandardCharsets.US_ASCII), length);
  }

  /** Random decimal numbers as text, which compress well but not trivially. */
  private static byte[] compressible(int length) {
    Random random = new Random(length);
    StringBuilder text = new StringBuilder(length + 8);
    while (text.length() < length) {
      text.append(random.nextInt(100_000)).append(',');
    }
    return Arrays.copyOf(text.toString().getBytes(StandardCharsets.US_ASCII), length);
  }
}
