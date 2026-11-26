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

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** Benchmarks for the {@link CompressionCodec}s of {@link CommonsCompressionFactory}. */
@State(Scope.Benchmark)
public class CompressionCodecBenchmarks {
  // checkstyle:off: MissingJavadocMethod

  private static final int BUFFER_LENGTH = 8 * 1024 * 1024;

  @Param({"LZ4_FRAME", "ZSTD"})
  public CompressionUtil.CodecType codecType;

  /** {@code int64}: little-endian longs that count up. {@code text}: random decimal numbers. */
  @Param({"int64", "text"})
  public String data;

  private BufferAllocator allocator;

  private CompressionCodec codec;

  private ArrowBuf uncompressed;

  private ArrowBuf compressed;

  /** Setup benchmarks. */
  @Setup(Level.Trial)
  public void prepare() {
    allocator = new RootAllocator();
    codec = CommonsCompressionFactory.INSTANCE.createCodec(codecType);
    uncompressed = allocator.buffer(BUFFER_LENGTH);
    if (data.equals("int64")) {
      for (int i = 0; i < BUFFER_LENGTH / Long.BYTES; i++) {
        uncompressed.setLong((long) i * Long.BYTES, i);
      }
    } else {
      Random random = new Random(0);
      StringBuilder text = new StringBuilder(BUFFER_LENGTH + 8);
      while (text.length() < BUFFER_LENGTH) {
        text.append(random.nextInt(100_000)).append(',');
      }
      uncompressed.setBytes(
          0, text.substring(0, BUFFER_LENGTH).getBytes(StandardCharsets.US_ASCII));
    }
    uncompressed.writerIndex(BUFFER_LENGTH);
    compressed = compress();
  }

  /** Tear down benchmarks. */
  @TearDown(Level.Trial)
  public void tearDown() {
    compressed.close();
    uncompressed.close();
    allocator.close();
  }

  private ArrowBuf compress() {
    // compress() releases its input, so give it a reference of its own.
    uncompressed.getReferenceManager().retain();
    return codec.compress(allocator, uncompressed);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void compressBenchmark() {
    compress().close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void decompressBenchmark() {
    // decompress() releases its input, so give it a reference of its own.
    compressed.getReferenceManager().retain();
    codec.decompress(allocator, compressed).close();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(CompressionCodecBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
  // checkstyle:on: MissingJavadocMethod
}
