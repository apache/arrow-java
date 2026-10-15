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
package org.apache.arrow.memory;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for reference counting in {@link BufferLedger}: retain and release on one buffer from
 * one thread (the common case) and from several threads at once, retaining a buffer into another
 * allocator, and buffer allocation for context.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BufferLedgerBenchmarks {

  private static final int BUFFER_SIZE = 1024;

  private BufferAllocator allocator;

  private BufferAllocator otherAllocator;

  private ArrowBuf buffer;

  /** Setup benchmarks. */
  @Setup
  public void prepare() {
    allocator = new RootAllocator();
    otherAllocator = allocator.newChildAllocator("other", 0, Long.MAX_VALUE);
    buffer = allocator.buffer(BUFFER_SIZE);
  }

  /** Tear down benchmarks. */
  @TearDown
  public void tearDown() {
    buffer.close();
    otherAllocator.close();
    allocator.close();
  }

  private boolean retainRelease() {
    ReferenceManager referenceManager = buffer.getReferenceManager();
    referenceManager.retain();
    return referenceManager.release();
  }

  @Benchmark
  @Threads(1)
  public boolean retainReleaseUncontended() {
    return retainRelease();
  }

  @Benchmark
  @Threads(2)
  public boolean retainReleaseContended2() {
    return retainRelease();
  }

  @Benchmark
  @Threads(4)
  public boolean retainReleaseContended4() {
    return retainRelease();
  }

  @Benchmark
  @Threads(8)
  public boolean retainReleaseContended8() {
    return retainRelease();
  }

  /** Retain the buffer into another allocator and close the new buffer. */
  @Benchmark
  @Threads(1)
  public long retainToAllocatorClose() {
    try (ArrowBuf retained = buffer.getReferenceManager().retain(buffer, otherAllocator)) {
      return retained.memoryAddress();
    }
  }

  /** Allocate and close a buffer, for comparison with the cost of a retain and release. */
  @Benchmark
  @Threads(1)
  public long allocateClose() {
    try (ArrowBuf buf = allocator.buffer(BUFFER_SIZE)) {
      return buf.memoryAddress();
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BufferLedgerBenchmarks.class.getSimpleName()).forks(1).build();

    new Runner(opt).run();
  }
}
