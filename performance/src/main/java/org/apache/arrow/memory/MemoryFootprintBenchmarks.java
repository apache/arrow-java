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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks for memory footprint of Arrow memory objects.
 *
 * <p>This benchmark measures the heap memory overhead of creating many ArrowBuf instances. The
 * optimizations using AtomicFieldUpdater instead of AtomicLong/AtomicInteger objects should reduce
 * memory overhead significantly.
 *
 * <p>Expected savings per instance: - ArrowBuf: 8 bytes (id field removed) - BufferLedger: 28 bytes
 * (20 from AtomicInteger + 8 from ledgerId) - Accountant: 48 bytes (3 × 16 bytes from AtomicLong
 * objects)
 *
 * <p>For 1M ArrowBuf instances, this should save approximately 8 MB of heap memory.
 */
@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgs = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class MemoryFootprintBenchmarks {

  /** Number of ArrowBuf instances to create for memory footprint measurement. */
  private static final int NUM_BUFFERS = 100_000;

  /** Size in bytes of each buffer allocation. */
  private static final int BUFFER_SIZE = 1024;

  /** Root allocator used for all buffer allocations in the benchmark. */
  private RootAllocator allocator;

  /** Array to hold references to allocated buffers, preventing garbage collection. */
  private ArrowBuf[] buffers;

  /** JMX bean for querying heap memory usage statistics. */
  private MemoryMXBean memoryBean;

  /**
   * Sets up the benchmark state before each trial.
   *
   * <p>Initializes the memory monitoring bean, creates a root allocator with sufficient capacity,
   * and allocates the buffer reference array.
   */
  @Setup(Level.Trial)
  public void setup() {
    memoryBean = ManagementFactory.getMemoryMXBean();
    allocator = new RootAllocator((long) NUM_BUFFERS * BUFFER_SIZE);
    buffers = new ArrowBuf[NUM_BUFFERS];
  }

  /**
   * Cleans up buffers after each benchmark invocation.
   *
   * <p>Closes all allocated buffers to prevent memory leaks and ensure each iteration starts with a
   * clean slate. This is critical for the memory footprint benchmark which allocates many buffers
   * that would otherwise accumulate across warmup and measurement iterations.
   */
  @TearDown(Level.Invocation)
  public void tearDown() {
    for (int i = 0; i < NUM_BUFFERS; i++) {
      if (buffers[i] != null) {
        buffers[i].close();
        buffers[i] = null;
      }
    }
  }

  /**
   * Cleans up the allocator after the trial completes.
   *
   * <p>Closes the root allocator to release all resources after all warmup and measurement
   * iterations are complete.
   */
  @TearDown(Level.Trial)
  public void tearDownTrial() {
    allocator.close();
  }

  /**
   * Benchmark that measures heap memory usage when creating many ArrowBuf instances.
   *
   * <p>This benchmark creates {@value #NUM_BUFFERS} ArrowBuf instances and measures the heap memory
   * used. With the AtomicFieldUpdater optimizations, we expect to save approximately 800 KB of heap
   * memory (8 bytes × 100,000 instances) just from removing the id field in ArrowBuf.
   *
   * <p>The benchmark performs garbage collection before and after allocation to ensure accurate
   * measurement of heap memory delta. Results are printed to stdout for analysis.
   *
   * @return the total heap memory used by the allocated buffers in bytes
   */
  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public long measureArrowBufMemoryFootprint() {
    // Force GC before measurement
    System.gc();
    System.gc();
    System.gc();

    MemoryUsage heapBefore = memoryBean.getHeapMemoryUsage();
    long usedBefore = heapBefore.getUsed();

    // Allocate buffers
    for (int i = 0; i < NUM_BUFFERS; i++) {
      buffers[i] = allocator.buffer(BUFFER_SIZE);
    }

    // Force GC to get accurate measurement
    System.gc();
    System.gc();
    System.gc();

    MemoryUsage heapAfter = memoryBean.getHeapMemoryUsage();
    long usedAfter = heapAfter.getUsed();

    long memoryUsed = usedAfter - usedBefore;

    // Print memory usage for analysis
    System.out.printf(
        "Created %d ArrowBuf instances. Heap memory used: %d bytes (%.2f MB)%n",
        NUM_BUFFERS, memoryUsed, memoryUsed / (1024.0 * 1024.0));
    System.out.printf(
        "Average memory per ArrowBuf: %.2f bytes%n", (double) memoryUsed / NUM_BUFFERS);

    return memoryUsed;
  }

  /**
   * Benchmark that measures allocation and deallocation performance.
   *
   * <p>This complements the memory footprint benchmark by measuring the time it takes to allocate
   * and deallocate 1,000 buffers in a tight loop. This helps identify any performance regressions
   * introduced by memory optimizations.
   *
   * <p>Uses a local buffer array to avoid interference with the shared {@link #buffers} array used
   * by other benchmarks.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void measureAllocationPerformance() {
    ArrowBuf[] localBuffers = new ArrowBuf[1000];

    for (int i = 0; i < 1000; i++) {
      localBuffers[i] = allocator.buffer(BUFFER_SIZE);
    }

    for (int i = 0; i < 1000; i++) {
      localBuffers[i].close();
    }
  }

  /**
   * Main entry point for running the benchmarks standalone.
   *
   * <p>This allows running the benchmarks directly from the command line or IDE without using the
   * Maven JMH plugin. Example usage:
   *
   * <pre>{@code
   * java -cp target/benchmarks.jar org.apache.arrow.memory.MemoryFootprintBenchmarks
   * }</pre>
   *
   * @param args command line arguments (not used)
   * @throws RunnerException if the benchmark runner encounters an error
   */
  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(MemoryFootprintBenchmarks.class.getSimpleName())
            .forks(1)
            .build();

    new Runner(opt).run();
  }
}
