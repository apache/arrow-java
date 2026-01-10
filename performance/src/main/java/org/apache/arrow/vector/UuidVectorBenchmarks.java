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
package org.apache.arrow.vector;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.impl.UuidWriterImpl;
import org.apache.arrow.vector.holders.NullableUuidHolder;
import org.apache.arrow.vector.holders.UuidHolder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** Benchmarks for {@link UuidVector}. */
@State(Scope.Benchmark)
public class UuidVectorBenchmarks {
  // checkstyle:off: MissingJavadocMethod

  private static final int VECTOR_LENGTH = 10_000;

  private static final int ALLOCATOR_CAPACITY = 1024 * 1024;

  private BufferAllocator allocator;

  private UuidVector vector;

  private UUID[] testUuids;

  @Setup
  public void prepare() {
    allocator = new RootAllocator(ALLOCATOR_CAPACITY);
    vector = new UuidVector("vector", allocator);
    vector.allocateNew(VECTOR_LENGTH);
    vector.setValueCount(VECTOR_LENGTH);

    // Pre-generate UUIDs for consistent benchmarking
    testUuids = new UUID[VECTOR_LENGTH];
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      testUuids[i] = new UUID(i, i * 2L);
    }
  }

  @TearDown
  public void tearDown() {
    vector.close();
    allocator.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setWithUuidHolder() {
    UuidHolder holder = new UuidHolder();
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      // Old version: get the holder populated with buffer reference, then set it back
      vector.get(i, holder);
      vector.setSafe(i, holder);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setWithNullableUuidHolder() {
    NullableUuidHolder holder = new NullableUuidHolder();
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      // Old version: get the holder populated with buffer reference, then set it back
      holder.isSet = i % 3 == 0 ? 0 : 1;
      if (holder.isSet == 1) {
        vector.get(i, holder);
      }
      vector.setSafe(i, holder);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setUuidDirectly() {
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      vector.setSafe(i, testUuids[i]);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void setWithWriter() {
    UuidWriterImpl writer = new UuidWriterImpl(vector);
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      if (i % 3 != 0) {
        writer.writeExtension(testUuids[i]);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void getWithUuidHolder() {
    UuidHolder holder = new UuidHolder();
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      vector.get(i, holder);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void getWithNullableUuidHolder() {
    NullableUuidHolder holder = new NullableUuidHolder();
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      vector.get(i, holder);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void getUuidDirectly() {
    for (int i = 0; i < VECTOR_LENGTH; i++) {
      UUID uuid = vector.getObject(i);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
      new OptionsBuilder()
        .include(UuidVectorBenchmarks.class.getSimpleName())
        .forks(1)
        .addProfiler(GCProfiler.class)
        .build();

    new Runner(opt).run();
  }
  // checkstyle:on: MissingJavadocMethod
}
