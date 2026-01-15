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
package org.apache.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestArrowMessageZeroCopy {

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testWrapGrpcBufferReturnsNullForRegularInputStream() throws IOException {
    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream stream = new ByteArrayInputStream(testData);

    // ByteArrayInputStream doesn't implement Detachable or HasByteBuffer
    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length);
    assertNull(result, "Should return null for streams not implementing required interfaces");
  }

  @Test
  public void testWrapGrpcBufferSucceedsForDirectBuffer() throws IOException {
    byte[] testData = new byte[] {11, 22, 33, 44, 55};
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(testData);

    assertInstanceOf(Detachable.class, stream, "Real gRPC stream should implement Detachable");
    assertInstanceOf(
        HasByteBuffer.class, stream, "Real gRPC stream should implement HasByteBuffer");
    assertTrue(
        ((HasByteBuffer) stream).byteBufferSupported(),
        "Direct buffer stream should support ByteBuffer");
    assertTrue(
        ((HasByteBuffer) stream).getByteBuffer().isDirect(),
        "Should have direct ByteBuffer backing");

    try (ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length)) {
      assertNotNull(result, "Should succeed for gRPC stream with direct buffer");
      assertEquals(testData.length, result.capacity());

      // Check received data is the same
      byte[] readData = new byte[testData.length];
      result.getBytes(0, readData);
      assertArrayEquals(testData, readData);
    }
  }

  @Test
  public void testWrapGrpcBufferReturnsNullForRealGrpcHeapByteBuffer() throws IOException {
    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream stream = MockGrpcInputStream.ofHeapBuffer(testData);

    assertInstanceOf(Detachable.class, stream, "Real gRPC stream should implement Detachable");
    assertInstanceOf(
        HasByteBuffer.class, stream, "Real gRPC stream should implement HasByteBuffer");
    assertTrue(
        ((HasByteBuffer) stream).byteBufferSupported(),
        "Heap ByteBuffer stream should support ByteBuffer");
    assertFalse(
        ((HasByteBuffer) stream).getByteBuffer().isDirect(), "Should have heap ByteBuffer backing");

    // Zero-copy should return null for heap buffer (not direct)
    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length);
    assertNull(result, "Should return null for gRPC stream with heap buffer");
  }

  @Test
  public void testWrapGrpcBufferReturnsNullWhenByteBufferNotSupported() throws IOException {
    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    InputStream stream = MockGrpcInputStream.withoutByteBufferSupport(testData);

    // Verify the stream has the expected gRPC interfaces
    assertInstanceOf(Detachable.class, stream, "Real gRPC stream should implement Detachable");
    assertInstanceOf(
        HasByteBuffer.class, stream, "Real gRPC stream should implement HasByteBuffer");
    // Byte array backed streams don't support ByteBuffer access
    assertFalse(
        ((HasByteBuffer) stream).byteBufferSupported(),
        "Byte array stream should not support ByteBuffer");

    // Zero-copy should return null when byteBufferSupported() is false
    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length);
    assertNull(result, "Should return null for gRPC stream without ByteBuffer support");
  }

  @Test
  public void testWrapGrpcBufferMemoryAccounting() throws IOException {
    byte[] testData = new byte[1024];
    new Random(42).nextBytes(testData);
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(testData);

    assertEquals(0, allocator.getAllocatedMemory());

    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length);
    assertNotNull(result, "Should succeed for gRPC stream with direct buffer");
    assertEquals(testData.length, allocator.getAllocatedMemory());

    byte[] readData = new byte[testData.length];
    result.getBytes(0, readData);
    assertArrayEquals(testData, readData);

    result.close();
    assertEquals(0, allocator.getAllocatedMemory());
  }

  @Test
  public void testWrapGrpcBufferReturnsNullForInsufficientData() throws IOException {
    byte[] testData = new byte[] {1, 2, 3};
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(testData);

    // Request more data than available
    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, 10);
    assertNull(result, "Should return null when buffer has insufficient data");
  }

  @Test
  public void testWrapGrpcBufferLargeData() throws IOException {
    byte[] testData = new byte[64 * 1024];
    new Random(42).nextBytes(testData);
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(testData);

    try (ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length)) {
      assertNotNull(result, "Should succeed for large data with real gRPC stream");
      assertEquals(testData.length, result.capacity());

      // Verify data integrity
      byte[] readData = new byte[testData.length];
      result.getBytes(0, readData);
      assertArrayEquals(testData, readData);
    }
  }

  /** Mock InputStream implementing gRPC's Detachable and HasByteBuffer for testing zero-copy. */
  private static class MockGrpcInputStream extends InputStream
      implements Detachable, HasByteBuffer {
    private ByteBuffer buffer;
    private final boolean byteBufferSupported;

    private MockGrpcInputStream(ByteBuffer buffer, boolean byteBufferSupported) {
      this.buffer = buffer;
      this.byteBufferSupported = byteBufferSupported;
    }

    static MockGrpcInputStream ofDirectBuffer(byte[] data) {
      ByteBuffer buf = ByteBuffer.allocateDirect(data.length);
      buf.put(data).flip();
      return new MockGrpcInputStream(buf, true);
    }

    static MockGrpcInputStream ofHeapBuffer(byte[] data) {
      return new MockGrpcInputStream(ByteBuffer.wrap(data), true);
    }

    static MockGrpcInputStream withoutByteBufferSupport(byte[] data) {
      return new MockGrpcInputStream(ByteBuffer.wrap(data), false);
    }

    @Override
    public boolean byteBufferSupported() {
      return byteBufferSupported;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      return byteBufferSupported ? buffer : null;
    }

    @Override
    public InputStream detach() {
      ByteBuffer detached = this.buffer;
      this.buffer = null;
      return new MockGrpcInputStream(detached, byteBufferSupported);
    }

    @Override
    public int read() {
      return (buffer != null && buffer.hasRemaining()) ? (buffer.get() & 0xFF) : -1;
    }

    @Override
    public void close() {
      buffer = null;
    }
  }
}
