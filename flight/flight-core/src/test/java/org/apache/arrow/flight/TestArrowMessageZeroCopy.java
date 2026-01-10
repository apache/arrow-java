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
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
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

  private static InputStream createGrpcStreamWithDirectBuffer(byte[] data) {
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(data.length);
    directBuffer.put(data);
    directBuffer.flip();
    ReadableBuffer readableBuffer = ReadableBuffers.wrap(directBuffer);
    return ReadableBuffers.openStream(readableBuffer, true);
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
  public void testWrapGrpcBufferSucceedsForRealGrpcDirectBuffer() throws IOException {
    byte[] testData = new byte[] {11, 22, 33, 44, 55};
    InputStream stream = createGrpcStreamWithDirectBuffer(testData);

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
      assertNotNull(result, "Should succeed for real gRPC stream with direct buffer");
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
    ByteBuffer heapBuffer = ByteBuffer.wrap(testData);
    ReadableBuffer readableBuffer = ReadableBuffers.wrap(heapBuffer);

    InputStream stream = ReadableBuffers.openStream(readableBuffer, true);

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
    assertNull(result, "Should return null for real gRPC stream with heap buffer");
  }

  @Test
  public void testWrapGrpcBufferReturnsNullForRealGrpcByteArrayStream() throws IOException {
    byte[] testData = new byte[] {1, 2, 3, 4, 5};
    ReadableBuffer readableBuffer = ReadableBuffers.wrap(testData);
    InputStream stream = ReadableBuffers.openStream(readableBuffer, true);

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
    assertNull(result, "Should return null for real gRPC stream backed by byte array");
  }

  @Test
  public void testWrapGrpcBufferMemoryAccountingWithRealGrpcStream() throws IOException {
    byte[] testData = new byte[1024];
    new Random(42).nextBytes(testData);
    InputStream stream = createGrpcStreamWithDirectBuffer(testData);

    long memoryBefore = allocator.getAllocatedMemory();
    assertEquals(0, memoryBefore);

    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length);
    assertNotNull(result, "Should succeed for real gRPC stream with direct buffer");

    long memoryDuring = allocator.getAllocatedMemory();
    assertEquals(testData.length, memoryDuring);

    byte[] readData = new byte[testData.length];
    result.getBytes(0, readData);
    assertArrayEquals(testData, readData);

    result.close();

    long memoryAfter = allocator.getAllocatedMemory();
    assertEquals(0, memoryAfter);
  }

  @Test
  public void testWrapGrpcBufferReturnsNullForInsufficientDataWithRealGrpcStream()
      throws IOException {
    byte[] testData = new byte[] {1, 2, 3};
    InputStream stream = createGrpcStreamWithDirectBuffer(testData);

    // Request more data than available
    ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, 10);
    assertNull(result, "Should return null when buffer has insufficient data");
  }

  @Test
  public void testWrapGrpcBufferLargeDataWithRealGrpcStream() throws IOException {
    // Test with larger data (64KB)
    byte[] testData = new byte[64 * 1024];
    new Random(42).nextBytes(testData);
    InputStream stream = createGrpcStreamWithDirectBuffer(testData);

    try (ArrowBuf result = ArrowMessage.wrapGrpcBuffer(stream, allocator, testData.length)) {
      assertNotNull(result, "Should succeed for large data with real gRPC stream");
      assertEquals(testData.length, result.capacity());

      // Verify data integrity
      byte[] readData = new byte[testData.length];
      result.getBytes(0, readData);
      assertArrayEquals(testData, readData);
    }
  }
}
