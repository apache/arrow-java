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
package org.apache.arrow.flight.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import io.grpc.internal.ReadableBuffer;
import io.grpc.internal.ReadableBuffers;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

/** Tests for {@link GetReadableBuffer}. */
public class TestGetReadableBuffer {

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  /** Check if the HasByteBuffer API is enabled (new implementation). */
  static boolean isHasByteBufferApiEnabled() {
    return GetReadableBuffer.isHasByteBufferApiEnabled();
  }

  /** Check if the legacy reflection-based implementation is enabled. */
  static boolean isLegacyReflectionEnabled() {
    return !GetReadableBuffer.isHasByteBufferApiEnabled();
  }

  // --- Feature Flag Tests ---

  @Test
  public void testFeatureFlag_isConsistentWithSystemProperty() {
    String propertyValue = System.getProperty(GetReadableBuffer.HASBYTEBUFFER_API_PROPERTY, "true");
    boolean expectedEnabled = !"false".equalsIgnoreCase(propertyValue);
    assertEquals(expectedEnabled, GetReadableBuffer.isHasByteBufferApiEnabled());
  }

  // --- Slow Path Tests (work with both implementations) ---

  @Test
  public void testSlowPath_regularInputStream() throws IOException {
    byte[] testData = {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream stream = new ByteArrayInputStream(testData);

    try (ArrowBuf buf = allocator.buffer(testData.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, false);

      assertEquals(testData.length, buf.writerIndex());
      byte[] result = new byte[testData.length];
      buf.getBytes(0, result);
      assertArrayEquals(testData, result);
    }
  }

  @Test
  public void testSlowPath_fastPathDisabled() throws IOException {
    byte[] testData = {10, 20, 30, 40};
    HasByteBufferInputStream stream =
        new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true);

    try (ArrowBuf buf = allocator.buffer(testData.length)) {
      // fastPath=false should force slow path even with HasByteBuffer stream
      GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, false);

      assertEquals(testData.length, buf.writerIndex());
      byte[] result = new byte[testData.length];
      buf.getBytes(0, result);
      assertArrayEquals(testData, result);
    }
  }

  @Test
  public void testSlowPath_byteBufferNotSupported() throws IOException {
    byte[] testData = {100, (byte) 200, 50, 75};
    HasByteBufferInputStream stream =
        new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), false);

    try (ArrowBuf buf = allocator.buffer(testData.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, true);

      assertEquals(testData.length, buf.writerIndex());
      byte[] result = new byte[testData.length];
      buf.getBytes(0, result);
      assertArrayEquals(testData, result);
    }
  }

  @Test
  public void testSlowPath_emptyBuffer() throws IOException {
    InputStream stream = new ByteArrayInputStream(new byte[0]);

    try (ArrowBuf buf = allocator.buffer(8)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, 0, false);
      assertEquals(0, buf.writerIndex());
    }
  }

  @Test
  public void testDataIntegrity_writerIndexSet() throws IOException {
    byte[] testData = {1, 2, 3, 4, 5, 6, 7, 8};
    InputStream stream = new ByteArrayInputStream(testData);

    try (ArrowBuf buf = allocator.buffer(16)) {
      buf.writerIndex(4);
      GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, false);
      assertEquals(testData.length, buf.writerIndex());
    }
  }

  // --- Fast Path Tests (only run when HasByteBuffer API is enabled) ---

  @Nested
  @EnabledIf("org.apache.arrow.flight.grpc.TestGetReadableBuffer#isHasByteBufferApiEnabled")
  class HasByteBufferApiTests {

    @Test
    public void testFastPath_singleByteBuffer() throws IOException {
      byte[] testData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      HasByteBufferInputStream stream =
          new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true);

      try (ArrowBuf buf = allocator.buffer(testData.length)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, true);

        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    public void testFastPath_multipleByteBuffers() throws IOException {
      byte[] part1 = {1, 2, 3, 4};
      byte[] part2 = {5, 6, 7, 8};
      byte[] part3 = {9, 10};
      byte[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

      List<ByteBuffer> buffers = new ArrayList<>();
      buffers.add(ByteBuffer.wrap(part1));
      buffers.add(ByteBuffer.wrap(part2));
      buffers.add(ByteBuffer.wrap(part3));
      HasByteBufferInputStream stream = new HasByteBufferInputStream(buffers, true);

      try (ArrowBuf buf = allocator.buffer(expected.length)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, expected.length, true);

        assertEquals(expected.length, buf.writerIndex());
        byte[] result = new byte[expected.length];
        buf.getBytes(0, result);
        assertArrayEquals(expected, result);
      }
    }

    @Test
    public void testFastPath_emptyBuffer() throws IOException {
      HasByteBufferInputStream stream = new HasByteBufferInputStream(List.of(), true);

      try (ArrowBuf buf = allocator.buffer(8)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, 0, true);
        assertEquals(0, buf.writerIndex());
      }
    }

    @Test
    public void testFastPath_partialByteBuffer() throws IOException {
      byte[] part1 = {1, 2, 3};
      byte[] part2 = {4, 5, 6, 7, 8};
      byte[] expected = {1, 2, 3, 4, 5};

      List<ByteBuffer> buffers = new ArrayList<>();
      buffers.add(ByteBuffer.wrap(part1));
      buffers.add(ByteBuffer.wrap(part2));
      HasByteBufferInputStream stream = new HasByteBufferInputStream(buffers, true);

      try (ArrowBuf buf = allocator.buffer(expected.length)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, expected.length, true);

        assertEquals(expected.length, buf.writerIndex());
        byte[] result = new byte[expected.length];
        buf.getBytes(0, result);
        assertArrayEquals(expected, result);
      }
    }

    @Test
    public void testFastPath_largeData() throws IOException {
      int size = 64 * 1024;
      byte[] testData = new byte[size];
      for (int i = 0; i < size; i++) {
        testData[i] = (byte) (i % 256);
      }

      List<ByteBuffer> buffers = new ArrayList<>();
      int chunkSize = 8 * 1024;
      for (int offset = 0; offset < size; offset += chunkSize) {
        int len = Math.min(chunkSize, size - offset);
        byte[] chunk = new byte[len];
        System.arraycopy(testData, offset, chunk, 0, len);
        buffers.add(ByteBuffer.wrap(chunk));
      }
      HasByteBufferInputStream stream = new HasByteBufferInputStream(buffers, true);

      try (ArrowBuf buf = allocator.buffer(size)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, size, true);

        assertEquals(size, buf.writerIndex());
        byte[] result = new byte[size];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    public void testErrorHandling_unexpectedEndOfStream() {
      byte[] testData = {1, 2, 3};
      HasByteBufferInputStream stream =
          new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true);

      try (ArrowBuf buf = allocator.buffer(10)) {
        assertThrows(
            IOException.class, () -> GetReadableBuffer.readIntoBuffer(stream, buf, 10, true));
      }
    }

    @Test
    public void testErrorHandling_skipFailure() {
      byte[] testData = {1, 2, 3, 4, 5};
      HasByteBufferInputStream stream =
          new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true, true);

      try (ArrowBuf buf = allocator.buffer(testData.length)) {
        assertThrows(
            IOException.class,
            () -> GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, true));
      }
    }

    @Test
    public void testDataIntegrity_offsetWriting() throws IOException {
      byte[] testData = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
      HasByteBufferInputStream stream =
          new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true);

      try (ArrowBuf buf = allocator.buffer(16)) {
        for (int i = 0; i < 16; i++) {
          buf.setByte(i, 0xFF);
        }

        GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, true);

        assertEquals((byte) 0xDE, buf.getByte(0));
        assertEquals((byte) 0xAD, buf.getByte(1));
        assertEquals((byte) 0xBE, buf.getByte(2));
        assertEquals((byte) 0xEF, buf.getByte(3));
        assertEquals(testData.length, buf.writerIndex());
      }
    }

    /**
     * Verifies the HasByteBuffer data transfer path using the actual gRPC {@link
     * ReadableBuffers#openStream} implementation.
     *
     * <p>This test uses the real gRPC BufferInputStream class (via ReadableBuffers.openStream) to
     * ensure our code works correctly with actual gRPC streams, not just custom test helpers.
     */
    @Test
    public void testHasByteBufferPath_dataTransferWithObjectTracking() throws IOException {
      // Create wrapper objects as the conceptual "source" of our data
      // Each ByteWrapper is a distinct Java object instance we can track
      final ByteWrapper[] sourceObjects = {
        new ByteWrapper((byte) 1),
        new ByteWrapper((byte) 2),
        new ByteWrapper((byte) 3),
        new ByteWrapper((byte) 4),
        new ByteWrapper((byte) 5),
        new ByteWrapper((byte) 6),
        new ByteWrapper((byte) 7),
        new ByteWrapper((byte) 8)
      };

      // Extract byte values from wrapper objects into the backing array
      final byte[] backingArrayRef = new byte[sourceObjects.length];
      for (int i = 0; i < sourceObjects.length; i++) {
        backingArrayRef[i] = sourceObjects[i].getValue();
      }

      // Create ByteBuffer that wraps the backing array
      final ByteBuffer byteBufferRef = ByteBuffer.wrap(backingArrayRef);

      // Verify initial object relationships
      assertTrue(byteBufferRef.hasArray(), "ByteBuffer should be backed by an array");
      assertSame(
          backingArrayRef,
          byteBufferRef.array(),
          "ByteBuffer.array() must return the exact same byte[] instance");

      // Create stream using actual gRPC ReadableBuffers implementation
      // ReadableBuffers.wrap() creates a ReadableBuffer from a ByteBuffer
      // ReadableBuffers.openStream() creates a BufferInputStream that implements HasByteBuffer
      ReadableBuffer readableBuffer = ReadableBuffers.wrap(byteBufferRef);
      InputStream stream = ReadableBuffers.openStream(readableBuffer, true);

      // Verify the stream implements HasByteBuffer (required for the fast path)
      assertTrue(
          stream instanceof HasByteBuffer, "gRPC BufferInputStream should implement HasByteBuffer");
      assertTrue(
          ((HasByteBuffer) stream).byteBufferSupported(),
          "gRPC BufferInputStream should support byteBuffer");

      try (ArrowBuf buf = allocator.buffer(backingArrayRef.length)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, backingArrayRef.length, true);

        // Verify data transfer
        assertEquals(backingArrayRef.length, buf.writerIndex());
        byte[] result = new byte[backingArrayRef.length];
        buf.getBytes(0, result);
        assertArrayEquals(backingArrayRef, result);

        // VERIFICATION: Check that source objects are preserved and data transferred correctly

        // 1. The source wrapper objects are unchanged and still accessible with same identity
        for (int i = 0; i < sourceObjects.length; i++) {
          assertSame(
              sourceObjects[i],
              sourceObjects[i],
              "Source wrapper object at index " + i + " must retain identity");
          assertEquals(
              sourceObjects[i].getValue(),
              backingArrayRef[i],
              "Wrapper value at index " + i + " must match backing array");
        }

        // 2. The original backing array reference is preserved in the original ByteBuffer
        assertSame(
            backingArrayRef,
            byteBufferRef.array(),
            "Original ByteBuffer's backing array must be the same instance");

        // 3. Verify ArrowBuf received a copy (data independence)
        byte[] originalValues = result.clone();
        backingArrayRef[0] = 99;
        buf.getBytes(0, result);
        assertArrayEquals(originalValues, result, "ArrowBuf data should be independent of source");

        // 4. Verify modifying backing array doesn't affect wrapper objects
        assertEquals(
            (byte) 1,
            sourceObjects[0].getValue(),
            "Wrapper objects should be independent of backing array modifications");
      }
    }
  }

  /** Wrapper class that holds a byte value as a distinct Java object for reference tracking. */
  private static final class ByteWrapper {
    private final byte value;

    ByteWrapper(byte value) {
      this.value = value;
    }

    byte getValue() {
      return value;
    }
  }

  // --- Legacy Reflection Tests (only run when HasByteBuffer API is disabled) ---

  /**
   * Tests for the legacy reflection-based implementation. These tests only run when the
   * HasByteBuffer API is disabled via the system property {@code
   * arrow.flight.grpc.enable_hasbytebuffer_api=false}.
   */
  @Nested
  @EnabledIf("org.apache.arrow.flight.grpc.TestGetReadableBuffer#isLegacyReflectionEnabled")
  class LegacyReflectionTests {

    @Test
    public void testFastPath_fallsBackToSlowPath_withRegularInputStream() throws IOException {
      // When fastPath=true but stream is not BufferInputStream, should use slow path
      byte[] testData = {1, 2, 3, 4, 5};
      InputStream stream = new ByteArrayInputStream(testData);

      try (ArrowBuf buf = allocator.buffer(testData.length)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, true);

        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    public void testFastPath_fallsBackToSlowPath_withHasByteBufferStream() throws IOException {
      // When fastPath=true but HasByteBuffer API is disabled, should use slow path
      byte[] testData = {10, 20, 30, 40};
      HasByteBufferInputStream stream =
          new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true);

      try (ArrowBuf buf = allocator.buffer(testData.length)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, testData.length, true);

        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    public void testFastPath_largeData_fallsBackToSlowPath() throws IOException {
      // Verify large data works correctly when falling back to slow path
      int size = 64 * 1024;
      byte[] testData = new byte[size];
      for (int i = 0; i < size; i++) {
        testData[i] = (byte) (i % 256);
      }
      InputStream stream = new ByteArrayInputStream(testData);

      try (ArrowBuf buf = allocator.buffer(size)) {
        GetReadableBuffer.readIntoBuffer(stream, buf, size, true);

        assertEquals(size, buf.writerIndex());
        byte[] result = new byte[size];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }
  }

  // --- Zero-Copy Ownership Transfer Tests ---

  @Nested
  @EnabledIf("org.apache.arrow.flight.grpc.TestGetReadableBuffer#isHasByteBufferApiEnabled")
  class ZeroCopyOwnershipTransferTests {

    @Test
    public void testReadWithOwnershipTransfer_emptyBuffer() throws IOException {
      DetachableHasByteBufferInputStream stream =
          new DetachableHasByteBufferInputStream(List.of(), true, true);

      try (ArrowBuf buf = GetReadableBuffer.readWithOwnershipTransfer(allocator, stream, 0)) {
        assertNotNull(buf);
        assertEquals(0, buf.capacity());
      }
    }

    @Test
    public void testReadWithOwnershipTransfer_fallbackToRegularStream() throws IOException {
      // Regular stream without Detachable should fall back to copy
      byte[] testData = {1, 2, 3, 4, 5};
      HasByteBufferInputStream stream =
          new HasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), true);

      try (ArrowBuf buf =
          GetReadableBuffer.readWithOwnershipTransfer(allocator, stream, testData.length)) {
        assertNotNull(buf);
        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    public void testReadWithOwnershipTransfer_fallbackToHeapBuffer() throws IOException {
      // Heap buffer (non-direct) should fall back to copy
      byte[] testData = {1, 2, 3, 4, 5};
      ByteBuffer heapBuffer = ByteBuffer.wrap(testData);
      DetachableHasByteBufferInputStream stream =
          new DetachableHasByteBufferInputStream(List.of(heapBuffer), true, false);

      try (ArrowBuf buf =
          GetReadableBuffer.readWithOwnershipTransfer(allocator, stream, testData.length)) {
        assertNotNull(buf);
        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }

    @Test
    public void testReadWithOwnershipTransfer_directBuffer() throws IOException {
      // Direct buffer with Detachable should attempt zero-copy
      byte[] testData = {1, 2, 3, 4, 5, 6, 7, 8};
      ByteBuffer directBuffer = ByteBuffer.allocateDirect(testData.length);
      directBuffer.put(testData);
      directBuffer.flip();

      AtomicBoolean detachCalled = new AtomicBoolean(false);
      AtomicBoolean streamClosed = new AtomicBoolean(false);
      DetachableHasByteBufferInputStream stream =
          new DetachableHasByteBufferInputStream(List.of(directBuffer), true, true) {
            @Override
            public InputStream detach() {
              detachCalled.set(true);
              return new DetachableHasByteBufferInputStream(
                  List.of(directBuffer.duplicate()), true, true) {
                @Override
                public void close() throws IOException {
                  streamClosed.set(true);
                  super.close();
                }
              };
            }
          };

      try (ArrowBuf buf =
          GetReadableBuffer.readWithOwnershipTransfer(allocator, stream, testData.length)) {
        assertNotNull(buf);
        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);

        // Verify detach was called for direct buffer
        assertTrue(detachCalled.get(), "detach() should be called for direct buffer");
      }
      // After ArrowBuf is closed, the detached stream should be closed
      assertTrue(streamClosed.get(), "Detached stream should be closed when ArrowBuf is released");
    }

    @Test
    public void testReadWithOwnershipTransfer_fragmentedBuffers() throws IOException {
      // Fragmented buffers (multiple small buffers) should fall back to copy
      byte[] part1 = {1, 2, 3};
      byte[] part2 = {4, 5, 6};
      byte[] expected = {1, 2, 3, 4, 5, 6};

      ByteBuffer directBuffer1 = ByteBuffer.allocateDirect(part1.length);
      directBuffer1.put(part1);
      directBuffer1.flip();

      ByteBuffer directBuffer2 = ByteBuffer.allocateDirect(part2.length);
      directBuffer2.put(part2);
      directBuffer2.flip();

      DetachableHasByteBufferInputStream stream =
          new DetachableHasByteBufferInputStream(List.of(directBuffer1, directBuffer2), true, true);

      try (ArrowBuf buf =
          GetReadableBuffer.readWithOwnershipTransfer(allocator, stream, expected.length)) {
        assertNotNull(buf);
        assertEquals(expected.length, buf.writerIndex());
        byte[] result = new byte[expected.length];
        buf.getBytes(0, result);
        assertArrayEquals(expected, result);
      }
    }

    @Test
    public void testReadWithOwnershipTransfer_byteBufferNotSupported() throws IOException {
      // Stream that doesn't support byteBuffer should fall back to copy
      byte[] testData = {1, 2, 3, 4, 5};
      DetachableHasByteBufferInputStream stream =
          new DetachableHasByteBufferInputStream(List.of(ByteBuffer.wrap(testData)), false, true);

      try (ArrowBuf buf =
          GetReadableBuffer.readWithOwnershipTransfer(allocator, stream, testData.length)) {
        assertNotNull(buf);
        assertEquals(testData.length, buf.writerIndex());
        byte[] result = new byte[testData.length];
        buf.getBytes(0, result);
        assertArrayEquals(testData, result);
      }
    }
  }

  /**
   * Test helper class that implements both InputStream and HasByteBuffer. This allows testing the
   * fast path without depending on gRPC internal classes.
   */
  private static class HasByteBufferInputStream extends InputStream implements HasByteBuffer {
    private final List<ByteBuffer> buffers;
    private final boolean byteBufferSupported;
    private final boolean failOnSkip;
    private int currentBufferIndex;

    HasByteBufferInputStream(List<ByteBuffer> buffers, boolean byteBufferSupported) {
      this(buffers, byteBufferSupported, false);
    }

    HasByteBufferInputStream(
        List<ByteBuffer> buffers, boolean byteBufferSupported, boolean failOnSkip) {
      this.buffers = new ArrayList<>();
      for (ByteBuffer bb : buffers) {
        ByteBuffer copy = ByteBuffer.allocate(bb.remaining());
        copy.put(bb.duplicate());
        copy.flip();
        this.buffers.add(copy);
      }
      this.byteBufferSupported = byteBufferSupported;
      this.failOnSkip = failOnSkip;
      this.currentBufferIndex = 0;
    }

    @Override
    public boolean byteBufferSupported() {
      return byteBufferSupported;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      while (currentBufferIndex < buffers.size()
          && !buffers.get(currentBufferIndex).hasRemaining()) {
        currentBufferIndex++;
      }

      if (currentBufferIndex >= buffers.size()) {
        return null;
      }

      return buffers.get(currentBufferIndex).asReadOnlyBuffer();
    }

    @Override
    public long skip(long n) throws IOException {
      if (failOnSkip) {
        throw new IOException("Simulated skip failure");
      }

      long skipped = 0;
      while (skipped < n && currentBufferIndex < buffers.size()) {
        ByteBuffer current = buffers.get(currentBufferIndex);
        int toSkip = (int) Math.min(n - skipped, current.remaining());
        current.position(current.position() + toSkip);
        skipped += toSkip;

        if (!current.hasRemaining()) {
          currentBufferIndex++;
        }
      }
      return skipped;
    }

    @Override
    public int read() throws IOException {
      while (currentBufferIndex < buffers.size()) {
        ByteBuffer current = buffers.get(currentBufferIndex);
        if (current.hasRemaining()) {
          return current.get() & 0xFF;
        }
        currentBufferIndex++;
      }
      return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }

      int totalRead = 0;
      while (totalRead < len && currentBufferIndex < buffers.size()) {
        ByteBuffer current = buffers.get(currentBufferIndex);
        if (current.hasRemaining()) {
          int toRead = Math.min(len - totalRead, current.remaining());
          current.get(b, off + totalRead, toRead);
          totalRead += toRead;
        }
        if (!current.hasRemaining()) {
          currentBufferIndex++;
        }
      }
      return totalRead == 0 ? -1 : totalRead;
    }

    @Override
    public int available() {
      int available = 0;
      for (int i = currentBufferIndex; i < buffers.size(); i++) {
        available += buffers.get(i).remaining();
      }
      return available;
    }
  }

  /**
   * Test helper class that implements InputStream, HasByteBuffer, and Detachable. This allows
   * testing the zero-copy ownership transfer path.
   */
  private static class DetachableHasByteBufferInputStream extends InputStream
      implements HasByteBuffer, Detachable {
    private final List<ByteBuffer> buffers;
    private final boolean byteBufferSupported;
    private final boolean useDirect;
    private int currentBufferIndex;
    private int markBufferIndex;
    private int[] markPositions;

    DetachableHasByteBufferInputStream(
        List<ByteBuffer> buffers, boolean byteBufferSupported, boolean useDirect) {
      this.buffers = new ArrayList<>();
      for (ByteBuffer bb : buffers) {
        ByteBuffer copy;
        if (useDirect && bb.isDirect()) {
          // Keep direct buffers as-is (duplicate to get independent position)
          copy = bb.duplicate();
        } else if (useDirect) {
          // Convert to direct buffer
          copy = ByteBuffer.allocateDirect(bb.remaining());
          copy.put(bb.duplicate());
          copy.flip();
        } else {
          // Use heap buffer
          copy = ByteBuffer.allocate(bb.remaining());
          copy.put(bb.duplicate());
          copy.flip();
        }
        this.buffers.add(copy);
      }
      this.byteBufferSupported = byteBufferSupported;
      this.useDirect = useDirect;
      this.currentBufferIndex = 0;
      this.markBufferIndex = 0;
      this.markPositions = null;
    }

    @Override
    public boolean byteBufferSupported() {
      return byteBufferSupported;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      while (currentBufferIndex < buffers.size()
          && !buffers.get(currentBufferIndex).hasRemaining()) {
        currentBufferIndex++;
      }

      if (currentBufferIndex >= buffers.size()) {
        return null;
      }

      // Return a duplicate so that position/limit changes don't affect the internal buffer
      // This matches the HasByteBuffer contract
      return buffers.get(currentBufferIndex).duplicate();
    }

    @Override
    public InputStream detach() {
      // Create a new stream with the remaining data
      List<ByteBuffer> remainingBuffers = new ArrayList<>();
      for (int i = currentBufferIndex; i < buffers.size(); i++) {
        ByteBuffer bb = buffers.get(i);
        if (bb.hasRemaining()) {
          remainingBuffers.add(bb.duplicate());
        }
      }
      // Clear this stream's buffers
      buffers.clear();
      currentBufferIndex = 0;
      return new DetachableHasByteBufferInputStream(
          remainingBuffers, byteBufferSupported, useDirect);
    }

    @Override
    public boolean markSupported() {
      return true;
    }

    @Override
    public void mark(int readLimit) {
      markBufferIndex = currentBufferIndex;
      // Save positions of all buffers from current index onwards
      markPositions = new int[buffers.size()];
      for (int i = 0; i < buffers.size(); i++) {
        markPositions[i] = buffers.get(i).position();
      }
    }

    @Override
    public void reset() throws IOException {
      if (markPositions == null) {
        throw new IOException("Mark not set");
      }
      currentBufferIndex = markBufferIndex;
      // Restore positions of all buffers
      for (int i = 0; i < buffers.size(); i++) {
        buffers.get(i).position(markPositions[i]);
      }
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = 0;
      while (skipped < n && currentBufferIndex < buffers.size()) {
        ByteBuffer current = buffers.get(currentBufferIndex);
        int toSkip = (int) Math.min(n - skipped, current.remaining());
        current.position(current.position() + toSkip);
        skipped += toSkip;

        if (!current.hasRemaining()) {
          currentBufferIndex++;
        }
      }
      return skipped;
    }

    @Override
    public int read() throws IOException {
      while (currentBufferIndex < buffers.size()) {
        ByteBuffer current = buffers.get(currentBufferIndex);
        if (current.hasRemaining()) {
          return current.get() & 0xFF;
        }
        currentBufferIndex++;
      }
      return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }

      int totalRead = 0;
      while (totalRead < len && currentBufferIndex < buffers.size()) {
        ByteBuffer current = buffers.get(currentBufferIndex);
        if (current.hasRemaining()) {
          int toRead = Math.min(len - totalRead, current.remaining());
          current.get(b, off + totalRead, toRead);
          totalRead += toRead;
        }
        if (!current.hasRemaining()) {
          currentBufferIndex++;
        }
      }
      return totalRead == 0 ? -1 : totalRead;
    }

    @Override
    public int available() {
      int available = 0;
      for (int i = currentBufferIndex; i < buffers.size(); i++) {
        available += buffers.get(i).remaining();
      }
      return available;
    }
  }
}
