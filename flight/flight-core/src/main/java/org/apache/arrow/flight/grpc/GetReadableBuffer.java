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

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import io.grpc.internal.ReadableBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Utility class for efficiently reading data from gRPC InputStreams into Arrow buffers.
 *
 * <p>This class provides two implementations for zero-copy reads:
 *
 * <ul>
 *   <li><b>HasByteBuffer API (default)</b>: Uses the public {@link HasByteBuffer} interface
 *       available since gRPC 1.21.0. This is the preferred approach as it uses stable public APIs.
 *   <li><b>Reflection-based (legacy)</b>: Uses reflection to access gRPC internal classes. This can
 *       be enabled by setting the system property {@code
 *       arrow.flight.grpc.enable_hasbytebuffer_api} to {@code false}.
 * </ul>
 *
 * <p>When neither fast path is available, falls back to copying via a byte array.
 */
public class GetReadableBuffer {

  /**
   * System property to control whether the HasByteBuffer API is used. Default is {@code true}. Set
   * to {@code false} to use the legacy reflection-based implementation.
   */
  public static final String HASBYTEBUFFER_API_PROPERTY =
      "arrow.flight.grpc.enable_hasbytebuffer_api";

  private static final boolean USE_HASBYTEBUFFER_API;

  // Legacy reflection-based fields
  private static final Field READABLE_BUFFER;
  private static final Class<?> BUFFER_INPUT_STREAM;

  static {
    // Determine which implementation to use based on system property (default: true)
    USE_HASBYTEBUFFER_API =
        !"false".equalsIgnoreCase(System.getProperty(HASBYTEBUFFER_API_PROPERTY, "true"));

    // Initialize legacy reflection-based implementation (used as fallback or when explicitly
    // enabled)
    Field tmpField = null;
    Class<?> tmpClazz = null;
    if (!USE_HASBYTEBUFFER_API) {
      try {
        Class<?> clazz = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream");
        Field f = clazz.getDeclaredField("buffer");
        f.setAccessible(true);
        tmpField = f;
        tmpClazz = clazz;
      } catch (Exception e) {
        new RuntimeException(
                "Failed to initialize GetReadableBuffer reflection, falling back to slow path", e)
            .printStackTrace();
      }
    }
    READABLE_BUFFER = tmpField;
    BUFFER_INPUT_STREAM = tmpClazz;
  }

  private GetReadableBuffer() {}

  /**
   * Returns whether the HasByteBuffer API is enabled.
   *
   * @return true if the HasByteBuffer API is enabled, false if using legacy reflection
   */
  public static boolean isHasByteBufferApiEnabled() {
    return USE_HASBYTEBUFFER_API;
  }

  /**
   * Extracts the ReadableBuffer for the given input stream using reflection.
   *
   * @param is Must be an instance of io.grpc.internal.ReadableBuffers$BufferInputStream or null
   *     will be returned.
   * @deprecated This method uses gRPC internal APIs via reflection. Prefer using {@link
   *     #readIntoBuffer} which uses the public HasByteBuffer API by default.
   */
  @Deprecated
  public static ReadableBuffer getReadableBuffer(InputStream is) {
    if (BUFFER_INPUT_STREAM == null || !is.getClass().equals(BUFFER_INPUT_STREAM)) {
      return null;
    }

    try {
      return (ReadableBuffer) READABLE_BUFFER.get(is);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Helper method to read a gRPC-provided InputStream into an ArrowBuf.
   *
   * <p>When fastPath is enabled, this method attempts to use zero-copy reads:
   *
   * <ul>
   *   <li>If HasByteBuffer API is enabled (default), uses {@link HasByteBuffer#getByteBuffer()}
   *   <li>If HasByteBuffer API is disabled, uses reflection to access gRPC internal ReadableBuffer
   * </ul>
   *
   * <p>Falls back to copying via a byte array if zero-copy is not available.
   *
   * @param stream The stream to read from.
   * @param buf The buffer to read into.
   * @param size The number of bytes to read.
   * @param fastPath Whether to enable the fast path (zero-copy reads).
   * @throws IOException if there is an error reading from the stream
   */
  public static void readIntoBuffer(
      final InputStream stream, final ArrowBuf buf, final int size, final boolean fastPath)
      throws IOException {
    if (size == 0) {
      buf.writerIndex(0);
      return;
    }

    if (fastPath) {
      if (USE_HASBYTEBUFFER_API) {
        // New implementation using public HasByteBuffer API
        if (stream instanceof HasByteBuffer) {
          HasByteBuffer hasByteBuffer = (HasByteBuffer) stream;
          if (hasByteBuffer.byteBufferSupported()) {
            readUsingHasByteBuffer(stream, hasByteBuffer, buf, size);
            return;
          }
        }
      } else {
        // Legacy implementation using reflection
        ReadableBuffer readableBuffer = getReadableBuffer(stream);
        if (readableBuffer != null) {
          readableBuffer.readBytes(buf.nioBuffer(0, size));
          buf.writerIndex(size);
          return;
        }
      }
    }

    // Slow path: copy via byte array
    byte[] heapBytes = new byte[size];
    ByteStreams.readFully(stream, heapBytes);
    buf.writeBytes(heapBytes);
    buf.writerIndex(size);
  }

  /**
   * Reads data from a stream using the HasByteBuffer zero-copy API.
   *
   * <p>This method copies data from gRPC's ByteBuffers into the provided ArrowBuf. While it avoids
   * intermediate byte[] allocations, it still performs a memory copy.
   *
   * @param stream The underlying InputStream (for skip operations)
   * @param hasByteBuffer The HasByteBuffer interface of the stream
   * @param buf The ArrowBuf to write into
   * @param size The number of bytes to read
   * @throws IOException if there is an error reading from the stream
   */
  private static void readUsingHasByteBuffer(
      final InputStream stream, final HasByteBuffer hasByteBuffer, final ArrowBuf buf, int size)
      throws IOException {
    int offset = 0;
    int remaining = size;

    while (remaining > 0) {
      ByteBuffer byteBuffer = hasByteBuffer.getByteBuffer();
      if (byteBuffer == null) {
        throw new IOException(
            "Unexpected end of stream: expected " + size + " bytes, got " + offset);
      }

      int available = byteBuffer.remaining();
      int toCopy = Math.min(remaining, available);

      // Copy data from the ByteBuffer to the ArrowBuf.
      // We use the ByteBuffer directly without duplicate() since HasByteBuffer javadoc states:
      // "The returned buffer's content should not be modified, but the position, limit, and mark
      // may be changed. Operations for changing the position, limit, and mark of the returned
      // buffer does not affect the position, limit, and mark of this input stream."
      int originalLimit = byteBuffer.limit();
      byteBuffer.limit(byteBuffer.position() + toCopy);
      buf.setBytes(offset, byteBuffer);
      byteBuffer.limit(originalLimit);

      // Advance the stream position
      long skipped = stream.skip(toCopy);
      if (skipped != toCopy) {
        throw new IOException("Failed to skip bytes: expected " + toCopy + ", skipped " + skipped);
      }

      offset += toCopy;
      remaining -= toCopy;
    }

    buf.writerIndex(size);
  }

  /**
   * Reads data from a gRPC stream with true zero-copy ownership transfer when possible.
   *
   * <p>This method attempts to achieve zero-copy by taking ownership of gRPC's underlying
   * ByteBuffers using the {@link Detachable} interface. When successful, the returned ArrowBuf
   * directly wraps the gRPC buffer's memory without any data copying.
   *
   * <p>Zero-copy ownership transfer is only possible when:
   *
   * <ul>
   *   <li>The stream implements both {@link HasByteBuffer} and {@link Detachable}
   *   <li>The stream contains a single contiguous direct ByteBuffer
   *   <li>The ByteBuffer's remaining bytes match the requested size
   * </ul>
   *
   * <p>When zero-copy is not possible, this method falls back to allocating a new buffer and
   * copying the data.
   *
   * @param allocator The allocator to use for buffer allocation (used for both zero-copy wrapping
   *     and fallback allocation)
   * @param stream The gRPC InputStream to read from
   * @param size The number of bytes to read
   * @return An ArrowBuf containing the data. The caller is responsible for closing this buffer.
   * @throws IOException if there is an error reading from the stream
   */
  public static ArrowBuf readWithOwnershipTransfer(
      final BufferAllocator allocator, final InputStream stream, final int size)
      throws IOException {
    if (size == 0) {
      return allocator.getEmpty();
    }

    // Try zero-copy ownership transfer if the stream supports it
    if (USE_HASBYTEBUFFER_API && stream instanceof HasByteBuffer && stream instanceof Detachable) {
      HasByteBuffer hasByteBuffer = (HasByteBuffer) stream;
      if (hasByteBuffer.byteBufferSupported()) {
        ArrowBuf zeroCopyBuf = tryZeroCopyOwnershipTransfer(allocator, stream, hasByteBuffer, size);
        if (zeroCopyBuf != null) {
          return zeroCopyBuf;
        }
      }
    }

    // Fall back to copy-based approach
    ArrowBuf buf = allocator.buffer(size);
    try {
      readIntoBuffer(stream, buf, size, true);
      return buf;
    } catch (Exception e) {
      buf.close();
      throw e;
    }
  }

  /**
   * Attempts zero-copy ownership transfer from gRPC stream to ArrowBuf.
   *
   * @return ArrowBuf wrapping gRPC's memory if successful, null if zero-copy is not possible
   */
  private static ArrowBuf tryZeroCopyOwnershipTransfer(
      final BufferAllocator allocator,
      final InputStream stream,
      final HasByteBuffer hasByteBuffer,
      final int size)
      throws IOException {
    // Check if mark is supported - we need it to reset the stream if zero-copy fails
    if (!stream.markSupported()) {
      return null;
    }

    // Use mark() to prevent premature deallocation while we inspect the buffer
    // According to gRPC docs: "skip() deallocates the last ByteBuffer, similar to read()"
    // mark() prevents this deallocation
    stream.mark(size);

    try {
      ByteBuffer byteBuffer = hasByteBuffer.getByteBuffer();
      if (byteBuffer == null) {
        // No need to reset - stream is already at end
        return null;
      }

      // Zero-copy only works with direct ByteBuffers (they have a memory address)
      if (!byteBuffer.isDirect()) {
        // Stream position hasn't changed, no need to reset
        return null;
      }

      // Check if this single buffer contains all the data we need
      if (byteBuffer.remaining() < size) {
        // Data is fragmented across multiple buffers, can't do zero-copy
        // Stream position hasn't changed, no need to reset
        return null;
      }

      // Take ownership of the underlying buffers using Detachable.detach()
      Detachable detachable = (Detachable) stream;
      InputStream detachedStream = detachable.detach();

      // Get the ByteBuffer from the detached stream
      if (!(detachedStream instanceof HasByteBuffer)) {
        // Detached stream doesn't support HasByteBuffer, fall back
        // Note: original stream is now empty after detach(), can't fall back
        closeQuietly(detachedStream);
        throw new IOException("Detached stream does not support HasByteBuffer");
      }

      HasByteBuffer detachedHasByteBuffer = (HasByteBuffer) detachedStream;
      ByteBuffer detachedBuffer = detachedHasByteBuffer.getByteBuffer();
      if (detachedBuffer == null || !detachedBuffer.isDirect()) {
        closeQuietly(detachedStream);
        throw new IOException("Detached buffer is null or not direct");
      }

      // Get the memory address of the ByteBuffer
      long memoryAddress =
          MemoryUtil.getByteBufferAddress(detachedBuffer) + detachedBuffer.position();

      // Create a ForeignAllocation that will close the detached stream when released
      final InputStream streamToClose = detachedStream;
      ForeignAllocation allocation =
          new ForeignAllocation(size, memoryAddress) {
            @Override
            protected void release0() {
              closeQuietly(streamToClose);
            }
          };

      // Wrap the foreign allocation in an ArrowBuf
      ArrowBuf buf = allocator.wrapForeignAllocation(allocation);
      buf.writerIndex(size);
      return buf;

    } catch (Exception e) {
      // Reset the stream position on failure (if possible)
      try {
        stream.reset();
      } catch (IOException resetEx) {
        e.addSuppressed(resetEx);
      }
      throw e;
    }
  }

  private static void closeQuietly(InputStream stream) {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException ignored) {
        // Ignore close exceptions
      }
    }
  }
}
