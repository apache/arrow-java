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

import com.google.common.io.ByteStreams;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses FlightData protobuf messages into ArrowMessage objects.
 *
 * <p>This class handles parsing from both regular InputStreams (with data copying) and ArrowBuf
 * (with zero-copy slicing for large fields like app_metadata and body).
 *
 * <p>Small fields (descriptor, header) are always copied. Large fields (app_metadata, body) use
 * zero-copy slicing when parsing from ArrowBuf.
 */
final class FlightDataParser {

  // Protobuf wire format tags for FlightData fields
  private static final int DESCRIPTOR_TAG =
      (FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int HEADER_TAG =
      (FlightData.DATA_HEADER_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int BODY_TAG =
      (FlightData.DATA_BODY_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int APP_METADATA_TAG =
      (FlightData.APP_METADATA_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  /** Base class for FlightData readers with common parsing logic. */
  abstract static class FlightDataReader {
    protected final BufferAllocator allocator;

    protected FlightDescriptor descriptor;
    protected MessageMetadataResult header;
    protected ArrowBuf appMetadata;
    protected ArrowBuf body;

    FlightDataReader(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    /** Parses the FlightData and returns an ArrowMessage. */
    final ArrowMessage toMessage() {
      try {
        parseFields();
        ArrowBuf adjustedBody = adjustBodyForHeaderType();
        ArrowMessage message = new ArrowMessage(descriptor, header, appMetadata, adjustedBody);
        // Ownership transferred to ArrowMessage
        appMetadata = null;
        body = null;
        return message;
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        cleanup();
      }
    }

    private ArrowBuf adjustBodyForHeaderType() {
      if (header == null) {
        return body;
      }
      switch (ArrowMessage.HeaderType.getHeader(header.headerType())) {
        case SCHEMA:
          if (body != null && body.capacity() == 0) {
            body.close();
            return null;
          }
          break;
        case DICTIONARY_BATCH:
        case RECORD_BATCH:
          if (body == null) {
            return allocator.getEmpty();
          }
          break;
        case NONE:
        case TENSOR:
        default:
          break;
      }
      return body;
    }

    private void parseFields() throws IOException {
      while (hasRemaining()) {
        int tag = readTag();
        if (tag == -1) {
          break;
        }
        switch (tag) {
          case DESCRIPTOR_TAG:
            {
              int size = readLength();
              byte[] bytes = readBytes(size);
              descriptor = FlightDescriptor.parseFrom(bytes);
              break;
            }
          case HEADER_TAG:
            {
              int size = readLength();
              byte[] bytes = readBytes(size);
              header = MessageMetadataResult.create(ByteBuffer.wrap(bytes), size);
              break;
            }
          case APP_METADATA_TAG:
            {
              int size = readLength();
              closeAppMetadata();
              appMetadata = readBuffer(size);
              break;
            }
          case BODY_TAG:
            {
              int size = readLength();
              closeBody();
              body = readBuffer(size);
              break;
            }
          default:
            // ignore unknown fields
        }
      }
    }

    /** Returns true if there is more data to read. */
    protected abstract boolean hasRemaining() throws IOException;

    /** Reads the next protobuf tag, or -1 if no more data. */
    protected abstract int readTag() throws IOException;

    /** Reads a varint-encoded length. */
    protected abstract int readLength() throws IOException;

    /** Reads the specified number of bytes into a new byte array. */
    protected abstract byte[] readBytes(int size) throws IOException;

    /** Reads the specified number of bytes into an ArrowBuf. */
    protected abstract ArrowBuf readBuffer(int size) throws IOException;

    /** Called in finally block to clean up resources. Subclasses can override to add cleanup. */
    protected void cleanup() {
      closeAppMetadata();
      closeBody();
    }

    private void closeAppMetadata() {
      if (appMetadata != null) {
        appMetadata.close();
        appMetadata = null;
      }
    }

    private void closeBody() {
      if (body != null) {
        body.close();
        body = null;
      }
    }
  }

  /** Parses FlightData from an InputStream, copying data into Arrow-managed buffers. */
  static final class InputStreamReader extends FlightDataReader {
    private final InputStream stream;

    InputStreamReader(BufferAllocator allocator, InputStream stream) {
      super(allocator);
      this.stream = stream;
    }

    @Override
    protected boolean hasRemaining() throws IOException {
      return stream.available() > 0;
    }

    @Override
    protected int readTag() throws IOException {
      int tagFirstByte = stream.read();
      if (tagFirstByte == -1) {
        return -1;
      }
      return CodedInputStream.readRawVarint32(tagFirstByte, stream);
    }

    @Override
    protected int readLength() throws IOException {
      int firstByte = stream.read();
      return CodedInputStream.readRawVarint32(firstByte, stream);
    }

    @Override
    protected byte[] readBytes(int size) throws IOException {
      byte[] bytes = new byte[size];
      ByteStreams.readFully(stream, bytes);
      return bytes;
    }

    @Override
    protected ArrowBuf readBuffer(int size) throws IOException {
      ArrowBuf buf = allocator.buffer(size);
      byte[] heapBytes = new byte[size];
      ByteStreams.readFully(stream, heapBytes);
      buf.writeBytes(heapBytes);
      buf.writerIndex(size);
      return buf;
    }
  }

  /** Parses FlightData from an ArrowBuf, using zero-copy slicing for large fields. */
  static final class ArrowBufReader extends FlightDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowBufReader.class);

    private final ArrowBuf backingBuffer;
    private final ByteBuffer buffer;

    ArrowBufReader(BufferAllocator allocator, ArrowBuf backingBuffer) {
      super(allocator);
      this.backingBuffer = backingBuffer;
      this.buffer = backingBuffer.nioBuffer(0, (int) backingBuffer.capacity());
    }

    static ArrowBufReader tryArrowBufReader(BufferAllocator allocator, InputStream stream) {
      if (!(stream instanceof Detachable) || !(stream instanceof HasByteBuffer)) {
        return null;
      }

      HasByteBuffer hasByteBuffer = (HasByteBuffer) stream;
      if (!hasByteBuffer.byteBufferSupported()) {
        return null;
      }

      ByteBuffer peekBuffer = hasByteBuffer.getByteBuffer();
      if (peekBuffer == null || !peekBuffer.isDirect()) {
        return null;
      }

      try {
        int available = stream.available();
        if (available > 0 && peekBuffer.remaining() < available) {
          return null;
        }
      } catch (IOException ioe) {
        return null;
      }

      InputStream detachedStream = ((Detachable) stream).detach();
      if (!(detachedStream instanceof HasByteBuffer)) {
        closeQuietly(detachedStream);
        return null;
      }

      ByteBuffer detachedBuffer = ((HasByteBuffer) detachedStream).getByteBuffer();
      if (detachedBuffer == null || !detachedBuffer.isDirect()) {
        closeQuietly(detachedStream);
        return null;
      }

      long bufferAddress = MemoryUtil.getByteBufferAddress(detachedBuffer);
      int bufferSize = detachedBuffer.remaining();

      ForeignAllocation foreignAllocation =
          new ForeignAllocation(bufferSize, bufferAddress + detachedBuffer.position()) {
            @Override
            protected void release0() {
              closeQuietly(detachedStream);
            }
          };

      try {
        ArrowBuf backingBuffer = allocator.wrapForeignAllocation(foreignAllocation);
        return new ArrowBufReader(allocator, backingBuffer);
      } catch (Throwable t) {
        closeQuietly(detachedStream);
        throw t;
      }
    }

    private static void closeQuietly(InputStream stream) {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          LOG.debug("Error closing detached gRPC stream", e);
        }
      }
    }

    @Override
    protected void cleanup() {
      super.cleanup();
      backingBuffer.close();
    }

    @Override
    protected boolean hasRemaining() {
      return buffer.hasRemaining();
    }

    @Override
    protected int readTag() throws IOException {
      if (!buffer.hasRemaining()) {
        return -1;
      }
      int tagFirstByte = buffer.get() & 0xFF;
      return readRawVarint32(tagFirstByte);
    }

    @Override
    protected int readLength() throws IOException {
      if (!buffer.hasRemaining()) {
        throw new IOException("Unexpected end of buffer");
      }
      int firstByte = buffer.get() & 0xFF;
      return readRawVarint32(firstByte);
    }

    /**
     * Decodes a Base 128 Varint from the ByteBuffer.
     *
     * <p>This is a manual implementation because CodedInputStream only provides a static helper for
     * InputStream, not ByteBuffer. We need direct ByteBuffer access to track positions for
     * zero-copy slicing in {@link #readBuffer(int)}.
     *
     * <p>Varints are a variable-length encoding for integers used by Protocol Buffers. Each byte
     * uses 7 bits for data and 1 bit (MSB) as a continuation flag:
     *
     * <ul>
     *   <li>MSB = 1: more bytes follow
     *   <li>MSB = 0: this is the last byte
     * </ul>
     *
     * <p>Bytes are stored in little-endian order (least significant group first).
     *
     * @see <a href="https://protobuf.dev/programming-guides/encoding/#varints">Protocol Buffers
     *     Encoding: Varints</a>
     */
    private int readRawVarint32(int firstByte) throws IOException {
      // Check MSB: if 0, this single byte contains the entire value (0-127)
      if ((firstByte & 0x80) == 0) {
        return firstByte;
      }
      // Extract lower 7 bits of first byte as the starting result
      int result = firstByte & 0x7F;
      // Process continuation bytes, shifting each 7-bit group into position
      for (int shift = 7; shift < 32; shift += 7) {
        if (!buffer.hasRemaining()) {
          throw new IOException("Unexpected end of buffer");
        }
        int b = buffer.get() & 0xFF;
        // OR the 7 data bits into the result at the current shift position
        result |= (b & 0x7F) << shift;
        // If MSB is 0, we've reached the last byte
        if ((b & 0x80) == 0) {
          return result;
        }
      }
      // A valid 32-bit varint uses at most 5 bytes (5 * 7 = 35 bits > 32 bits)
      throw new IOException("Malformed varint");
    }

    @Override
    protected byte[] readBytes(int size) throws IOException {
      if (buffer.remaining() < size) {
        throw new IOException("Unexpected end of buffer");
      }
      byte[] bytes = new byte[size];
      buffer.get(bytes);
      return bytes;
    }

    @Override
    protected ArrowBuf readBuffer(int size) throws IOException {
      if (buffer.remaining() < size) {
        throw new IOException("Unexpected end of buffer");
      }
      int offset = buffer.position();
      buffer.position(offset + size);
      backingBuffer.getReferenceManager().retain();
      return backingBuffer.slice(offset, size);
    }
  }
}
