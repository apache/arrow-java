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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import io.grpc.protobuf.ProtoUtils;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for zero-copy buffer handling in ArrowMessage parsing. */
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
  public void testParseUsesDetachedBuffer() throws Exception {
    byte[] appMetadataBytes = new byte[] {1, 2, 3};
    byte[] bodyBytes = new byte[] {4, 5, 6, 7};
    FlightDescriptor descriptor =
        FlightDescriptor.newBuilder()
            .setType(FlightDescriptor.DescriptorType.PATH)
            .addPath("path")
            .build();
    FlightData flightData =
        FlightData.newBuilder()
            .setFlightDescriptor(descriptor)
            .setAppMetadata(ByteString.copyFrom(appMetadataBytes))
            .setDataBody(ByteString.copyFrom(bodyBytes))
            .build();

    byte[] serialized;
    try (InputStream grpcStream =
        ProtoUtils.marshaller(FlightData.getDefaultInstance()).stream(flightData)) {
      serialized = ByteStreams.toByteArray(grpcStream);
    }

    InputStream stream = MockGrpcInputStream.ofDirectBuffer(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      assertEquals(descriptor, message.getDescriptor());

      ArrowBuf appMetadata = message.getApplicationMetadata();
      assertNotNull(appMetadata);
      byte[] appMetadataRead = new byte[appMetadataBytes.length];
      appMetadata.getBytes(0, appMetadataRead);
      assertArrayEquals(appMetadataBytes, appMetadataRead);

      ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
      byte[] bodyRead = new byte[bodyBytes.length];
      body.getBytes(0, bodyRead);
      assertArrayEquals(bodyBytes, bodyRead);
    }
  }

  @Test
  public void testFallbackDoesNotDetachStream() throws Exception {
    byte[] appMetadataBytes = new byte[] {8, 9};
    byte[] bodyBytes = new byte[] {10, 11, 12};
    FlightDescriptor descriptor =
        FlightDescriptor.newBuilder()
            .setType(FlightDescriptor.DescriptorType.PATH)
            .addPath("fallback")
            .build();
    FlightData flightData =
        FlightData.newBuilder()
            .setFlightDescriptor(descriptor)
            .setAppMetadata(ByteString.copyFrom(appMetadataBytes))
            .setDataBody(ByteString.copyFrom(bodyBytes))
            .build();

    byte[] serialized;
    try (InputStream grpcStream =
        ProtoUtils.marshaller(FlightData.getDefaultInstance()).stream(flightData)) {
      serialized = ByteStreams.toByteArray(grpcStream);
    }

    MockGrpcInputStream stream = MockGrpcInputStream.ofHeapBuffer(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      assertEquals(descriptor, message.getDescriptor());
      assertEquals(0, stream.getDetachCount());
    }
  }

  /** Mock InputStream implementing gRPC's Detachable and HasByteBuffer for testing zero-copy. */
  private static class MockGrpcInputStream extends InputStream
      implements Detachable, HasByteBuffer {
    private ByteBuffer buffer;
    private final boolean byteBufferSupported;
    private int detachCount;

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
      detachCount++;
      ByteBuffer detached = this.buffer;
      this.buffer = null;
      return new MockGrpcInputStream(detached, byteBufferSupported);
    }

    int getDetachCount() {
      return detachCount;
    }

    @Override
    public int read() {
      return (buffer != null && buffer.hasRemaining()) ? (buffer.get() & 0xFF) : -1;
    }

    @Override
    public int available() {
      return buffer == null ? 0 : buffer.remaining();
    }

    @Override
    public void close() {
      buffer = null;
    }
  }
}
