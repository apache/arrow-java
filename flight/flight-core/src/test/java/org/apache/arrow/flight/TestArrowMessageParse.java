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
import com.google.protobuf.CodedOutputStream;
import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import io.grpc.protobuf.ProtoUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests FlightData parsing including duplicate field handling, well-formed messages, and zero-copy
 * behavior. Covers both InputStream (with copying) and ArrowBuf (zero-copy) parsing paths. Verifies
 * that duplicate protobuf fields use last-occurrence-wins semantics without memory leaks.
 */
public class TestArrowMessageParse {

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  /** Verifies duplicate app_metadata fields via InputStream path use last-occurrence-wins. */
  @Test
  public void testDuplicateAppMetadataInputStream() throws Exception {
    byte[] firstAppMetadata = new byte[] {1, 2, 3};
    byte[] secondAppMetadata = new byte[] {4, 5, 6, 7, 8};

    byte[] serialized =
        buildFlightDataDescriptors(
            List.of(
                Pair.of(FlightData.APP_METADATA_FIELD_NUMBER, firstAppMetadata),
                Pair.of(FlightData.APP_METADATA_FIELD_NUMBER, secondAppMetadata)));
    InputStream stream = new ByteArrayInputStream(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      ArrowBuf appMetadata = message.getApplicationMetadata();
      assertNotNull(appMetadata);
      // Use readableBytes() instead of capacity() since allocator may round up
      assertEquals(secondAppMetadata.length, appMetadata.readableBytes());

      byte[] actual = new byte[secondAppMetadata.length];
      appMetadata.getBytes(0, actual);
      assertArrayEquals(secondAppMetadata, actual);
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /**
   * Verifies duplicate app_metadata fields via zero-copy ArrowBuf path use last-occurrence-wins.
   */
  @Test
  public void testDuplicateAppMetadataArrowBuf() throws Exception {
    byte[] firstAppMetadata = new byte[] {1, 2, 3};
    byte[] secondAppMetadata = new byte[] {4, 5, 6, 7, 8};

    // Verify clean start
    assertEquals(0, allocator.getAllocatedMemory());

    byte[] serialized =
        buildFlightDataDescriptors(
            List.of(
                Pair.of(FlightData.APP_METADATA_FIELD_NUMBER, firstAppMetadata),
                Pair.of(FlightData.APP_METADATA_FIELD_NUMBER, secondAppMetadata)));
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      ArrowBuf appMetadata = message.getApplicationMetadata();
      assertNotNull(appMetadata);
      assertEquals(secondAppMetadata.length, appMetadata.readableBytes());

      byte[] actual = new byte[secondAppMetadata.length];
      appMetadata.getBytes(0, actual);
      assertArrayEquals(secondAppMetadata, actual);

      // Zero-copy: only the backing buffer (serialized message) should be allocated
      assertEquals(serialized.length, allocator.getAllocatedMemory());
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** Verifies duplicate body fields via InputStream path use last-occurrence-wins. */
  @Test
  public void testDuplicateBodyInputStream() throws Exception {
    byte[] firstBody = new byte[] {10, 20, 30};
    byte[] secondBody = new byte[] {40, 50, 60, 70};

    byte[] serialized =
        buildFlightDataDescriptors(
            List.of(
                Pair.of(FlightData.DATA_BODY_FIELD_NUMBER, firstBody),
                Pair.of(FlightData.DATA_BODY_FIELD_NUMBER, secondBody)));
    InputStream stream = new ByteArrayInputStream(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
      assertNotNull(body);
      assertEquals(secondBody.length, body.readableBytes());

      byte[] actual = new byte[secondBody.length];
      body.getBytes(0, actual);
      assertArrayEquals(secondBody, actual);
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** Verifies duplicate body fields via zero-copy ArrowBuf path use last-occurrence-wins. */
  @Test
  public void testDuplicateBodyArrowBuf() throws Exception {
    byte[] firstBody = new byte[] {10, 20, 30};
    byte[] secondBody = new byte[] {40, 50, 60, 70};

    // Verify clean start
    assertEquals(0, allocator.getAllocatedMemory());

    byte[] serialized =
        buildFlightDataDescriptors(
            List.of(
                Pair.of(FlightData.DATA_BODY_FIELD_NUMBER, firstBody),
                Pair.of(FlightData.DATA_BODY_FIELD_NUMBER, secondBody)));
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
      assertNotNull(body);
      assertEquals(secondBody.length, body.readableBytes());

      byte[] actual = new byte[secondBody.length];
      body.getBytes(0, actual);
      assertArrayEquals(secondBody, actual);

      // Zero-copy: only the backing buffer (serialized message) should be allocated
      assertEquals(serialized.length, allocator.getAllocatedMemory());
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** Verifies well-formed FlightData message parsing via InputStream path. */
  @Test
  public void testFieldsInputStream() throws Exception {
    byte[] appMetadataBytes = new byte[] {100, 101, 102};
    byte[] bodyBytes = new byte[] {50, 51, 52, 53, 54};
    FlightDescriptor expectedDescriptor = createTestDescriptor();

    byte[] serialized = buildFlightDataWithBothFields(appMetadataBytes, bodyBytes);
    InputStream stream = new ByteArrayInputStream(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      // Verify descriptor
      assertEquals(expectedDescriptor, message.getDescriptor());

      // Verify header is present (Schema message type)
      assertEquals(ArrowMessage.HeaderType.SCHEMA, message.getMessageType());

      // Verify app metadata
      ArrowBuf appMetadata = message.getApplicationMetadata();
      assertNotNull(appMetadata);
      assertEquals(appMetadataBytes.length, appMetadata.readableBytes());
      byte[] actualAppMetadata = new byte[appMetadataBytes.length];
      appMetadata.getBytes(0, actualAppMetadata);
      assertArrayEquals(appMetadataBytes, actualAppMetadata);

      // Verify body
      ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
      assertNotNull(body);
      assertEquals(bodyBytes.length, body.readableBytes());
      byte[] actualBody = new byte[bodyBytes.length];
      body.getBytes(0, actualBody);
      assertArrayEquals(bodyBytes, actualBody);
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** Verifies well-formed FlightData message parsing via zero-copy ArrowBuf path. */
  @Test
  public void testFieldsArrowBuf() throws Exception {
    byte[] appMetadataBytes = new byte[] {100, 101, 102};
    byte[] bodyBytes = new byte[] {50, 51, 52, 53, 54};
    FlightDescriptor expectedDescriptor = createTestDescriptor();

    assertEquals(0, allocator.getAllocatedMemory());

    byte[] serialized = buildFlightDataWithBothFields(appMetadataBytes, bodyBytes);
    InputStream stream = MockGrpcInputStream.ofDirectBuffer(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      // Verify descriptor
      assertEquals(expectedDescriptor, message.getDescriptor());

      // Verify header is present (Schema message type)
      assertEquals(ArrowMessage.HeaderType.SCHEMA, message.getMessageType());

      // Verify app metadata
      ArrowBuf appMetadata = message.getApplicationMetadata();
      assertNotNull(appMetadata);
      assertEquals(appMetadataBytes.length, appMetadata.readableBytes());
      byte[] actualAppMetadata = new byte[appMetadataBytes.length];
      appMetadata.getBytes(0, actualAppMetadata);
      assertArrayEquals(appMetadataBytes, actualAppMetadata);

      // Verify body
      ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
      assertNotNull(body);
      assertEquals(bodyBytes.length, body.readableBytes());
      byte[] actualBody = new byte[bodyBytes.length];
      body.getBytes(0, actualBody);
      assertArrayEquals(bodyBytes, actualBody);

      // Zero-copy: only the backing buffer (serialized message) should be allocated
      assertEquals(serialized.length, allocator.getAllocatedMemory());
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** Verifies that heap buffers fall back to InputStream path without calling detach(). */
  @Test
  public void testHeapBufferFallbackDoesNotDetach() throws Exception {
    byte[] appMetadataBytes = new byte[] {8, 9};
    byte[] bodyBytes = new byte[] {10, 11, 12};

    byte[] serialized = buildFlightDataWithBothFields(appMetadataBytes, bodyBytes);
    MockGrpcInputStream stream = MockGrpcInputStream.ofHeapBuffer(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      assertNotNull(message.getDescriptor());
      assertEquals(0, stream.getDetachCount());
    }
  }

  /** Verifies fallback to InputStream path when getByteBuffer() returns null. */
  @Test
  public void testNullByteBufferFallbackToInputStream() throws Exception {
    byte[] appMetadataBytes = new byte[] {20, 21, 22};
    byte[] bodyBytes = new byte[] {30, 31, 32, 33};
    FlightDescriptor expectedDescriptor = createTestDescriptor();

    byte[] serialized = buildFlightDataWithBothFields(appMetadataBytes, bodyBytes);
    MockGrpcInputStream stream = new MockGrpcInputStream(ByteBuffer.wrap(serialized), false);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      assertEquals(expectedDescriptor, message.getDescriptor());

      ArrowBuf appMetadata = message.getApplicationMetadata();
      assertNotNull(appMetadata);
      byte[] actualAppMetadata = new byte[appMetadataBytes.length];
      appMetadata.getBytes(0, actualAppMetadata);
      assertArrayEquals(appMetadataBytes, actualAppMetadata);

      ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
      assertNotNull(body);
      byte[] actualBody = new byte[bodyBytes.length];
      body.getBytes(0, actualBody);
      assertArrayEquals(bodyBytes, actualBody);

      assertEquals(0, stream.getDetachCount());
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  // Helper methods to build complete FlightData messages

  private FlightDescriptor createTestDescriptor() {
    return FlightDescriptor.newBuilder()
        .setType(FlightDescriptor.DescriptorType.PATH)
        .addPath("test")
        .addPath("path")
        .build();
  }

  private byte[] createSchemaHeader() {
    Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("name", new ArrowType.Utf8())));
    ByteBuffer headerBuffer = MessageSerializer.serializeMetadata(schema, IpcOption.DEFAULT);
    byte[] headerBytes = new byte[headerBuffer.remaining()];
    headerBuffer.get(headerBytes);
    return headerBytes;
  }

  private byte[] buildFlightDataWithBothFields(byte[] appMetadata, byte[] body) throws IOException {
    FlightData flightData =
        FlightData.newBuilder()
            .setFlightDescriptor(createTestDescriptor())
            .setDataHeader(ByteString.copyFrom(createSchemaHeader()))
            .setAppMetadata(ByteString.copyFrom(appMetadata))
            .setDataBody(ByteString.copyFrom(body))
            .build();
    try (InputStream grpcStream =
        ProtoUtils.marshaller(FlightData.getDefaultInstance()).stream(flightData)) {
      return ByteStreams.toByteArray(grpcStream);
    }
  }

  // Helper methods to build FlightData messages with duplicate fields

  private byte[] buildFlightDataDescriptors(List<Pair<Integer, byte[]>> descriptors)
      throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream cos = CodedOutputStream.newInstance(baos);

    for (Pair<Integer, byte[]> descriptor : descriptors) {
      cos.writeBytes(descriptor.getKey(), ByteString.copyFrom(descriptor.getValue()));
    }
    cos.flush();
    return baos.toByteArray();
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
