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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.MethodDescriptor;
import io.grpc.internal.ReadableBuffers;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests for deframing FlightData messages in {@link ArrowMessage}. */
public class TestArrowMessage {

  private static final int[] LENGTH_DELIMITED_FIELDS = {
    FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER,
    FlightData.DATA_HEADER_FIELD_NUMBER,
    FlightData.APP_METADATA_FIELD_NUMBER,
    FlightData.DATA_BODY_FIELD_NUMBER
  };

  /** A declared field length far larger than any frame built here. */
  private static final int OVERSIZED_LENGTH = 1 << 20;

  private static final byte[] FIRST = new byte[] {1, 2, 3, 4};
  private static final byte[] LAST = new byte[] {5, 6, 7, 8, 9, 10};

  /** The kinds of stream gRPC hands to the marshaller. */
  enum StreamType {
    /** An uncompressed message: available() is the number of bytes left in the message. */
    KNOWN_LENGTH {
      @Override
      InputStream open(byte[] frame) {
        return ReadableBuffers.openStream(ReadableBuffers.wrap(frame), true);
      }
    },
    /** A compressed message: available() is 1 until EOF, however long the message is. */
    COMPRESSED {
      @Override
      InputStream open(byte[] frame) throws IOException {
        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
          gzip.write(frame);
        }
        return new GZIPInputStream(new ByteArrayInputStream(compressed.toByteArray()));
      }
    };

    abstract InputStream open(byte[] frame) throws IOException;
  }

  private BufferAllocator allocator;
  private MethodDescriptor.Marshaller<ArrowMessage> marshaller;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    marshaller = ArrowMessage.createMarshaller(allocator);
  }

  @AfterEach
  public void tearDown() {
    // Fails if a test leaked a buffer.
    allocator.close();
  }

  /** A well-formed frame parses whichever kind of stream it arrives on. */
  @ParameterizedTest
  @EnumSource
  public void frameAcceptsWellFormedFrame(StreamType streamType) throws Exception {
    final Schema schema =
        new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));
    final FlightData data =
        FlightData.newBuilder()
            .setFlightDescriptor(FlightDescriptor.command(FIRST).toProtocol())
            .setDataHeader(
                ByteString.copyFrom(MessageSerializer.serializeMetadata(schema, IpcOption.DEFAULT)))
            .setAppMetadata(ByteString.copyFrom(LAST))
            .build();

    try (ArrowMessage message = marshaller.parse(streamType.open(data.toByteArray()))) {
      assertEquals(data.getFlightDescriptor(), message.getDescriptor());
      assertEquals(schema, message.asSchema());
      assertArrayEquals(LAST, toByteArray(message.getApplicationMetadata()));
    }
  }

  /** A repeated buffer field keeps the last occurrence and releases the earlier one. */
  @ParameterizedTest
  @EnumSource
  public void frameKeepsLastOfRepeatedField(StreamType streamType) throws Exception {
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    for (byte[] value : new byte[][] {FIRST, LAST}) {
      FlightData.newBuilder()
          .setAppMetadata(ByteString.copyFrom(value))
          .setDataBody(ByteString.copyFrom(value))
          .build()
          .writeTo(frame);
    }

    try (ArrowMessage message = marshaller.parse(streamType.open(frame.toByteArray()))) {
      assertArrayEquals(LAST, toByteArray(message.getApplicationMetadata()));
      assertArrayEquals(LAST, toByteArray(Iterables.getOnlyElement(message.getBufs())));
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /**
   * A field declaring more bytes than the frame holds is rejected, without first allocating a
   * buffer of the declared length.
   */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsOversizedFieldLength(StreamType streamType) throws Exception {
    for (int field : LENGTH_DELIMITED_FIELDS) {
      assertRejected(streamType, fieldPrefix(field, OVERSIZED_LENGTH));
    }
    assertEquals(0, allocator.getPeakMemoryAllocation());
  }

  /** A negative length, which a 5-byte varint can encode, is rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsNegativeFieldLength(StreamType streamType) throws Exception {
    for (int field : LENGTH_DELIMITED_FIELDS) {
      assertRejected(streamType, fieldPrefix(field, -1));
    }
  }

  /** Buffers read for earlier fields are released when a later field is rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameReleasesBuffersWhenLaterFieldIsRejected(StreamType streamType) throws Exception {
    for (int field : LENGTH_DELIMITED_FIELDS) {
      final ByteArrayOutputStream frame = new ByteArrayOutputStream();
      FlightData.newBuilder()
          .setAppMetadata(ByteString.copyFrom(FIRST))
          .setDataBody(ByteString.copyFrom(LAST))
          .build()
          .writeTo(frame);
      frame.write(fieldPrefix(field, OVERSIZED_LENGTH));

      assertRejected(streamType, frame.toByteArray());
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  private void assertRejected(StreamType streamType, byte[] frame) throws IOException {
    final InputStream stream = streamType.open(frame);
    final RuntimeException e = assertThrows(RuntimeException.class, () -> marshaller.parse(stream));
    assertInstanceOf(IOException.class, e.getCause());
  }

  /** The tag and length prefix of a length-delimited field, without any content. */
  private static byte[] fieldPrefix(int fieldNumber, int length) throws IOException {
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    final CodedOutputStream out = CodedOutputStream.newInstance(frame);
    out.writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(length);
    out.flush();
    return frame.toByteArray();
  }

  private static byte[] toByteArray(ArrowBuf buf) {
    final byte[] bytes = new byte[(int) buf.readableBytes()];
    buf.getBytes(0, bytes);
    return bytes;
  }
}
