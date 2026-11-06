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
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

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

  /** Writes one field into a frame. */
  @FunctionalInterface
  interface FieldWriter {
    void write(CodedOutputStream out) throws IOException;
  }

  private static final Schema SCHEMA =
      new Schema(Collections.singletonList(Field.nullable("foo", new ArrowType.Int(32, true))));

  /**
   * Payload whose bytes look like the descriptor, header and app_metadata tags (0x0A, 0x12, 0x1A).
   * A parser that doesn't skip the field reads these as tags, and the following byte as a length.
   */
  private static byte[] tagLikePayload(int length) {
    final byte[] tags = {0x0A, 0x12, 0x1A};
    final byte[] payload = new byte[length];
    for (int i = 0; i < length; i++) {
      payload[i] = tags[i % tags.length];
    }
    return payload;
  }

  /**
   * Fields the parser doesn't know, or knows with another wire type. Every value is chosen so that
   * its bytes can be mistaken for a tag if the field is not skipped properly.
   */
  static Stream<Arguments> unknownFields() {
    final List<Named<FieldWriter>> fields = new ArrayList<>();
    // Varints equal to a descriptor, header and app_metadata tag; and values that need 10 bytes.
    for (long value : new long[] {10, 18, 26, -1L, Long.MIN_VALUE}) {
      fields.add(Named.of("varint " + value, out -> out.writeUInt64(4, value)));
    }
    for (int length : new int[] {0, 10, 18, 26}) {
      fields.add(
          Named.of(
              "length-delimited of " + length + " bytes",
              out -> out.writeByteArray(5, tagLikePayload(length))));
    }
    fields.add(Named.of("fixed64", out -> out.writeFixed64(6, 0x0A121A0A121A0A12L)));
    fields.add(Named.of("fixed32", out -> out.writeFixed32(7, 0x0A121A0A)));
    fields.add(
        Named.of(
            "group",
            out -> {
              out.writeTag(8, WireFormat.WIRETYPE_START_GROUP);
              out.writeUInt64(1, 10);
              out.writeByteArray(2, tagLikePayload(18));
              out.writeTag(8, WireFormat.WIRETYPE_END_GROUP);
            }));
    fields.add(
        Named.of(
            "group in group",
            out -> {
              out.writeTag(8, WireFormat.WIRETYPE_START_GROUP);
              out.writeTag(9, WireFormat.WIRETYPE_START_GROUP);
              out.writeFixed32(1, 0x0A121A0A);
              out.writeTag(9, WireFormat.WIRETYPE_END_GROUP);
              out.writeUInt64(1, 26);
              out.writeTag(8, WireFormat.WIRETYPE_END_GROUP);
            }));
    // Known field numbers with a wire type other than the expected one. As in protobuf, these are
    // treated as unknown fields: they are skipped, not rejected.
    fields.add(
        Named.of(
            "descriptor field as varint",
            out -> out.writeUInt64(FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER, 10)));
    fields.add(
        Named.of(
            "header field as fixed32",
            out -> out.writeFixed32(FlightData.DATA_HEADER_FIELD_NUMBER, 18)));
    fields.add(
        Named.of(
            "app_metadata field as fixed64",
            out -> out.writeFixed64(FlightData.APP_METADATA_FIELD_NUMBER, 26)));
    fields.add(
        Named.of(
            "body field as varint", out -> out.writeUInt64(FlightData.DATA_BODY_FIELD_NUMBER, 26)));

    // Create a test case for each stream type.
    return Arrays.stream(StreamType.values())
        .flatMap(streamType -> fields.stream().map(field -> Arguments.of(streamType, field)));
  }

  /** A frame of the known fields, with {@code unknown} written before the known field at index. */
  private static byte[] frameWithUnknownField(FieldWriter unknown, int index) throws IOException {
    final FieldWriter[] known = {
      out ->
          out.writeMessage(
              FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER,
              FlightDescriptor.command(FIRST).toProtocol()),
      out ->
          out.writeBytes(
              FlightData.DATA_HEADER_FIELD_NUMBER,
              ByteString.copyFrom(MessageSerializer.serializeMetadata(SCHEMA, IpcOption.DEFAULT))),
      out -> out.writeByteArray(FlightData.APP_METADATA_FIELD_NUMBER, LAST),
    };
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    final CodedOutputStream out = CodedOutputStream.newInstance(frame);
    for (int i = 0; i <= known.length; i++) {
      if (i == index) {
        unknown.write(out);
      }
      if (i < known.length) {
        known[i].write(out);
      }
    }
    out.flush();
    return frame.toByteArray();
  }

  private void assertParsesAsKnownFields(StreamType streamType, byte[] frame) throws Exception {
    try (ArrowMessage message = marshaller.parse(streamType.open(frame))) {
      assertEquals(FlightDescriptor.command(FIRST).toProtocol(), message.getDescriptor());
      assertEquals(SCHEMA, message.asSchema());
      assertArrayEquals(LAST, toByteArray(message.getApplicationMetadata()));
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  /** An unknown field after the known ones is skipped. */
  @ParameterizedTest
  @MethodSource("unknownFields")
  public void frameSkipsUnknownField(StreamType streamType, FieldWriter unknown) throws Exception {
    assertParsesAsKnownFields(streamType, frameWithUnknownField(unknown, 3));
  }

  /** An unknown field is skipped wherever it sits among the known ones. */
  @ParameterizedTest
  @EnumSource
  public void frameSkipsUnknownFieldAtAnyPosition(StreamType streamType) throws Exception {
    // The payload looks like a descriptor, a header and an app_metadata tag in a row.
    final FieldWriter unknown = out -> out.writeByteArray(5, tagLikePayload(26));
    for (int index = 0; index <= 3; index++) {
      assertParsesAsKnownFields(streamType, frameWithUnknownField(unknown, index));
    }
  }

  /** Groups nested as deeply as allowed are skipped. */
  @ParameterizedTest
  @EnumSource
  public void frameSkipsGroupsNestedUpToLimit(StreamType streamType) throws Exception {
    final int limit = ArrowMessage.MAX_PROTOBUF_SKIP_GROUP_DEPTH;
    try (ArrowMessage message = marshaller.parse(streamType.open(nestedGroups(limit, limit)))) {
      assertNull(message.getApplicationMetadata());
    }
  }

  /** Groups nested more deeply than allowed are rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsGroupsNestedBeyondLimit(StreamType streamType) throws Exception {
    final int limit = ArrowMessage.MAX_PROTOBUF_SKIP_GROUP_DEPTH;
    assertRejected(streamType, nestedGroups(limit + 1, limit + 1));
  }

  /** A group must be closed by an end group with the same field number. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsGroupEndedByOtherGroup(StreamType streamType) throws Exception {
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    final CodedOutputStream out = CodedOutputStream.newInstance(frame);
    out.writeTag(8, WireFormat.WIRETYPE_START_GROUP);
    out.writeTag(9, WireFormat.WIRETYPE_END_GROUP);
    out.flush();
    assertRejected(streamType, frame.toByteArray());
  }

  /** A group that is still open at the end of the frame is rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsUnterminatedGroup(StreamType streamType) throws Exception {
    assertRejected(streamType, nestedGroups(2, 0));
  }

  /** An end group without a start group is rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsEndGroupWithoutStartGroup(StreamType streamType) throws Exception {
    assertRejected(streamType, nestedGroups(0, 1));
  }

  /** {@code opened} start group tags (field numbers 4..) followed by {@code closed} end tags. */
  private static byte[] nestedGroups(int opened, int closed) throws IOException {
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    final CodedOutputStream out = CodedOutputStream.newInstance(frame);
    for (int i = 1; i <= opened; i++) {
      out.writeTag(i + 3, WireFormat.WIRETYPE_START_GROUP);
    }
    for (int i = closed; i >= 1; i--) {
      out.writeTag(i + 3, WireFormat.WIRETYPE_END_GROUP);
    }
    out.flush();
    return frame.toByteArray();
  }

  /** Invalid wire types cannot be skipped and are rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsUnskippableWireTypes(StreamType streamType) throws Exception {
    for (int wireType : new int[] {WireFormat.WIRETYPE_END_GROUP, 6, 7}) {
      final ByteArrayOutputStream frame = new ByteArrayOutputStream();
      final CodedOutputStream out = CodedOutputStream.newInstance(frame);
      out.writeTag(5, wireType);
      out.flush();
      assertRejected(streamType, frame.toByteArray());
    }
  }

  /** A skipped field that runs past the end of the frame is rejected. */
  @ParameterizedTest
  @EnumSource
  public void frameRejectsTruncatedUnknownField(StreamType streamType) throws Exception {
    final byte[][] frames = {
      fieldPrefix(5, 10),
      {(byte) ((6 << 3) | WireFormat.WIRETYPE_FIXED64), 1, 2, 3},
      {(byte) ((7 << 3) | WireFormat.WIRETYPE_FIXED32), 1},
      {(byte) ((4 << 3) | WireFormat.WIRETYPE_VARINT), (byte) 0x80},
    };
    for (byte[] frame : frames) {
      assertRejected(streamType, frame);
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
