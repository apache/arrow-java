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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.WireFormat;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestArrowMessage {

  private static final int HEADER_TAG =
      (FlightData.DATA_HEADER_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int APP_METADATA_TAG =
      (FlightData.APP_METADATA_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  /**
   * A field whose declared length is far larger than the bytes actually present in the frame must
   * be rejected before anything is allocated for it, rather than driving an allocation sized by the
   * attacker-controlled length prefix.
   */
  @Test
  public void frameRejectsOversizedFieldLength() {
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    writeRawVarint32(frame, HEADER_TAG);
    // Claim a much larger length than the (zero) bytes that follow.
    writeRawVarint32(frame, 1 << 20);

    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    final Exception e =
        assertThrows(
            Exception.class, () -> marshaller.parse(new ByteArrayInputStream(frame.toByteArray())));
    assertTrue(
        e.getMessage() != null && e.getMessage().contains("exceeds"),
        "unexpected failure: " + e.getMessage());
  }

  /** A well-formed field whose length matches the bytes present still parses. */
  @Test
  public void frameAcceptsWellFormedField() throws Exception {
    final byte[] payload = new byte[] {1, 2, 3, 4};
    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    writeRawVarint32(frame, APP_METADATA_TAG);
    writeRawVarint32(frame, payload.length);
    frame.write(payload);

    final MethodDescriptor.Marshaller<ArrowMessage> marshaller =
        ArrowMessage.createMarshaller(allocator);
    try (ArrowMessage message = marshaller.parse(new ByteArrayInputStream(frame.toByteArray()))) {
      assertNotNull(message.getApplicationMetadata());
    }
  }

  private static void writeRawVarint32(ByteArrayOutputStream out, int value) {
    while (true) {
      if ((value & ~0x7F) == 0) {
        out.write(value);
        return;
      }
      out.write((value & 0x7F) | 0x80);
      value >>>= 7;
    }
  }
}
