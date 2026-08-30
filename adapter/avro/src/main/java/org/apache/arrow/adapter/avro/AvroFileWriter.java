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
package org.apache.arrow.adapter.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.avro.Schema;
import org.apache.avro.file.Codec;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

class AvroFileWriter {

  // Use magic from Avro's own constants
  private static final byte[] AVRO_MAGIC = DataFileConstants.MAGIC;
  private static final int SYNC_MARKER_SIZE = 16;

  private final OutputStream stream;
  private final Encoder encoder;

  private final BufferOutputStream batchStream;
  private BinaryEncoder batchEncoder;
  private VectorSchemaRoot batch;

  private final Schema avroSchema;
  private final byte[] syncMarker;

  private final CompositeAvroProducer recordProducer;
  private final Codec avroCodec;

  public AvroFileWriter(
      OutputStream stream, VectorSchemaRoot firstBatch, DictionaryProvider dictionaries)
      throws IOException {

    this(stream, firstBatch, dictionaries, null);
  }

  public AvroFileWriter(
      OutputStream stream,
      VectorSchemaRoot firstBatch,
      DictionaryProvider dictionaries,
      String codecName)
      throws IOException {

    EncoderFactory encoderFactory = EncoderFactory.get();

    this.stream = stream;
    this.encoder = encoderFactory.binaryEncoder(stream, null);

    this.batchStream = new BufferOutputStream();
    this.batchEncoder = encoderFactory.binaryEncoder(stream, null);
    this.batch = firstBatch;

    try {

      this.avroSchema =
          ArrowToAvroUtils.createAvroSchema(firstBatch.getSchema().getFields(), dictionaries);

      this.recordProducer =
          ArrowToAvroUtils.createCompositeProducer(firstBatch.getFieldVectors(), dictionaries);

      // Generate a random sync marker
      var random = new Random();
      this.syncMarker = new byte[SYNC_MARKER_SIZE];
      random.nextBytes(this.syncMarker);

      // Look up the compression codec
      this.avroCodec = AvroCompression.getAvroCodec(codecName);
    } catch (Throwable e) {
      // Do not leak the batch buffer if there are problems during setup
      batchStream.close();
      throw e;
    }
  }

  // Write the Avro header (throws if already written)
  public void writeHeader() throws IOException {

    // Prepare the metadata map
    Map<String, byte[]> metadata = new HashMap<>();
    metadata.put("avro.schema", avroSchema.toString().getBytes(StandardCharsets.UTF_8));
    metadata.put("avro.codec", avroCodec.getName().getBytes(StandardCharsets.UTF_8));

    // Avro magic
    encoder.writeFixed(AVRO_MAGIC);

    // Write the metadata map
    encoder.writeMapStart();
    encoder.setItemCount(metadata.size());
    for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
      encoder.startItem();
      encoder.writeString(entry.getKey());
      encoder.writeBytes(entry.getValue());
    }
    encoder.writeMapEnd();

    // Sync marker denotes end of the header
    encoder.writeFixed(this.syncMarker);
    encoder.flush();
  }

  // Write the contents of the VSR as an Avro data block
  // Writes header if not yet written
  // Expects new data to be in the batch (i.e. VSR can be recycled)
  public void writeBatch() throws IOException {

    // Prepare batch stream and encoder
    batchStream.reset();
    batchEncoder = EncoderFactory.get().directBinaryEncoder(batchStream, batchEncoder);

    // Reset producers
    recordProducer.getProducers().forEach(producer -> producer.setPosition(0));

    // Produce a batch, writing to the batch stream (buffer)
    for (int row = 0; row < batch.getRowCount(); row++) {
      recordProducer.produce(batchEncoder);
    }

    batchEncoder.flush();

    // Compress the batch buffer using Avro's codecs
    ByteBuffer batchBuffer = ByteBuffer.wrap(batchStream.internalBuffer());
    ByteBuffer batchCompressed = avroCodec.compress(batchBuffer);

    // Write Avro block to the main encoder
    encoder.writeLong(batch.getRowCount());
    encoder.writeBytes(batchCompressed);
    encoder.writeFixed(syncMarker);
  }

  // Reset vectors in all the producers
  // Supports a stream of VSRs if source VSR is not recycled
  void resetBatch(VectorSchemaRoot batch) {
    recordProducer.resetProducerVectors(batch);
    this.batch = batch;
  }

  public void flush() throws IOException {
    encoder.flush();
  }

  // Closes encoder and / or channel
  // Does not close VSR or dictionary vectors
  public void close() throws IOException {
    encoder.flush();
    stream.close();
    batchStream.close();
  }

  private static final class BufferOutputStream extends ByteArrayOutputStream {

    byte[] internalBuffer() {
      return buf;
    }
  }
}
