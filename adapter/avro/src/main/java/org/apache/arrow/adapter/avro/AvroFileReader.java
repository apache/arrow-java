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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.arrow.adapter.avro.consumers.CompositeAvroConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

class AvroFileReader implements DictionaryProvider {

  // Writer owns a channel / decoder and will close them
  // Schema / VSR / dictionaries are created when header is read
  // VSR / dictionaries are cleaned up on close
  // Dictionaries accessible through DictionaryProvider iface

  // Use magic from Avro's own constants
  private static final byte[] AVRO_MAGIC = DataFileConstants.MAGIC;
  private static final int SYNC_MARKER_SIZE = 16;

  private final InputStream stream;
  private final BinaryDecoder decoder;
  private final BufferAllocator allocator;
  private final boolean blocking;

  private org.apache.avro.Schema avroSchema;
  private String avroCodec;
  private final byte[] syncMarker;

  private CompositeAvroConsumer recordConsumer;
  private VectorSchemaRoot arrowBatch;
  private Schema arrowSchema;
  private DictionaryProvider.MapDictionaryProvider dictionaries;

  private long nextBatchPosition;
  private ByteBuffer batchBuffer;
  private BinaryDecoder batchDecoder;
  private final byte[] batchSyncMarker;

  // Create a new AvroFileReader for the input stream
  // In order to support non-blocking mode, the stream must support mark / reset
  public AvroFileReader(InputStream stream, BufferAllocator allocator, boolean blocking) {

    this.stream = stream;
    this.allocator = allocator;
    this.blocking = blocking;

    if (blocking) {
      this.decoder = DecoderFactory.get().binaryDecoder(stream, null);
    } else {
      if (!stream.markSupported()) {
        throw new IllegalArgumentException(
            "Input stream must support mark/reset for non-blocking mode");
      }
      this.decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
    }

    this.syncMarker = new byte[SYNC_MARKER_SIZE];
    this.batchSyncMarker = new byte[SYNC_MARKER_SIZE];
  }

  // Read the Avro header and set up schema / VSR / dictionaries
  void readHeader() throws IOException {

    if (avroSchema != null) {
      throw new IllegalStateException("Avro header has already been read");
    }

    // Keep track of the header size
    long headerSize = 0;

    // Read Avro magic
    byte[] magic = new byte[AVRO_MAGIC.length];
    decoder.readFixed(magic);
    headerSize += magic.length;

    // Validate Avro magic
    int validateMagic =
        BinaryData.compareBytes(AVRO_MAGIC, 0, AVRO_MAGIC.length, magic, 0, AVRO_MAGIC.length);

    if (validateMagic != 0) {
      throw new RuntimeException("Invalid AVRO data file: The file is not an Avro file");
    }

    // Read the metadata map
    for (long count = decoder.readMapStart(); count != 0; count = decoder.mapNext()) {

      headerSize += zigzagSize(count);

      for (long i = 0; i < count; i++) {

        ByteBuffer keyBuffer = decoder.readBytes(null);
        ByteBuffer valueBuffer = decoder.readBytes(null);

        headerSize += zigzagSize(keyBuffer.remaining()) + keyBuffer.remaining();
        headerSize += zigzagSize(valueBuffer.remaining()) + valueBuffer.remaining();

        String key = new String(keyBuffer.array(), StandardCharsets.UTF_8);

        // Handle header entries for schema and codec
        if ("avro.schema".equals(key)) {
          avroSchema = processSchema(valueBuffer);
        } else if ("avro.codec".equals(key)) {
          avroCodec = processCodec(valueBuffer);
        }
      }
    }

    // End of map marker
    headerSize += 1;

    // Sync marker denotes end of the header
    decoder.readFixed(syncMarker);
    headerSize += syncMarker.length;

    // Schema must always be present
    if (avroSchema == null) {
      throw new RuntimeException("Invalid AVRO data file: Schema missing in file header");
    }

    // Prepare read config
    this.dictionaries = new DictionaryProvider.MapDictionaryProvider();
    AvroToArrowConfig config = new AvroToArrowConfig(allocator, 0, dictionaries, Set.of(), false);

    // Calling this method will also populate the dictionary map
    this.recordConsumer = AvroToArrowUtils.createCompositeConsumer(avroSchema, config);

    // Initialize data vectors
    List<FieldVector> vectors = new ArrayList<>(arrowSchema.getFields().size());
    for (int i = 0; i < arrowSchema.getFields().size(); i++) {
      FieldVector vector = recordConsumer.getConsumers().get(i).getVector();
      vectors.add(vector);
    }

    // Initialize batch and schema
    this.arrowBatch = new VectorSchemaRoot(vectors);
    this.arrowSchema = arrowBatch.getSchema();

    // First batch starts after the header
    this.nextBatchPosition = headerSize;
  }

  private org.apache.avro.Schema processSchema(ByteBuffer buffer) throws IOException {

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

    try (InputStream schemaStream = new ByteArrayInputStream(buffer.array())) {
      return parser.parse(schemaStream);
    }
  }

  private String processCodec(ByteBuffer buffer) {

    if (buffer != null && buffer.remaining() > 0) {
      return new String(buffer.array(), StandardCharsets.UTF_8);
    } else {
      return DataFileConstants.NULL_CODEC;
    }
  }

  // Schema and VSR available after readHeader()
  Schema getSchema() {
    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }
    return arrowSchema;
  }

  VectorSchemaRoot getVectorSchemaRoot() {
    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }
    return arrowBatch;
  }

  @Override
  public Set<Long> getDictionaryIds() {
    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }
    return dictionaries.getDictionaryIds();
  }

  @Override
  public Dictionary lookup(long id) {
    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }
    return dictionaries.lookup(id);
  }

  // Read the next Avro block and load it into the VSR
  // Return true if successful, false if EOS
  // Also false in non-blocking mode if need more data
  boolean readBatch() throws IOException {

    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }

    if (!hasNextBatch()) {
      return false;
    }

    // Read Avro block from the main encoder
    long nRows = decoder.readLong();
    batchBuffer = decoder.readBytes(batchBuffer);
    decoder.readFixed(batchSyncMarker);

    long batchSize =
        zigzagSize(nRows)
            + zigzagSize(batchBuffer.remaining())
            + batchBuffer.remaining()
            + SYNC_MARKER_SIZE;

    // Validate sync marker - mismatch indicates a corrupt file
    int validateMarker =
        BinaryData.compareBytes(
            syncMarker, 0, SYNC_MARKER_SIZE, batchSyncMarker, 0, SYNC_MARKER_SIZE);

    if (validateMarker != 0) {
      throw new RuntimeException("Invalid AVRO data file: The file is corrupted");
    }

    // Reset producers
    recordConsumer
        .getConsumers()
        .forEach(consumer -> ensureCapacity(consumer.getVector(), (int) nRows));
    recordConsumer.getConsumers().forEach(consumer -> consumer.setPosition(0));

    // Decompress the batch buffer using Avro's codecs
    var codec = AvroCompression.getAvroCodec(avroCodec);
    var batchDecompressed = codec.decompress(batchBuffer);

    // Prepare batch stream and decoder
    try (InputStream batchStream = new ByteArrayInputStream(batchDecompressed.array())) {

      batchDecoder = DecoderFactory.get().directBinaryDecoder(batchStream, batchDecoder);

      // Consume a batch, reading from the batch stream (buffer)
      for (int row = 0; row < nRows; row++) {
        recordConsumer.consume(batchDecoder);
      }

      arrowBatch.setRowCount((int) nRows);

      // Update next batch position
      nextBatchPosition += batchSize;

      // Batch is ready
      return true;
    }
  }

  private void ensureCapacity(FieldVector vector, int capacity) {
    if (vector.getValueCapacity() < capacity) {
      vector.setInitialCapacity(capacity);
    }
  }

  // Check for position and size of the next Avro data block
  // Provides a mechanism for non-blocking / reactive styles
  boolean hasNextBatch() throws IOException {

    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }

    if (blocking) {
      return !decoder.isEnd();
    }

    var in = decoder.inputStream();
    in.mark(1);

    try {

      int nextByte = in.read();
      in.reset();

      return nextByte >= 0;
    } catch (EOFException e) {
      return false;
    }
  }

  long nextBatchPosition() {

    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }

    return nextBatchPosition;
  }

  public long nextBatchSize() throws IOException {

    if (avroSchema == null) {
      throw new IllegalStateException("Avro header has not been read yet");
    }

    if (blocking) {
      throw new IllegalStateException("Next batch size is only available in non-blocking mode");
    }

    InputStream in = decoder.inputStream();
    in.mark(20);

    long nRows = decoder.readLong();
    long nBytes = decoder.readLong();

    in.reset();

    return zigzagSize(nRows) + zigzagSize(nBytes) + nBytes + SYNC_MARKER_SIZE;
  }

  private int zigzagSize(long n) {

    long val = (n << 1) ^ (n >> 63); // move sign to low-order bit
    int bytes = 1;

    while ((val & ~0x7F) != 0) {
      bytes += 1;
      val >>>= 7;
    }

    return bytes;
  }

  // Closes encoder and / or channel
  // Also closes VSR and dictionary vectors
  void close() throws IOException {

    stream.close();

    if (arrowBatch != null) {
      arrowBatch.close();
    }

    if (dictionaries != null) {
      for (long dictionaryId : dictionaries.getDictionaryIds()) {
        Dictionary dictionary = dictionaries.lookup(dictionaryId);
        dictionary.getVector().close();
      }
    }
  }
}
