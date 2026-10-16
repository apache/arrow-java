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
package org.apache.arrow.dataset.file;

/**
 * Configuration properties for Parquet Writer. Allows customization of Parquet file writing
 * behavior. Use {@link Builder} to create instances.
 */
public class ParquetWriterProperties {

  private final long writeBatchSize;
  private final long maxRowGroupLength;
  private final long dataPageSize;
  private final int compressionLevel;
  private final String compressionCodec;
  private final boolean writePageIndex;
  private final boolean useThreads;

  private ParquetWriterProperties(Builder builder) {
    this.writeBatchSize = builder.writeBatchSize;
    this.maxRowGroupLength = builder.maxRowGroupLength;
    this.dataPageSize = builder.dataPageSize;
    this.compressionLevel = builder.compressionLevel;
    this.compressionCodec = builder.compressionCodec;
    this.writePageIndex = builder.writePageIndex;
    this.useThreads = builder.useThreads;
  }

  /**
   * Get the write batch size (number of rows per batch).
   *
   * @return write batch size, or -1 if not set
   */
  public long getWriteBatchSize() {
    return writeBatchSize;
  }

  /**
   * Create a new Builder for ParquetWriterProperties.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Get the maximum row group length.
   *
   * @return maximum row group length, or -1 if not set
   */
  public long getMaxRowGroupLength() {
    return maxRowGroupLength;
  }

  /**
   * Get the data page size.
   *
   * @return data page size, or -1 if not set
   */
  public long getDataPageSize() {
    return dataPageSize;
  }

  /**
   * Get the compression level.
   *
   * @return compression level, or -1 if not set
   */
  public int getCompressionLevel() {
    return compressionLevel;
  }

  /**
   * Get the compression codec.
   *
   * @return compression codec name, or null if not set
   */
  public String getCompressionCodec() {
    return compressionCodec;
  }

  /**
   * Get whether page index writing is enabled.
   *
   * @return true if page index writing is enabled
   */
  public boolean getWritePageIndex() {
    return writePageIndex;
  }

  /**
   * Get whether multi-threading is enabled.
   *
   * @return true if multi-threading is enabled
   */
  public boolean getUseThreads() {
    return useThreads;
  }

  /** Builder for creating ParquetWriterProperties instances. */
  public static class Builder {
    private long maxRowGroupLength = -1;
    private long dataPageSize = -1;
    private int compressionLevel = -1;
    private String compressionCodec = null;
    private boolean writePageIndex = false;
    private boolean useThreads = false;
    private long writeBatchSize = -1;

    private Builder() {}

    /**
     * Set the write batch size (number of rows per batch).
     *
     * @param writeBatchSize number of rows per batch
     * @return this builder for method chaining
     */
    public Builder writeBatchSize(long writeBatchSize) {
      this.writeBatchSize = writeBatchSize;
      return this;
    }

    /**
     * Set the maximum row group length (in rows).
     *
     * @param maxRowGroupLength maximum number of rows per row group
     * @return this builder for method chaining
     */
    public Builder maxRowGroupLength(long maxRowGroupLength) {
      this.maxRowGroupLength = maxRowGroupLength;
      return this;
    }

    /**
     * Set the data page size (in bytes).
     *
     * @param dataPageSize data page size in bytes
     * @return this builder for method chaining
     */
    public Builder dataPageSize(long dataPageSize) {
      this.dataPageSize = dataPageSize;
      return this;
    }

    /**
     * Set the compression level.
     *
     * @param compressionLevel compression level (typically 1-9)
     * @return this builder for method chaining
     */
    public Builder compressionLevel(int compressionLevel) {
      this.compressionLevel = compressionLevel;
      return this;
    }

    /**
     * Set the compression codec.
     *
     * @param compressionCodec compression codec name (e.g., "SNAPPY", "GZIP", "LZ4", "ZSTD")
     * @return this builder for method chaining
     */
    public Builder compressionCodec(String compressionCodec) {
      this.compressionCodec = compressionCodec;
      return this;
    }

    /**
     * Enable or disable page index writing.
     *
     * @param writePageIndex whether to write page index
     * @return this builder for method chaining
     */
    public Builder writePageIndex(boolean writePageIndex) {
      this.writePageIndex = writePageIndex;
      return this;
    }

    /**
     * Enable or disable multi-threading for column writing.
     *
     * @param useThreads whether to use threads for column writing
     * @return this builder for method chaining
     */
    public Builder useThreads(boolean useThreads) {
      this.useThreads = useThreads;
      return this;
    }

    /**
     * Build a ParquetWriterProperties instance with the configured values.
     *
     * @return a new ParquetWriterProperties instance
     */
    public ParquetWriterProperties build() {
      return new ParquetWriterProperties(this);
    }
  }
}
