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
package org.apache.arrow.vector.variant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Wrapper around parquet-variant's Variant implementation.
 *
 * <p>This wrapper exists to isolate the parquet-variant dependency from Arrow's public API,
 * allowing the vector module to expose variant functionality without requiring users to depend on
 * parquet-variant directly. It also ensures that nested variant values (from arrays and objects)
 * are consistently wrapped.
 */
public class Variant {

  private final org.apache.parquet.variant.Variant delegate;

  /** Creates a Variant from raw metadata and value byte arrays. */
  public Variant(byte[] metadata, byte[] value) {
    this.delegate = new org.apache.parquet.variant.Variant(value, metadata);
  }

  /** Creates a Variant by copying data from ArrowBuf instances. */
  public Variant(
      ArrowBuf metadataBuffer,
      int metadataStart,
      int metadataEnd,
      ArrowBuf valueBuffer,
      int valueStart,
      int valueEnd) {
    byte[] metadata = new byte[metadataEnd - metadataStart];
    byte[] value = new byte[valueEnd - valueStart];
    metadataBuffer.getBytes(metadataStart, metadata);
    valueBuffer.getBytes(valueStart, value);
    this.delegate = new org.apache.parquet.variant.Variant(value, metadata);
  }

  private Variant(org.apache.parquet.variant.Variant delegate) {
    this.delegate = delegate;
  }

  public ByteBuffer getValueBuffer() {
    return delegate.getValueBuffer();
  }

  public ByteBuffer getMetadataBuffer() {
    return delegate.getMetadataBuffer();
  }

  public boolean getBoolean() {
    return delegate.getBoolean();
  }

  public byte getByte() {
    return delegate.getByte();
  }

  public short getShort() {
    return delegate.getShort();
  }

  public int getInt() {
    return delegate.getInt();
  }

  public long getLong() {
    return delegate.getLong();
  }

  public double getDouble() {
    return delegate.getDouble();
  }

  public BigDecimal getDecimal() {
    return delegate.getDecimal();
  }

  public float getFloat() {
    return delegate.getFloat();
  }

  public ByteBuffer getBinary() {
    return delegate.getBinary();
  }

  public UUID getUUID() {
    return delegate.getUUID();
  }

  public String getString() {
    return delegate.getString();
  }

  public Type getType() {
    return Type.fromParquet(delegate.getType());
  }

  public int numObjectElements() {
    return delegate.numObjectElements();
  }

  public Variant getFieldByKey(String key) {
    org.apache.parquet.variant.Variant result = delegate.getFieldByKey(key);
    return result != null ? wrap(result) : null;
  }

  public ObjectField getFieldAtIndex(int idx) {
    org.apache.parquet.variant.Variant.ObjectField field = delegate.getFieldAtIndex(idx);
    return new ObjectField(field.key, wrap(field.value));
  }

  public int numArrayElements() {
    return delegate.numArrayElements();
  }

  public Variant getElementAtIndex(int index) {
    org.apache.parquet.variant.Variant result = delegate.getElementAtIndex(index);
    return result != null ? wrap(result) : null;
  }

  private static Variant wrap(org.apache.parquet.variant.Variant parquetVariant) {
    return new Variant(parquetVariant);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Variant variant = (Variant) o;
    return delegate.getMetadataBuffer().equals(variant.delegate.getMetadataBuffer())
        && delegate.getValueBuffer().equals(variant.delegate.getValueBuffer());
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate.getMetadataBuffer(), delegate.getValueBuffer());
  }

  @Override
  public String toString() {
    return "Variant{type=" + getType() + '}';
  }

  public enum Type {
    OBJECT,
    ARRAY,
    NULL,
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    STRING,
    DOUBLE,
    DECIMAL4,
    DECIMAL8,
    DECIMAL16,
    DATE,
    TIMESTAMP_TZ,
    TIMESTAMP_NTZ,
    FLOAT,
    BINARY,
    TIME,
    TIMESTAMP_NANOS_TZ,
    TIMESTAMP_NANOS_NTZ,
    UUID;

    static Type fromParquet(org.apache.parquet.variant.Variant.Type parquetType) {
      return Type.valueOf(parquetType.name());
    }
  }

  public static final class ObjectField {
    public final String key;
    public final Variant value;

    public ObjectField(String key, Variant value) {
      this.key = key;
      this.value = value;
    }
  }
}
