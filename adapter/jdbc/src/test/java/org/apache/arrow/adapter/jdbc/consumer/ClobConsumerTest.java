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
package org.apache.arrow.adapter.jdbc.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import org.apache.arrow.adapter.jdbc.ResultSetUtility;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

public class ClobConsumerTest extends AbstractConsumerTest {

  private static final int INITIAL_VALUE_ALLOCATION = BaseValueVector.INITIAL_VALUE_ALLOCATION;
  private static final int DEFAULT_RECORD_BYTE_COUNT = 8;

  private void assertConsume(boolean nullable, String value) throws SQLException, IOException {
    try (final VarCharVector vector = new VarCharVector("clob", allocator)) {
      ClobConsumer consumer = ClobConsumer.createConsumer(vector, 0, nullable);
      consumer.consume(new SingleClobResultSet(value));
      assertEquals(0, vector.getLastSet());
      assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), vector.get(0));
    }
  }

  @Test
  public void testConsumeSmallClob() throws SQLException, IOException {
    assertConsume(false, repeat('a', DEFAULT_RECORD_BYTE_COUNT));
  }

  @Test
  public void testConsumeClobLargerThanDataBuffer() throws SQLException, IOException {
    // A CLOB whose bytes exceed the VarCharVector's initial data-buffer capacity must grow the
    // data buffer, not write past it.
    assertConsume(false, repeat('a', INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT * 4));
  }

  private static String repeat(char c, int count) {
    char[] chars = new char[count];
    java.util.Arrays.fill(chars, c);
    return new String(chars);
  }

  /** A {@link java.sql.ResultSet} that returns a single non-null CLOB for any column. */
  private static class SingleClobResultSet extends ResultSetUtility.ThrowingResultSet {
    private final Clob clob;

    SingleClobResultSet(String value) {
      this.clob = new StringClob(value);
    }

    @Override
    public Clob getClob(int columnIndex) {
      return clob;
    }

    @Override
    public boolean wasNull() {
      return false;
    }
  }

  /** Minimal read-only {@link Clob} backed by a {@link String}. */
  private static class StringClob implements Clob {
    private final String value;

    StringClob(String value) {
      this.value = value;
    }

    @Override
    public long length() {
      return value.length();
    }

    @Override
    public String getSubString(long pos, int length) {
      int start = (int) (pos - 1);
      int end = Math.min(value.length(), start + length);
      return value.substring(start, end);
    }

    @Override
    public Reader getCharacterStream() {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position(String searchstr, long start) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position(Clob searchstr, long start) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int setString(long pos, String str) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int setString(long pos, String str, int offset, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.io.OutputStream setAsciiStream(long pos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.io.Writer setCharacterStream(long pos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void truncate(long len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void free() {}

    @Override
    public Reader getCharacterStream(long pos, long length) {
      throw new UnsupportedOperationException();
    }
  }
}
