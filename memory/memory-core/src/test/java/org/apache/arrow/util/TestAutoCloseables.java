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
package org.apache.arrow.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TestAutoCloseables {

  /** Closeable that records that it was closed and can optionally throw. */
  private static final class TrackCloseable implements AutoCloseable {
    private boolean closed;
    private final Exception toThrow;

    TrackCloseable() {
      this.toThrow = null;
    }

    TrackCloseable(Exception toThrow) {
      this.toThrow = toThrow;
    }

    @Override
    public void close() throws Exception {
      closed = true;
      if (toThrow != null) {
        throw toThrow;
      }
    }

    boolean isClosed() {
      return closed;
    }
  }

  @Test
  public void testCloseVarargsIgnoresNulls() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    AutoCloseables.close(a, null, b);
    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
  }

  @Test
  public void testCloseVarargsThrowsFirstExceptionAndSuppressesRest() throws Exception {
    Exception e1 = new Exception("first");
    Exception e2 = new Exception("second");
    TrackCloseable c1 = new TrackCloseable(e1);
    TrackCloseable c2 = new TrackCloseable(e2);
    Exception thrown = assertThrows(Exception.class, () -> AutoCloseables.close(c1, c2));
    assertEquals("first", thrown.getMessage());
    assertTrue(Arrays.asList(thrown.getSuppressed()).contains(e2));
  }

  @Test
  public void testCloseIterableNullIterableReturns() throws Exception {
    AutoCloseables.close((List<AutoCloseable>) null); // no exception
  }

  @Test
  public void testCloseIterableIgnoresNullElements() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    List<AutoCloseable> list = Arrays.asList(a, null, b);
    AutoCloseables.close(list);
    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
  }

  @Test
  public void testCloseIterableWhenIterableIsAlsoAutoCloseable() throws Exception {
    TrackCloseable iter = new TrackCloseable();
    TrackCloseable inner = new TrackCloseable();
    // When the Iterable itself implements AutoCloseable (e.g. VectorContainer),
    // close(Iterable) calls close() on it and does not iterate over elements
    class IterableCloseable implements Iterable<AutoCloseable>, AutoCloseable {
      @Override
      @SuppressWarnings("unchecked")
      public Iterator<AutoCloseable> iterator() {
        return (Iterator<AutoCloseable>) Collections.singletonList(inner);
      }

      @Override
      public void close() throws Exception {
        iter.close();
      }
    }
    AutoCloseables.close(new IterableCloseable());
    assertTrue(iter.isClosed());
    assertFalse(inner.isClosed());
  }

  @Test
  public void testCloseIterableVarargsWithNullIterables() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    TrackCloseable c = new TrackCloseable();
    List<AutoCloseable> list1 = Arrays.asList(null, a, b);
    List<AutoCloseable> list2 = Collections.singletonList(c);
    AutoCloseables.close(list1, null, list2);
    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
    assertTrue(c.isClosed());
  }

  @Test
  public void testCloseThrowableSuppressesException() {
    Exception e = new Exception("from close");
    TrackCloseable c = new TrackCloseable(e);
    Exception main = new Exception("main");
    AutoCloseables.close(main, c);
    assertTrue(c.isClosed());
    assertEquals(1, main.getSuppressed().length);
    assertEquals(e, main.getSuppressed()[0]);
  }

  @Test
  public void testCloseThrowableWithNullCloseables() {
    Exception main = new Exception("main");
    AutoCloseables.close(main, (AutoCloseable) null);
    assertEquals(0, main.getSuppressed().length);

    AutoCloseables.close(main, (AutoCloseable[]) null); // no exception
  }

  @Test
  public void testIterFiltersNulls() {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    Iterable<AutoCloseable> it = AutoCloseables.iter(a, null, b);
    List<AutoCloseable> list = new ArrayList<>();
    it.forEach(list::add);
    assertEquals(2, list.size());
    assertTrue(list.contains(a));
    assertTrue(list.contains(b));
  }

  @Test
  public void testIterEmptyVarargs() {
    Iterable<AutoCloseable> it = AutoCloseables.iter();
    List<AutoCloseable> list = new ArrayList<>();
    it.forEach(list::add);
    assertTrue(list.isEmpty());
  }

  @Test
  public void testIterWithNull() {
    AutoCloseables.iter((AutoCloseable) null); // no exception
  }

  @Test
  public void testCloseNoCheckedWithNull() {
    AutoCloseables.closeNoChecked(null); // no exception
  }

  @Test
  public void testCloseNoCheckedWrapsException() {
    Exception e = new Exception("close failed");
    TrackCloseable c = new TrackCloseable(e);
    RuntimeException re =
        assertThrows(RuntimeException.class, () -> AutoCloseables.closeNoChecked(c));
    assertSame(re.getCause(), e);
    assertTrue(re.getMessage().contains("close failed"));
  }

  @Test
  public void testNoop() throws Exception {
    AutoCloseable noop = AutoCloseables.noop();
    assertSame(noop, AutoCloseables.noop());
    noop.close(); // no exception
  }

  @Test
  public void testAllClosesCollectionOnClose() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    List<AutoCloseable> list = Arrays.asList(a, b);
    AutoCloseable all = AutoCloseables.all(list);
    assertFalse(a.isClosed());
    assertFalse(b.isClosed());
    all.close();
    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
  }

  @Test
  public void testAllWithNullCollection() throws Exception {
    AutoCloseable all = AutoCloseables.all(null);
    all.close(); // no exception
  }

  @Test
  public void testRollbackCloseableClosesWhenNotCommitted() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    AutoCloseables.RollbackCloseable rb = AutoCloseables.rollbackable(a, b);
    rb.close();
    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
  }

  @Test
  public void testRollbackCloseableDoesNotCloseWhenCommitted() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    AutoCloseables.RollbackCloseable rb = AutoCloseables.rollbackable(a, b);
    rb.commit();
    rb.close();
    assertFalse(a.isClosed());
    assertFalse(b.isClosed());
  }

  @Test
  public void testRollbackCloseableAddAndAddAll() throws Exception {
    TrackCloseable a = new TrackCloseable();
    TrackCloseable b = new TrackCloseable();
    TrackCloseable c = new TrackCloseable();
    TrackCloseable d = new TrackCloseable();
    AutoCloseables.RollbackCloseable rb = AutoCloseables.rollbackable(a);
    rb.add(b);
    rb.addAll(c, d);
    rb.addAll((AutoCloseable[]) null); // null varargs shouldn't fail
    rb.addAll((List<AutoCloseable>) null); // null Iterable shouldn't fail
    rb.close();
    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
    assertTrue(c.isClosed());
    assertTrue(d.isClosed());
  }

  @Test
  public void testRollbackCloseableWithNull() throws Exception {
    AutoCloseables.rollbackable((AutoCloseable) null); // no exception
  }

  @Test
  public void testRollbackCloseableWithNulls() throws Exception {
    TrackCloseable a = new TrackCloseable();
    AutoCloseables.RollbackCloseable rb = AutoCloseables.rollbackable(a, null);
    rb.close();
    assertTrue(a.isClosed());
  }
}
