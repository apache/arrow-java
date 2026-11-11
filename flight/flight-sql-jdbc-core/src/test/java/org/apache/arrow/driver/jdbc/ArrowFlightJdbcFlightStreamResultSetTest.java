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
package org.apache.arrow.driver.jdbc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.SQLException;
import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue;
import org.apache.arrow.flight.FlightStream;
import org.junit.jupiter.api.Test;

public class ArrowFlightJdbcFlightStreamResultSetTest {

  @Test
  public void testClosedQueueDuringCollectionHandoffClosesUnacceptedEndpoints() throws Exception {
    final FlightStream acceptedStream = mock(FlightStream.class);
    final FlightStream rejectedStream = mock(FlightStream.class);
    final FlightStream unacceptedStream = mock(FlightStream.class);
    final List<CloseableEndpointStreamPair> endpoints =
        List.of(
            new CloseableEndpointStreamPair(acceptedStream, null),
            new CloseableEndpointStreamPair(rejectedStream, null),
            new CloseableEndpointStreamPair(unacceptedStream, null));
    final Exception closeFailure = new Exception("Failed to close rejected endpoint");
    doThrow(closeFailure).when(rejectedStream).close();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try (final FlightEndpointDataQueue queue = FlightEndpointDataQueue.createNewQueue(executor)) {
      final List<CloseableEndpointStreamPair> closingEndpoints =
          new AbstractList<>() {
            @Override
            public CloseableEndpointStreamPair get(final int index) {
              if (index == 1) {
                // Close between handing off the first and second endpoint.
                assertDoesNotThrow(queue::close);
              }
              return endpoints.get(index);
            }

            @Override
            public int size() {
              return endpoints.size();
            }
          };
      final SQLException exception =
          assertThrows(
              SQLException.class,
              () ->
                  ArrowFlightJdbcFlightStreamResultSet.enqueueEndpointData(
                      queue, closingEndpoints));

      assertEquals("Statement canceled", exception.getMessage());
      assertEquals(1, exception.getSuppressed().length);
      assertSame(closeFailure, exception.getSuppressed()[0]);
      verify(acceptedStream).close();
      verify(rejectedStream).close();
      verify(unacceptedStream).close();
    } finally {
      executor.shutdownNow();
    }
  }
}
