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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import org.apache.arrow.driver.jdbc.client.CloseableEndpointStreamPair;
import org.apache.arrow.driver.jdbc.utils.FlightEndpointDataQueue;
import org.junit.jupiter.api.Test;

public class ArrowFlightJdbcFlightStreamResultSetTest {

  @Test
  public void testClosedQueueDuringCollectionHandoffClosesUnacceptedEndpoints() throws Exception {
    final FlightEndpointDataQueue queue = mock(FlightEndpointDataQueue.class);
    final CloseableEndpointStreamPair acceptedEndpoint = mock(CloseableEndpointStreamPair.class);
    final CloseableEndpointStreamPair rejectedEndpoint = mock(CloseableEndpointStreamPair.class);
    final CloseableEndpointStreamPair unacceptedEndpoint = mock(CloseableEndpointStreamPair.class);
    final Exception closeFailure = new Exception("Failed to close rejected endpoint");
    doThrow(new IllegalStateException("FlightEndpointDataQueue closed"))
        .when(queue)
        .enqueue(rejectedEndpoint);
    when(queue.isClosed()).thenReturn(true);
    doThrow(closeFailure).when(rejectedEndpoint).close();

    final SQLException exception =
        assertThrows(
            SQLException.class,
            () ->
                ArrowFlightJdbcFlightStreamResultSet.enqueueEndpointData(
                    queue, List.of(acceptedEndpoint, rejectedEndpoint, unacceptedEndpoint)));

    assertEquals("Statement canceled", exception.getMessage());
    assertEquals(1, exception.getSuppressed().length);
    assertSame(closeFailure, exception.getSuppressed()[0]);
    verify(acceptedEndpoint, never()).close();
    verify(rejectedEndpoint).close();
    verify(unacceptedEndpoint).close();
    verify(queue).enqueue(acceptedEndpoint);
    verify(queue).enqueue(rejectedEndpoint);
    verify(queue, never()).enqueue(unacceptedEndpoint);
  }
}
