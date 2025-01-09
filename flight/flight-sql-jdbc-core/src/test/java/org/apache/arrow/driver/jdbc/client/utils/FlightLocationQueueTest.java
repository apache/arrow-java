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
package org.apache.arrow.driver.jdbc.client.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.Test;

class FlightLocationQueueTest {
  @Test
  void basicOperation() {
    Location location1 = Location.forGrpcInsecure("localhost", 8080);
    Location location2 = Location.forGrpcInsecure("localhost", 8081);
    FlightLocationQueue queue = new FlightLocationQueue(null, List.of(location1, location2));
    assertTrue(queue.hasNext());
    assertEquals(location1, queue.next());
    assertTrue(queue.hasNext());
    assertEquals(location2, queue.next());
    assertFalse(queue.hasNext());
  }

  @Test
  void badAfterGood() {
    Location location1 = Location.forGrpcInsecure("localhost", 8080);
    Location location2 = Location.forGrpcInsecure("localhost", 8081);
    FlightClientCache cache = new FlightClientCache();
    cache.markLocationAsDud(location1.toString());
    FlightLocationQueue queue = new FlightLocationQueue(cache, List.of(location1, location2));
    assertTrue(queue.hasNext());
    assertEquals(location2, queue.next());
    assertTrue(queue.hasNext());
    assertEquals(location1, queue.next());
    assertFalse(queue.hasNext());
  }

  @Test
  void iteratorInvariants() {
    FlightLocationQueue empty = new FlightLocationQueue(null, Collections.emptyList());
    assertFalse(empty.hasNext());
    assertThrows(NoSuchElementException.class, empty::next);
  }
}
