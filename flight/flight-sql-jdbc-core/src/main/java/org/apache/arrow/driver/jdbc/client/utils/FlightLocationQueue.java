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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.flight.Location;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A queue of Flight locations to connect to for an endpoint.
 *
 * <p>This helper class is intended to encapsulate the retry logic in a testable manner.
 */
public final class FlightLocationQueue implements Iterator<Location> {
  private final Deque<Location> locations;
  private final Deque<Location> badLocations;

  /**
   * Create a new queue.
   *
   * @param flightClientCache An optional cache used to sort previously unreachable locations to the
   *     end.
   * @param locations The locations to try.
   */
  public FlightLocationQueue(
      @Nullable FlightClientCache flightClientCache, List<Location> locations) {
    this.locations = new ArrayDeque<>();
    this.badLocations = new ArrayDeque<>();

    for (Location location : locations) {
      if (flightClientCache != null && flightClientCache.isDud(location.toString())) {
        this.badLocations.add(location);
      } else {
        this.locations.add(location);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !locations.isEmpty() || !badLocations.isEmpty();
  }

  @Override
  public Location next() {
    if (!locations.isEmpty()) {
      return locations.pop();
    } else if (!badLocations.isEmpty()) {
      return badLocations.pop();
    }
    throw new NoSuchElementException();
  }
}
