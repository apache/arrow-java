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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import org.apache.arrow.util.VisibleForTesting;

/**
 * A cache for Flight clients.
 *
 * <p>The intent is to avoid constantly recreating clients to the same locations. gRPC can multiplex
 * multiple requests over a single TCP connection, and a cache would let us take advantage of that.
 *
 * <p>At the time being it only tracks whether a location is reachable or not. To actually cache
 * clients, we would need a way to incorporate other connection parameters (authentication, etc.)
 * into the cache key.
 */
public final class FlightClientCache {
  @VisibleForTesting Cache<String, ClientCacheEntry> clientCache;

  public FlightClientCache() {
    this.clientCache = Caffeine.newBuilder().expireAfterWrite(Duration.ofSeconds(600)).build();
  }

  public boolean isDud(String key) {
    return clientCache.getIfPresent(key) != null;
  }

  public void markLocationAsDud(String key) {
    clientCache.put(key, new ClientCacheEntry());
  }

  public void markLocationAsReachable(String key) {
    clientCache.invalidate(key);
  }

  /** A cache entry (empty because we only track reachability, see outer class docstring). */
  public static final class ClientCacheEntry {}
}
