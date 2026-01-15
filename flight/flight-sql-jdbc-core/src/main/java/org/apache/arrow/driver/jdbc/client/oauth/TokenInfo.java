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
package org.apache.arrow.driver.jdbc.client.oauth;

import java.time.Instant;
import java.util.Objects;

/** Holds OAuth token information including the access token and expiration time. */
public class TokenInfo {
  private final String accessToken;
  private final Instant expiresAt;

  public TokenInfo(String accessToken, Instant expiresAt) {
    this.accessToken = Objects.requireNonNull(accessToken, "accessToken cannot be null");
    this.expiresAt = Objects.requireNonNull(expiresAt, "expiresAt cannot be null");
  }

  public String getAccessToken() {
    return accessToken;
  }

  /**
   * Checks if the token is expired or will expire within the buffer period.
   *
   * @param bufferSeconds seconds before actual expiration to consider token expired
   * @return true if token should be refreshed
   */
  public boolean isExpired(int bufferSeconds) {
    return Instant.now().plusSeconds(bufferSeconds).isAfter(expiresAt);
  }
}
