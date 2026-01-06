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

import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;

/** Writes OAuth bearer tokens to Flight call headers. */
public class OAuthCredentialWriter implements Consumer<CallHeaders> {
  private final OAuthTokenProvider tokenProvider;

  public OAuthCredentialWriter(OAuthTokenProvider tokenProvider) {
    this.tokenProvider = Objects.requireNonNull(tokenProvider, "tokenProvider cannot be null");
  }

  @Override
  public void accept(CallHeaders headers) {
    try {
      String token = tokenProvider.getValidToken();
      headers.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + token);
    } catch (SQLException e) {
      throw new OAuthTokenException("Failed to obtain OAuth token", e);
    }
  }
}
