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

import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import org.apache.arrow.util.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract base class for OAuth token providers that handles token caching, refresh logic, and
 * common request/response handling.
 */
public abstract class AbstractOAuthTokenProvider implements OAuthTokenProvider {
  protected static final int EXPIRATION_BUFFER_SECONDS = 30;
  protected static final int DEFAULT_EXPIRATION_SECONDS = 3600;

  private final Object tokenLock = new Object();
  private volatile @Nullable TokenInfo cachedToken;

  @VisibleForTesting
  URI tokenUri;

  @VisibleForTesting
  @Nullable ClientAuthentication clientAuth;

  @VisibleForTesting
  @Nullable Scope scope;

  @Override
  public String getValidToken() throws SQLException {
    TokenInfo token = cachedToken;
    if (token != null && !token.isExpired(EXPIRATION_BUFFER_SECONDS)) {
      return token.getAccessToken();
    }

    synchronized (tokenLock) {
      token = cachedToken;
      if (token != null && !token.isExpired(EXPIRATION_BUFFER_SECONDS)) {
        return token.getAccessToken();
      }
      cachedToken = fetchNewToken();
      return cachedToken.getAccessToken();
    }
  }

  /**
   * Fetches a new token from the authorization server. This method handles the common
   * request/response logic while delegating flow-specific request building to subclasses.
   *
   * @return the new token information
   * @throws SQLException if token cannot be obtained
   */
  protected TokenInfo fetchNewToken() throws SQLException {
    try {
      TokenRequest request = buildTokenRequest();
      TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

      if (!response.indicatesSuccess()) {
        TokenErrorResponse errorResponse = response.toErrorResponse();
        String errorMsg =
            String.format(
                "OAuth request failed: %s - %s",
                errorResponse.getErrorObject().getCode(),
                errorResponse.getErrorObject().getDescription());
        throw new SQLException(errorMsg);
      }

      AccessToken accessToken = response.toSuccessResponse().getTokens().getAccessToken();
      long expiresIn =
          accessToken.getLifetime() > 0 ? accessToken.getLifetime() : DEFAULT_EXPIRATION_SECONDS;
      Instant expiresAt = Instant.now().plusSeconds(expiresIn);

      return new TokenInfo(accessToken.getValue(), expiresAt);
    } catch (ParseException e) {
      throw new SQLException("Failed to parse OAuth token response", e);
    } catch (IOException e) {
      throw new SQLException("Failed to send OAuth token request", e);
    }
  }

  /**
   * Builds the flow-specific token request.
   *
   * @return the token request to send to the authorization server
   */
  protected abstract TokenRequest buildTokenRequest();
}
