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

import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * OAuth 2.0 Client Credentials flow token provider (RFC 6749 Section 4.4).
 *
 * <p>This provider handles service-to-service authentication where no user interaction is required.
 * Tokens are cached and automatically refreshed before expiration.
 */
public class ClientCredentialsTokenProvider implements OAuthTokenProvider {
  private static final int EXPIRATION_BUFFER_SECONDS = 30;
  private static final int DEFAULT_EXPIRATION_SECONDS = 3600;

  private final URI tokenUri;
  private final ClientSecretBasic clientAuth;
  private final @Nullable Scope scope;
  private final Object tokenLock = new Object();
  private volatile @Nullable TokenInfo cachedToken;

  /**
   * Creates a new ClientCredentialsTokenProvider.
   *
   * @param tokenUri the OAuth token endpoint URI
   * @param clientId the OAuth client ID
   * @param clientSecret the OAuth client secret
   * @param scope optional OAuth scopes (space-separated)
   */
  public ClientCredentialsTokenProvider(
      URI tokenUri, String clientId, String clientSecret, @Nullable String scope) {
    this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
    this.clientAuth = new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));
    this.scope = (scope != null && !scope.isEmpty()) ? Scope.parse(scope) : null;
  }

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
      refreshToken();
      return cachedToken.getAccessToken();
    }
  }

  private void refreshToken() throws SQLException {
    try {
      TokenRequest request =
          new TokenRequest(tokenUri, clientAuth, new ClientCredentialsGrant(), scope);

      TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

      if (!response.indicatesSuccess()) {
        TokenErrorResponse errorResponse = response.toErrorResponse();
        String errorMsg =
            String.format(
                "OAuth token request failed: %s - %s",
                errorResponse.getErrorObject().getCode(),
                errorResponse.getErrorObject().getDescription());
        throw new SQLException(errorMsg);
      }

      AccessToken accessToken = response.toSuccessResponse().getTokens().getAccessToken();
      long expiresIn =
          accessToken.getLifetime() > 0 ? accessToken.getLifetime() : DEFAULT_EXPIRATION_SECONDS;
      Instant expiresAt = Instant.now().plusSeconds(expiresIn);

      cachedToken = new TokenInfo(accessToken.getValue(), expiresAt);
    } catch (com.nimbusds.oauth2.sdk.ParseException e) {
      throw new SQLException("Failed to parse OAuth token response", e);
    } catch (IOException e) {
      throw new SQLException("Failed to send OAuth token request", e);
    }
  }
}
