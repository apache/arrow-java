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
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * OAuth 2.0 Client Credentials flow token provider (RFC 6749 Section 4.4).
 *
 * <p>This provider handles service-to-service authentication where no user interaction is required.
 * Tokens are cached and automatically refreshed before expiration.
 */
public class ClientCredentialsTokenProvider extends AbstractOAuthTokenProvider {

  /**
   * Creates a new ClientCredentialsTokenProvider.
   *
   * @param tokenUri the OAuth token endpoint URI
   * @param clientId the OAuth client ID
   * @param clientSecret the OAuth client secret
   * @param scope optional OAuth scopes (space-separated)
   */
  ClientCredentialsTokenProvider(
      URI tokenUri, String clientId, String clientSecret, @Nullable String scope) {
    this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
    this.clientAuth = new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));
    this.scope = (scope != null && !scope.isEmpty()) ? Scope.parse(scope) : null;
  }

  @Override
  protected TokenRequest buildTokenRequest() {
    return new TokenRequest(tokenUri, clientAuth, new ClientCredentialsGrant(), scope);
  }
}
