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

import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import org.apache.arrow.util.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * OAuth 2.0 Token Exchange flow token provider (RFC 8693).
 *
 * <p>This provider exchanges one token for another, commonly used for federated authentication,
 * delegation, or impersonation scenarios. Tokens are cached and automatically refreshed.
 */
public class TokenExchangeTokenProvider extends AbstractOAuthTokenProvider {

  @VisibleForTesting TokenExchangeGrant grant;

  @VisibleForTesting @Nullable List<URI> resources;

  /**
   * Creates a new TokenExchangeTokenProvider with full configuration.
   *
   * @param tokenUri the OAuth token endpoint URI
   * @param grant the token exchange grant containing subject/actor token information
   * @param clientAuth optional client authentication
   * @param scope optional OAuth scopes
   * @param resource optional target resource URI (RFC 8707)
   */
  TokenExchangeTokenProvider(
      URI tokenUri,
      TokenExchangeGrant grant,
      @Nullable ClientAuthentication clientAuth,
      @Nullable Scope scope,
      @Nullable List<URI> resource) {
    this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
    this.grant = Objects.requireNonNull(grant, "grant cannot be null");
    this.scope = scope;
    this.resources = resource;
    this.clientAuth = clientAuth;
  }

  @Override
  protected TokenRequest buildTokenRequest() {
    TokenRequest.Builder builder;
    if (clientAuth != null) {
      builder = new TokenRequest.Builder(tokenUri, clientAuth, grant);
    } else {
      builder = new TokenRequest.Builder(tokenUri, grant);
    }

    if (scope != null) {
      builder.scope(scope);
    }
    if (resources != null) {
      builder.resources(resources.toArray(new URI[0]));
    }

    return builder.build();
  }
}
