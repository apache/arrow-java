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
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.Token;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * OAuth 2.0 Token Exchange flow token provider (RFC 8693).
 *
 * <p>This provider exchanges one token for another, commonly used for federated authentication,
 * delegation, or impersonation scenarios. Tokens are cached and automatically refreshed.
 */
public class TokenExchangeTokenProvider extends AbstractOAuthTokenProvider {

  private final URI tokenUri;
  private final Token subjectToken;
  private final TokenTypeURI subjectTokenType;
  private final @Nullable Token actorToken;
  private final @Nullable TokenTypeURI actorTokenType;
  private final @Nullable List<Audience> audiences;
  private final @Nullable List<URI> resources;
  private final @Nullable TokenTypeURI requestedTokenType;
  private final @Nullable Scope scope;
  private final @Nullable ClientAuthentication clientAuth;

  /**
   * Creates a new TokenExchangeTokenProvider.
   *
   * @param tokenUri the OAuth token endpoint URI
   * @param subjectToken the subject token to exchange
   * @param subjectTokenType the type of the subject token (defaults to access_token if null)
   * @param actorToken optional actor token for delegation
   * @param actorTokenType the type of the actor token
   * @param audience optional target audience
   * @param resource optional target resource URI
   * @param requestedTokenType optional requested token type
   * @param scope optional OAuth scopes
   * @param clientId optional client ID for confidential clients
   * @param clientSecret optional client secret for confidential clients
   */
  public TokenExchangeTokenProvider(
      URI tokenUri,
      String subjectToken,
      String subjectTokenType,
      @Nullable String actorToken,
      @Nullable String actorTokenType,
      @Nullable String audience,
      @Nullable String resource,
      @Nullable String requestedTokenType,
      @Nullable String scope,
      @Nullable String clientId,
      @Nullable String clientSecret) {
    this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
    Objects.requireNonNull(subjectToken, "subjectToken cannot be null");
    this.subjectToken = new TypelessAccessToken(subjectToken);
    Objects.requireNonNull(subjectTokenType, "subjectTokenType cannot be null");
    this.subjectTokenType = parseTokenType(subjectTokenType);
    this.actorToken = actorToken != null ? new TypelessAccessToken(actorToken) : null;
    this.actorTokenType = actorTokenType != null ? parseTokenType(actorTokenType) : null;
    this.audiences = audience != null ? Collections.singletonList(new Audience(audience)) : null;
    this.resources = resource != null ? Collections.singletonList(URI.create(resource)) : null;
    this.requestedTokenType =
        requestedTokenType != null ? parseTokenType(requestedTokenType) : null;
    this.scope = (scope != null && !scope.isEmpty()) ? Scope.parse(scope) : null;

    if (clientId != null && clientSecret != null) {
      this.clientAuth = new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));
    } else {
      this.clientAuth = null;
    }
  }

  private static TokenTypeURI parseTokenType(String tokenType) {
    try {
      return TokenTypeURI.parse(tokenType);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Invalid token type URI: " + tokenType, e);
    }
  }

  @Override
  protected TokenRequest buildTokenRequest() {
    TokenExchangeGrant grant =
        new TokenExchangeGrant(
            subjectToken,
            subjectTokenType,
            actorToken,
            actorTokenType,
            requestedTokenType,
            audiences);

    if (clientAuth != null) {
      return new TokenRequest(tokenUri, clientAuth, grant, scope, resources, null);
    } else if (resources != null) {
      return new TokenRequest(tokenUri, null, grant, scope, resources, null, null);
    } else {
      return new TokenRequest(tokenUri, grant, scope);
    }
  }
}
