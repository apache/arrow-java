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
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  @VisibleForTesting
  TokenExchangeGrant grant;

  @VisibleForTesting
  @Nullable List<URI> resources;
  /**
   * Creates a new TokenExchangeTokenProvider with a pre-built TokenExchangeGrant.
   *
   * @param tokenUri the OAuth token endpoint URI
   * @param grant the token exchange grant containing subject/actor token information
   */
  public TokenExchangeTokenProvider(URI tokenUri, TokenExchangeGrant grant) {
    this(tokenUri, grant, null, null, null);
  }

  /**
   * Creates a new TokenExchangeTokenProvider with full configuration.
   *
   * @param tokenUri the OAuth token endpoint URI
   * @param grant the token exchange grant containing subject/actor token information
   * @param clientAuth optional client authentication
   * @param scope optional OAuth scopes
   * @param resource optional target resource URI (RFC 8707)
   */
  public TokenExchangeTokenProvider(
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

  private static TokenExchangeGrant createGrant(Map<String, List<String>> params)
      throws ParseException {
    return TokenExchangeGrant.parse(params);
  }

  /**
   * Returns a new Builder for creating TokenExchangeTokenProvider instances.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
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

  /** Builder for creating {@link TokenExchangeTokenProvider} instances. */
  public static class Builder {
    private static final String KEY_TOKEN_URI = "tokenUri";
    private static final String KEY_SUBJECT_TOKEN = "subject_token";
    private static final String KEY_SUBJECT_TOKEN_TYPE = "subject_token_type";
    private static final String KEY_ACTOR_TOKEN = "actor_token";
    private static final String KEY_ACTOR_TOKEN_TYPE = "actor_token_type";
    private static final String KEY_AUDIENCE = "audience";
    private static final String KEY_REQUESTED_TOKEN_TYPE = "requested_token_type";

    private final java.util.Map<String, List<String>> params = new java.util.HashMap<>();

    Builder() {}

    private @Nullable URI tokenUriValue;
    private @Nullable Scope scopeValue;
    private @Nullable List<URI> resourcesValue;
    private @Nullable ClientAuthentication clientAuthValue;

    /**
     * Sets the OAuth token endpoint URI (required).
     *
     * @param tokenUri the token endpoint URI
     * @return this builder
     */
    public Builder tokenUri(URI tokenUri) {
      this.tokenUriValue = tokenUri;
      return this;
    }

    /**
     * Sets the subject token to exchange (required).
     *
     * @param subjectToken the subject token value
     * @return this builder
     */
    public Builder subjectToken(String subjectToken) {
      params.put(KEY_SUBJECT_TOKEN, Collections.singletonList(subjectToken));
      return this;
    }

    /**
     * Sets the type of the subject token (required).
     *
     * @param subjectTokenType the subject token type URI
     * @return this builder
     */
    public Builder subjectTokenType(String subjectTokenType) {
      params.put(KEY_SUBJECT_TOKEN_TYPE, Collections.singletonList(subjectTokenType));
      return this;
    }

    /**
     * Sets the optional actor token for delegation scenarios.
     *
     * @param actorToken the actor token value
     * @return this builder
     */
    public Builder actorToken(String actorToken) {
      params.put(KEY_ACTOR_TOKEN, Collections.singletonList(actorToken));
      return this;
    }

    /**
     * Sets the type of the actor token.
     *
     * @param actorTokenType the actor token type URI
     * @return this builder
     */
    public Builder actorTokenType(String actorTokenType) {
      params.put(KEY_ACTOR_TOKEN_TYPE, Collections.singletonList(actorTokenType));
      return this;
    }

    /**
     * Sets the target audience for the exchanged token.
     *
     * @param audience the target audience
     * @return this builder
     */
    public Builder audience(String audience) {
      params.put(KEY_AUDIENCE, Collections.singletonList(audience));
      return this;
    }

    /**
     * Sets the requested token type for the exchanged token.
     *
     * @param requestedTokenType the requested token type URI
     * @return this builder
     */
    public Builder requestedTokenType(String requestedTokenType) {
      params.put(KEY_REQUESTED_TOKEN_TYPE, Collections.singletonList(requestedTokenType));
      return this;
    }

    /**
     * Sets the OAuth scopes for the token request.
     *
     * @param scope the OAuth scope object
     * @return this builder
     */
    public Builder scope(Scope scope) {
      this.scopeValue = scope;
      return this;
    }

    /**
     * Sets the target resource URIs (RFC 8707).
     *
     * @param resources the list of resource URIs
     * @return this builder
     */
    public Builder resources(List<URI> resources) {
      this.resourcesValue = resources;
      return this;
    }

    /**
     * Sets the client authentication.
     *
     * @param clientAuth the client authentication object
     * @return this builder
     */
    public Builder clientAuthentication(ClientAuthentication clientAuth) {
      this.clientAuthValue = clientAuth;
      return this;
    }

    /**
     * Builds a new TokenExchangeTokenProvider instance.
     *
     * @return the configured TokenExchangeTokenProvider
     * @throws IllegalStateException if required parameters are missing
     */
    public TokenExchangeTokenProvider build() {
      if (tokenUriValue == null) {
        throw new IllegalStateException(KEY_TOKEN_URI + " is required");
      }
      if (!params.containsKey(KEY_SUBJECT_TOKEN)) {
        throw new IllegalStateException(KEY_SUBJECT_TOKEN + " is required");
      }
      if (!params.containsKey(KEY_SUBJECT_TOKEN_TYPE)) {
        throw new IllegalStateException(KEY_SUBJECT_TOKEN_TYPE + " is required");
      }

      // Add grant_type for TokenExchangeGrant.parse()
      params.put(
          "grant_type",
          Collections.singletonList(com.nimbusds.oauth2.sdk.GrantType.TOKEN_EXCHANGE.getValue()));

      TokenExchangeGrant grant;
      try {
        grant = createGrant(params);
      } catch (ParseException e) {
        throw new IllegalStateException("Failed to create TokenExchangeGrant", e);
      }

      return new TokenExchangeTokenProvider(
          tokenUriValue, grant, clientAuthValue, scopeValue, resourcesValue);
    }
  }
}
