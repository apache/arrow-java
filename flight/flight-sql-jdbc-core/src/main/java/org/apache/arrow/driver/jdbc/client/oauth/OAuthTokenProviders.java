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
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Unified factory for creating OAuth token providers.
 *
 * <p>This class provides a single entry point for creating all OAuth token providers with a
 * consistent builder API. It supports:
 *
 * <ul>
 *   <li>Client Credentials flow (RFC 6749 Section 4.4)
 *   <li>Token Exchange flow (RFC 8693)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Client Credentials flow
 * OAuthTokenProvider provider = OAuthTokenProviders.clientCredentials()
 *     .tokenUri("https://auth.example.com/token")
 *     .clientId("my-client")
 *     .clientSecret("my-secret")
 *     .scope("read write")
 *     .build();
 *
 * // Token Exchange flow
 * OAuthTokenProvider provider = OAuthTokenProviders.tokenExchange()
 *     .tokenUri("https://auth.example.com/token")
 *     .subjectToken("user-token")
 *     .subjectTokenType("urn:ietf:params:oauth:token-type:access_token")
 *     .build();
 * }</pre>
 */
public final class OAuthTokenProviders {

  private OAuthTokenProviders() {}

  /**
   * Creates a new builder for Client Credentials flow.
   *
   * @return a new ClientCredentialsBuilder instance
   */
  public static ClientCredentialsBuilder clientCredentials() {
    return new ClientCredentialsBuilder();
  }

  /**
   * Creates a new builder for Token Exchange flow.
   *
   * @return a new TokenExchangeBuilder instance
   */
  public static TokenExchangeBuilder tokenExchange() {
    return new TokenExchangeBuilder();
  }

  /** Builder for creating {@link ClientCredentialsTokenProvider} instances. */
  public static class ClientCredentialsBuilder {
    private @Nullable URI tokenUri;
    private @Nullable String clientId;
    private @Nullable String clientSecret;
    private @Nullable String scope;

    ClientCredentialsBuilder() {}

    /**
     * Sets the OAuth token endpoint URI (required).
     *
     * @param tokenUri the token endpoint URI
     * @return this builder
     */
    public ClientCredentialsBuilder tokenUri(URI tokenUri) {
      this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
      return this;
    }

    /**
     * Sets the OAuth token endpoint URI from a string (required).
     *
     * @param tokenUri the token endpoint URI string
     * @return this builder
     * @throws IllegalArgumentException if the URI is invalid
     */
    public ClientCredentialsBuilder tokenUri(String tokenUri) {
      Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
      try {
        this.tokenUri = new URI(tokenUri);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid token URI: " + tokenUri, e);
      }
      return this;
    }

    /**
     * Sets the OAuth client ID (required).
     *
     * @param clientId the client ID
     * @return this builder
     */
    public ClientCredentialsBuilder clientId(String clientId) {
      this.clientId = Objects.requireNonNull(clientId, "clientId cannot be null");
      return this;
    }

    /**
     * Sets the OAuth client secret (required).
     *
     * @param clientSecret the client secret
     * @return this builder
     */
    public ClientCredentialsBuilder clientSecret(String clientSecret) {
      this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
      return this;
    }

    /**
     * Sets the OAuth scopes (optional).
     *
     * @param scope the space-separated scope string
     * @return this builder
     */
    public ClientCredentialsBuilder scope(@Nullable String scope) {
      this.scope = scope;
      return this;
    }

    /**
     * Builds a new ClientCredentialsTokenProvider instance.
     *
     * @return the configured ClientCredentialsTokenProvider
     * @throws IllegalStateException if required parameters are missing
     */
    public ClientCredentialsTokenProvider build() {
      if (tokenUri == null) {
        throw new IllegalStateException("tokenUri is required");
      }
      if (clientId == null) {
        throw new IllegalStateException("clientId is required");
      }
      if (clientSecret == null) {
        throw new IllegalStateException("clientSecret is required");
      }
      return new ClientCredentialsTokenProvider(tokenUri, clientId, clientSecret, scope);
    }
  }

  /** Builder for creating {@link TokenExchangeTokenProvider} instances. */
  public static class TokenExchangeBuilder {
    private @Nullable URI tokenUri;
    private @Nullable String subjectToken;
    private @Nullable String subjectTokenType;
    private @Nullable String actorToken;
    private @Nullable String actorTokenType;
    private @Nullable String audience;
    private @Nullable String requestedTokenType;
    private @Nullable Scope scope;
    private @Nullable List<URI> resources;
    private @Nullable ClientAuthentication clientAuth;

    TokenExchangeBuilder() {}

    /**
     * Sets the OAuth token endpoint URI (required).
     *
     * @param tokenUri the token endpoint URI
     * @return this builder
     */
    public TokenExchangeBuilder tokenUri(URI tokenUri) {
      this.tokenUri = Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
      return this;
    }

    /**
     * Sets the OAuth token endpoint URI from a string (required).
     *
     * @param tokenUri the token endpoint URI string
     * @return this builder
     * @throws IllegalArgumentException if the URI is invalid
     */
    public TokenExchangeBuilder tokenUri(String tokenUri) {
      Objects.requireNonNull(tokenUri, "tokenUri cannot be null");
      try {
        this.tokenUri = new URI(tokenUri);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid token URI: " + tokenUri, e);
      }
      return this;
    }

    /**
     * Sets the subject token to exchange (required).
     *
     * @param subjectToken the subject token value
     * @return this builder
     */
    public TokenExchangeBuilder subjectToken(String subjectToken) {
      this.subjectToken = Objects.requireNonNull(subjectToken, "subjectToken cannot be null");
      return this;
    }

    /**
     * Sets the type of the subject token (required).
     *
     * @param subjectTokenType the subject token type URI
     * @return this builder
     */
    public TokenExchangeBuilder subjectTokenType(String subjectTokenType) {
      this.subjectTokenType =
          Objects.requireNonNull(subjectTokenType, "subjectTokenType cannot be null");
      return this;
    }

    /**
     * Sets the optional actor token for delegation scenarios.
     *
     * @param actorToken the actor token value
     * @return this builder
     */
    public TokenExchangeBuilder actorToken(@Nullable String actorToken) {
      this.actorToken = actorToken;
      return this;
    }

    /**
     * Sets the type of the actor token.
     *
     * @param actorTokenType the actor token type URI
     * @return this builder
     */
    public TokenExchangeBuilder actorTokenType(@Nullable String actorTokenType) {
      this.actorTokenType = actorTokenType;
      return this;
    }

    /**
     * Sets the target audience for the exchanged token.
     *
     * @param audience the target audience
     * @return this builder
     */
    public TokenExchangeBuilder audience(@Nullable String audience) {
      this.audience = audience;
      return this;
    }

    /**
     * Sets the requested token type for the exchanged token.
     *
     * @param requestedTokenType the requested token type URI
     * @return this builder
     */
    public TokenExchangeBuilder requestedTokenType(@Nullable String requestedTokenType) {
      this.requestedTokenType = requestedTokenType;
      return this;
    }

    /**
     * Sets the OAuth scopes for the token request.
     *
     * @param scope the OAuth scope object
     * @return this builder
     */
    public TokenExchangeBuilder scope(@Nullable Scope scope) {
      this.scope = scope;
      return this;
    }

    /**
     * Sets the OAuth scopes from a space-separated string.
     *
     * @param scope the space-separated scope string
     * @return this builder
     */
    public TokenExchangeBuilder scope(@Nullable String scope) {
      this.scope = (scope != null && !scope.isEmpty()) ? Scope.parse(scope) : null;
      return this;
    }

    /**
     * Sets the target resource URIs (RFC 8707).
     *
     * @param resources the list of resource URIs
     * @return this builder
     */
    public TokenExchangeBuilder resources(@Nullable List<URI> resources) {
      this.resources = resources;
      return this;
    }

    /**
     * Sets a single target resource URI (RFC 8707).
     *
     * @param resource the resource URI
     * @return this builder
     */
    public TokenExchangeBuilder resource(@Nullable URI resource) {
      this.resources = resource != null ? Collections.singletonList(resource) : null;
      return this;
    }

    /**
     * Sets a single target resource URI from a string (RFC 8707).
     *
     * @param resource the resource URI string
     * @return this builder
     */
    public TokenExchangeBuilder resource(@Nullable String resource) {
      if (resource != null && !resource.isEmpty()) {
        this.resources = Collections.singletonList(URI.create(resource));
      } else {
        this.resources = null;
      }
      return this;
    }

    /**
     * Sets the client authentication.
     *
     * @param clientAuth the client authentication object
     * @return this builder
     */
    public TokenExchangeBuilder clientAuthentication(@Nullable ClientAuthentication clientAuth) {
      this.clientAuth = clientAuth;
      return this;
    }

    /**
     * Sets client authentication using client ID and secret.
     *
     * @param clientId the client ID
     * @param clientSecret the client secret
     * @return this builder
     */
    public TokenExchangeBuilder clientCredentials(String clientId, String clientSecret) {
      Objects.requireNonNull(clientId, "clientId cannot be null");
      Objects.requireNonNull(clientSecret, "clientSecret cannot be null");
      this.clientAuth = new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));
      return this;
    }

    /**
     * Builds a new TokenExchangeTokenProvider instance.
     *
     * @return the configured TokenExchangeTokenProvider
     * @throws IllegalStateException if required parameters are missing
     */
    public TokenExchangeTokenProvider build() {
      if (tokenUri == null) {
        throw new IllegalStateException("tokenUri is required");
      }
      if (subjectToken == null) {
        throw new IllegalStateException("subjectToken is required");
      }
      if (subjectTokenType == null) {
        throw new IllegalStateException("subjectTokenType is required");
      }

      TokenExchangeGrant grant = createGrant();
      return new TokenExchangeTokenProvider(tokenUri, grant, clientAuth, scope, resources);
    }

    private TokenExchangeGrant createGrant() {
      try {
        TypelessAccessToken subjectAccessToken = new TypelessAccessToken(subjectToken);
        TokenTypeURI subjectTypeUri = TokenTypeURI.parse(subjectTokenType);

        TypelessAccessToken actorAccessToken =
            actorToken != null ? new TypelessAccessToken(actorToken) : null;
        TokenTypeURI actorTypeUri =
            actorTokenType != null ? TokenTypeURI.parse(actorTokenType) : null;
        TokenTypeURI requestedTypeUri =
            requestedTokenType != null ? TokenTypeURI.parse(requestedTokenType) : null;
        List<Audience> audienceList =
            audience != null ? Collections.singletonList(new Audience(audience)) : null;

        return new TokenExchangeGrant(
            subjectAccessToken,
            subjectTypeUri,
            actorAccessToken,
            actorTypeUri,
            requestedTypeUri,
            audienceList);
      } catch (ParseException e) {
        throw new IllegalStateException("Failed to create TokenExchangeGrant", e);
      }
    }
  }
}
