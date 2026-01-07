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

import com.nimbusds.oauth2.sdk.GrantType;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration class for OAuth settings parsed from connection properties. */
public class OAuthConfiguration {

  private final GrantType grantType;
  private final URI tokenUri;
  private final @Nullable String clientId;
  private final @Nullable String clientSecret;
  private final @Nullable String scope;
  private final @Nullable String subjectToken;
  private final @Nullable String subjectTokenType;
  private final @Nullable String actorToken;
  private final @Nullable String actorTokenType;
  private final @Nullable String audience;
  private final @Nullable String resource;
  private final @Nullable String requestedTokenType;
  private final @Nullable String exchangeScope;

  private OAuthConfiguration(Builder builder) throws SQLException {
    this.grantType = builder.grantType;
    this.tokenUri = builder.tokenUri;
    this.clientId = builder.clientId;
    this.clientSecret = builder.clientSecret;
    this.scope = builder.scope;
    this.subjectToken = builder.subjectToken;
    this.subjectTokenType = builder.subjectTokenType;
    this.actorToken = builder.actorToken;
    this.actorTokenType = builder.actorTokenType;
    this.audience = builder.audience;
    this.resource = builder.resource;
    this.requestedTokenType = builder.requestedTokenType;
    this.exchangeScope = builder.exchangeScope;

    validate();
  }

  private void validate() throws SQLException {
    Objects.requireNonNull(grantType, "OAuth grant type is required");
    Objects.requireNonNull(tokenUri, "Token URI is required");

    if (GrantType.CLIENT_CREDENTIALS.equals(grantType)) {
      if (clientId == null || clientId.isEmpty()) {
        throw new SQLException("clientId is required for client_credentials flow");
      }
      if (clientSecret == null || clientSecret.isEmpty()) {
        throw new SQLException("clientSecret is required for client_credentials flow");
      }
    } else if (GrantType.TOKEN_EXCHANGE.equals(grantType)) {
      if (subjectToken == null || subjectToken.isEmpty()) {
        throw new SQLException("subjectToken is required for token_exchange flow");
      }
      if (subjectTokenType == null || subjectTokenType.isEmpty()) {
        throw new SQLException("subjectTokenType is required for token_exchange flow");
      }
    } else {
      throw new SQLException("Unsupported OAuth grant type: " + grantType);
    }
  }

  /**
   * Creates an OAuthTokenProvider based on the configured grant type.
   *
   * @return the token provider
   * @throws SQLException if the grant type is not supported or configuration is invalid
   */
  public OAuthTokenProvider createTokenProvider() throws SQLException {
    if (GrantType.CLIENT_CREDENTIALS.equals(grantType)) {
      return new ClientCredentialsTokenProvider(tokenUri, clientId, clientSecret, scope);
    } else if (GrantType.TOKEN_EXCHANGE.equals(grantType)) {
      return new TokenExchangeTokenProvider(
          tokenUri,
          subjectToken,
          subjectTokenType,
          actorToken,
          actorTokenType,
          audience,
          resource,
          requestedTokenType,
          exchangeScope,
          clientId,
          clientSecret);
    } else {
      throw new SQLException("Unsupported OAuth grant type: " + grantType);
    }
  }

  public GrantType getGrantType() {
    return grantType;
  }

  public URI getTokenUri() {
    return tokenUri;
  }

  public @Nullable String getClientId() {
    return clientId;
  }

  public @Nullable String getClientSecret() {
    return clientSecret;
  }

  public @Nullable String getScope() {
    return scope;
  }

  public @Nullable String getSubjectToken() {
    return subjectToken;
  }

  public @Nullable String getSubjectTokenType() {
    return subjectTokenType;
  }

  public @Nullable String getActorToken() {
    return actorToken;
  }

  public @Nullable String getActorTokenType() {
    return actorTokenType;
  }

  public @Nullable String getAudience() {
    return audience;
  }

  public @Nullable String getResource() {
    return resource;
  }

  public @Nullable String getRequestedTokenType() {
    return requestedTokenType;
  }

  public @Nullable String getExchangeScope() {
    return exchangeScope;
  }

  /** Builder for OAuthConfiguration. */
  public static class Builder {
    private GrantType grantType;
    private URI tokenUri;
    private @Nullable String clientId;
    private @Nullable String clientSecret;
    private @Nullable String scope;
    private @Nullable String subjectToken;
    private @Nullable String subjectTokenType;
    private @Nullable String actorToken;
    private @Nullable String actorTokenType;
    private @Nullable String audience;
    private @Nullable String resource;
    private @Nullable String requestedTokenType;
    private @Nullable String exchangeScope;

    /**
     * Sets the OAuth grant type from a string value.
     *
     * @param flowStr the flow type string (e.g., "client_credentials", "token_exchange")
     * @return this builder
     * @throws SQLException if the flow string is invalid
     */
    public Builder flow(String flowStr) throws SQLException {
      if (flowStr == null || flowStr.isEmpty()) {
        throw new SQLException("OAuth flow cannot be null or empty");
      }
      String normalized = flowStr.toLowerCase(Locale.ROOT);
      switch (normalized) {
        case "client_credentials":
          this.grantType = GrantType.CLIENT_CREDENTIALS;
          break;
        case "token_exchange":
          this.grantType = GrantType.TOKEN_EXCHANGE;
          break;
        default:
          throw new SQLException("Unknown OAuth flow: " + flowStr);
      }
      return this;
    }

    /**
     * Sets the OAuth grant type.
     *
     * @param grantType the OAuth grant type
     * @return this builder
     */
    public Builder grantType(GrantType grantType) {
      this.grantType = grantType;
      return this;
    }

    /**
     * Sets the token URI.
     *
     * @param tokenUri the OAuth token endpoint URI
     * @return this builder
     * @throws SQLException if the URI is invalid
     */
    public Builder tokenUri(String tokenUri) throws SQLException {
      if (tokenUri == null || tokenUri.isEmpty()) {
        throw new SQLException("Token URI cannot be null or empty");
      }
      try {
        this.tokenUri = new URI(tokenUri);
      } catch (URISyntaxException e) {
        throw new SQLException("Invalid token URI: " + tokenUri, e);
      }
      return this;
    }

    public Builder clientId(@Nullable String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder clientSecret(@Nullable String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public Builder scope(@Nullable String scope) {
      this.scope = scope;
      return this;
    }

    public Builder subjectToken(@Nullable String subjectToken) {
      this.subjectToken = subjectToken;
      return this;
    }

    public Builder subjectTokenType(@Nullable String subjectTokenType) {
      this.subjectTokenType = subjectTokenType;
      return this;
    }

    public Builder actorToken(@Nullable String actorToken) {
      this.actorToken = actorToken;
      return this;
    }

    public Builder actorTokenType(@Nullable String actorTokenType) {
      this.actorTokenType = actorTokenType;
      return this;
    }

    public Builder audience(@Nullable String audience) {
      this.audience = audience;
      return this;
    }

    public Builder resource(@Nullable String resource) {
      this.resource = resource;
      return this;
    }

    public Builder requestedTokenType(@Nullable String requestedTokenType) {
      this.requestedTokenType = requestedTokenType;
      return this;
    }

    public Builder exchangeScope(@Nullable String exchangeScope) {
      this.exchangeScope = exchangeScope;
      return this;
    }

    public OAuthConfiguration build() throws SQLException {
      return new OAuthConfiguration(this);
    }
  }
}
