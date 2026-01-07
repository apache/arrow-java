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
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
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
      TokenExchangeTokenProvider.Builder builder =
          new TokenExchangeTokenProvider.Builder()
              .tokenUri(tokenUri)
              .subjectToken(subjectToken)
              .subjectTokenType(subjectTokenType);

      if (actorToken != null) {
        builder.actorToken(actorToken);
      }
      if (actorTokenType != null) {
        builder.actorTokenType(actorTokenType);
      }
      if (audience != null) {
        builder.audience(audience);
      }
      if (requestedTokenType != null) {
        builder.requestedTokenType(requestedTokenType);
      }

      Scope scopeObj = createScope(scope);
      if (scopeObj != null) {
        builder.scope(scopeObj);
      }

      List<URI> resources = createResources(resource);
      if (resources != null) {
        builder.resources(resources);
      }

      ClientAuthentication clientAuth = createClientAuthentication();
      if (clientAuth != null) {
        builder.clientAuthentication(clientAuth);
      }

      return builder.build();
    } else {
      throw new SQLException("Unsupported OAuth grant type: " + grantType);
    }
  }

  /**
   * Creates a Scope object from the scope string.
   *
   * @param scopeStr the space-separated scope string
   * @return the Scope object, or null if the scope string is null or empty
   */
  private @Nullable Scope createScope(@Nullable String scopeStr) {
    if (scopeStr != null && !scopeStr.isEmpty()) {
      return Scope.parse(scopeStr);
    }
    return null;
  }

  /**
   * Creates a list of resource URIs from the resource string.
   *
   * @param resourceStr the resource URI string
   * @return the list of resource URIs, or null if the resource string is null or empty
   */
  private @Nullable List<URI> createResources(@Nullable String resourceStr) {
    if (resourceStr != null && !resourceStr.isEmpty()) {
      return Collections.singletonList(URI.create(resourceStr));
    }
    return null;
  }

  /**
   * Creates a ClientAuthentication object from the configured client credentials.
   *
   * @return the ClientAuthentication object, or null if no credentials are configured
   */
  private @Nullable ClientAuthentication createClientAuthentication() {
    if (clientId != null && clientSecret != null) {
      return new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));
    }
    return null;
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

    /**
     * Sets the OAuth grant type from a string value.
     *
     * <p>Accepts either user-friendly names ("client_credentials", "token_exchange") or the full
     * URN format as defined in RFC 6749 and RFC 8693.
     *
     * @param flowStr the flow type string (e.g., "client_credentials", "token_exchange")
     * @return this builder
     * @throws SQLException if the flow string is invalid
     */
    public Builder flow(String flowStr) throws SQLException {
      if (flowStr == null || flowStr.isEmpty()) {
        throw new SQLException("OAuth flow cannot be null or empty");
      }
      try {
        String normalized = flowStr.toLowerCase(Locale.ROOT);
        // Map user-friendly names to URN format for token_exchange
        if ("token_exchange".equals(normalized)) {
          normalized = GrantType.TOKEN_EXCHANGE.getValue();
        }
        GrantType parsed = GrantType.parse(normalized);
        if (!parsed.equals(GrantType.CLIENT_CREDENTIALS)
            && !parsed.equals(GrantType.TOKEN_EXCHANGE)) {
          throw new SQLException("Unsupported OAuth flow: " + flowStr);
        }
        this.grantType = parsed;
      } catch (com.nimbusds.oauth2.sdk.ParseException e) {
        throw new SQLException("Invalid OAuth flow: " + flowStr, e);
      }
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

    public OAuthConfiguration build() throws SQLException {
      return new OAuthConfiguration(this);
    }
  }
}
