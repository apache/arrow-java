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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nimbusds.oauth2.sdk.Scope;
import java.net.URI;
import java.sql.SQLException;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link OAuthConfiguration}. */
public class OAuthConfigurationTest {

  private static final String TOKEN_URI = "https://auth.example.com/token";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";
  private static final String SCOPE = "read write";
  private static final String SUBJECT_TOKEN = "subject-token-value";
  public static final String RESOURCE = "https://api.example.com/resource";

  @FunctionalInterface
  interface BuilderConfigurer {
    void configure(OAuthConfiguration.Builder builder) throws SQLException;
  }

  static Stream<Arguments> createFlowCases() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "string flow", (BuilderConfigurer) builder -> builder.flow("client_credentials"))),
        Arguments.of(
            Named.of(
                "uppercase string flow",
                (BuilderConfigurer) builder -> builder.flow("CLIENT_CREDENTIALS"))));
  }

  @ParameterizedTest
  @MethodSource("createFlowCases")
  public void testCreateFlowConfiguration(BuilderConfigurer flowConfigurer) throws SQLException {
    OAuthConfiguration.Builder builder = new OAuthConfiguration.Builder();
    flowConfigurer.configure(builder);
    OAuthConfiguration config =
        builder.tokenUri(TOKEN_URI).clientId(CLIENT_ID).clientSecret(CLIENT_SECRET).build();

    // Verify configuration creates correct provider type
    OAuthTokenProvider provider = config.createTokenProvider();
    assertInstanceOf(ClientCredentialsTokenProvider.class, provider);
  }

  @Test
  public void testCreateClientCredentialsTokenProvider() throws SQLException {
    OAuthConfiguration config =
        new OAuthConfiguration.Builder()
            .flow("client_credentials")
            .tokenUri(TOKEN_URI)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .scope(SCOPE)
            .build();

    OAuthTokenProvider provider = config.createTokenProvider();

    assertNotNull(provider);
    assertInstanceOf(ClientCredentialsTokenProvider.class, provider);

    ClientCredentialsTokenProvider ccProvider = (ClientCredentialsTokenProvider) provider;
    assertEquals(URI.create(TOKEN_URI), ccProvider.tokenUri);
    assertEquals(CLIENT_ID, ccProvider.clientAuth.getClientID().getValue());
    assertEquals(Scope.parse(SCOPE), ccProvider.scope);
  }

  @Test
  public void testCreateTokenExchangeTokenProviderWithAllOptions() throws SQLException {
    String subjectTokenType = "urn:ietf:params:oauth:token-type:access_token";
    String actorToken = "actor-token-value";
    String actorTokenType = "urn:ietf:params:oauth:token-type:jwt";
    String audience = "https://api.example.com";
    String requestedTokenType = "urn:ietf:params:oauth:token-type:access_token";

    OAuthConfiguration config =
        new OAuthConfiguration.Builder()
            .flow("token_exchange")
            .tokenUri(TOKEN_URI)
            .scope(SCOPE)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .resource(RESOURCE)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType(subjectTokenType)
            .actorToken(actorToken)
            .actorTokenType(actorTokenType)
            .audience(audience)
            .requestedTokenType(requestedTokenType)
            .build();

    OAuthTokenProvider provider = config.createTokenProvider();

    assertNotNull(provider);
    assertInstanceOf(TokenExchangeTokenProvider.class, provider);

    TokenExchangeTokenProvider teProvider = (TokenExchangeTokenProvider) provider;
    assertEquals(URI.create(TOKEN_URI), teProvider.tokenUri);
    assertNotNull(teProvider.grant);
    assertEquals(SUBJECT_TOKEN, teProvider.grant.getSubjectToken().getValue());
    assertEquals(subjectTokenType, teProvider.grant.getSubjectTokenType().getURI().toString());
    assertEquals(actorToken, teProvider.grant.getActorToken().getValue());
    assertEquals(actorTokenType, teProvider.grant.getActorTokenType().getURI().toString());
    assertNotNull(teProvider.grant.getAudience());
    assertEquals(1, teProvider.grant.getAudience().size());
    assertEquals(audience, teProvider.grant.getAudience().get(0).getValue());
    assertEquals(requestedTokenType, teProvider.grant.getRequestedTokenType().getURI().toString());
    assertEquals(Scope.parse(SCOPE), teProvider.scope);
    assertEquals(Collections.singletonList(URI.create(RESOURCE)), teProvider.resources);

    assertEquals(CLIENT_ID, teProvider.clientAuth.getClientID().getValue());
  }

  static Stream<Arguments> generalValidationErrorCases() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "null flow",
                (BuilderConfigurer) builder -> builder.flow((String) null).tokenUri(TOKEN_URI)),
            "OAuth flow cannot be null or empty"),
        Arguments.of(
            Named.of(
                "empty flow", (BuilderConfigurer) builder -> builder.flow("").tokenUri(TOKEN_URI)),
            "OAuth flow cannot be null or empty"),
        Arguments.of(
            Named.of(
                "invalid flow",
                (BuilderConfigurer) builder -> builder.flow("invalid_flow").tokenUri(TOKEN_URI)),
            "Unsupported OAuth flow: invalid_flow"),
        Arguments.of(
            Named.of(
                "null tokenUri",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("client_credentials")
                            .tokenUri((String) null)
                            .clientId(CLIENT_ID)
                            .clientSecret(CLIENT_SECRET)),
            "Token URI cannot be null or empty"),
        Arguments.of(
            Named.of(
                "empty tokenUri",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("client_credentials")
                            .tokenUri("")
                            .clientId(CLIENT_ID)
                            .clientSecret(CLIENT_SECRET)),
            "Token URI cannot be null or empty"),
        Arguments.of(
            Named.of(
                "invalid tokenUri",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("client_credentials")
                            .tokenUri("not a valid uri ://")
                            .clientId(CLIENT_ID)
                            .clientSecret(CLIENT_SECRET)),
            null),
        Arguments.of(
            Named.of(
                "invalid tokenUri",
                (BuilderConfigurer)
                    builder ->
                        builder.flow("client_credentials").tokenUri(TOKEN_URI).clientId(CLIENT_ID)),
            // null means verify exception has message and cause
            "clientSecret is required for client_credentials flow"));
  }

  @ParameterizedTest
  @MethodSource("generalValidationErrorCases")
  public void testGeneralValidationErrors(BuilderConfigurer configurer, String expectedMessage) {
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> {
              OAuthConfiguration.Builder builder = new OAuthConfiguration.Builder();
              configurer.configure(builder);
              builder.build();
            });

    if (expectedMessage != null) {
      assertEquals(expectedMessage, exception.getMessage());
    } else {
      assertNotNull(exception.getMessage());
      assertNotNull(exception.getCause());
    }
  }

  static Stream<Arguments> flowSpecificValidationErrorCases() {
    return Stream.of(
        // client_credentials flow validation
        Arguments.of(
            Named.of(
                "client_credentials: missing clientId",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("client_credentials")
                            .tokenUri(TOKEN_URI)
                            .clientSecret(CLIENT_SECRET)),
            "clientId is required for client_credentials flow"),
        Arguments.of(
            Named.of(
                "client_credentials: missing clientSecret",
                (BuilderConfigurer)
                    builder ->
                        builder.flow("client_credentials").tokenUri(TOKEN_URI).clientId(CLIENT_ID)),
            "clientSecret is required for client_credentials flow"),
        // token_exchange flow validation
        Arguments.of(
            Named.of(
                "token_exchange: missing subjectToken",
                (BuilderConfigurer) builder -> builder.flow("token_exchange").tokenUri(TOKEN_URI)),
            "subjectToken is required for token_exchange flow"),
        Arguments.of(
            Named.of(
                "token_exchange: empty subjectToken",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("token_exchange")
                            .tokenUri(TOKEN_URI)
                            .subjectToken("")
                            .subjectTokenType("urn:ietf:params:oauth:token-type:access_token")),
            "subjectToken is required for token_exchange flow"),
        Arguments.of(
            Named.of(
                "token_exchange: missing subjectTokenType",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("token_exchange")
                            .tokenUri(TOKEN_URI)
                            .subjectToken(SUBJECT_TOKEN)),
            "subjectTokenType is required for token_exchange flow"),
        Arguments.of(
            Named.of(
                "token_exchange: empty subjectTokenType",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("token_exchange")
                            .tokenUri(TOKEN_URI)
                            .subjectToken(SUBJECT_TOKEN)
                            .subjectTokenType("")),
            "subjectTokenType is required for token_exchange flow"));
  }

  @ParameterizedTest
  @MethodSource("flowSpecificValidationErrorCases")
  public void testFlowSpecificValidationErrors(
      BuilderConfigurer configurer, String expectedMessage) {
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> {
              OAuthConfiguration.Builder builder = new OAuthConfiguration.Builder();
              configurer.configure(builder);
              builder.build();
            });

    assertEquals(expectedMessage, exception.getMessage());
  }
}
