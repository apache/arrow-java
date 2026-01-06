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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.sql.SQLException;
import java.util.function.Consumer;
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

  @FunctionalInterface
  interface BuilderConfigurer {
    void configure(OAuthConfiguration.Builder builder) throws SQLException;
  }

  static Stream<Arguments> clientCredentialsFlowCases() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "string flow",
                (BuilderConfigurer) builder -> builder.flow("client_credentials"))),
        Arguments.of(
            Named.of(
                "enum flow",
                (BuilderConfigurer)
                    builder ->
                        builder.flow(OAuthConfiguration.OAuthFlow.CLIENT_CREDENTIALS))),
        Arguments.of(
            Named.of(
                "uppercase string flow",
                (BuilderConfigurer) builder -> builder.flow("CLIENT_CREDENTIALS"))));
  }

  @ParameterizedTest
  @MethodSource("clientCredentialsFlowCases")
  public void testClientCredentialsFlowConfiguration(BuilderConfigurer flowConfigurer)
      throws SQLException {
    OAuthConfiguration.Builder builder = new OAuthConfiguration.Builder();
    flowConfigurer.configure(builder);
    OAuthConfiguration config =
        builder
            .tokenUri(TOKEN_URI)
            .clientId(CLIENT_ID)
            .clientSecret(CLIENT_SECRET)
            .scope(SCOPE)
            .build();

    assertEquals(OAuthConfiguration.OAuthFlow.CLIENT_CREDENTIALS, config.getFlow());
    assertEquals(URI.create(TOKEN_URI), config.getTokenUri());
    assertEquals(CLIENT_ID, config.getClientId());
    assertEquals(CLIENT_SECRET, config.getClientSecret());
    assertEquals(SCOPE, config.getScope());
  }

  static Stream<Arguments> tokenExchangeConfigurationCases() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "all optional fields",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .audience("target-audience")
                            .resource("https://api.example.com")
                            .exchangeScope("api:read")),
            (Consumer<OAuthConfiguration>)
                config -> {
                  assertEquals("target-audience", config.getAudience());
                  assertEquals("https://api.example.com", config.getResource());
                  assertEquals("api:read", config.getExchangeScope());
                }),
        Arguments.of(
            Named.of(
                "actor token fields",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .actorToken("actor-token-value")
                            .actorTokenType("urn:ietf:params:oauth:token-type:access_token")),
            (Consumer<OAuthConfiguration>)
                config -> {
                  assertEquals("actor-token-value", config.getActorToken());
                  assertEquals(
                      "urn:ietf:params:oauth:token-type:access_token",
                      config.getActorTokenType());
                }),
        Arguments.of(
            Named.of(
                "requested token type",
                (BuilderConfigurer)
                    builder ->
                        builder.requestedTokenType(
                            "urn:ietf:params:oauth:token-type:id_token")),
            (Consumer<OAuthConfiguration>)
                config ->
                    assertEquals(
                        "urn:ietf:params:oauth:token-type:id_token",
                        config.getRequestedTokenType())));
  }

  @ParameterizedTest
  @MethodSource("tokenExchangeConfigurationCases")
  public void testTokenExchangeConfiguration(
      BuilderConfigurer optionalConfigurer, Consumer<OAuthConfiguration> verifier)
      throws SQLException {
    OAuthConfiguration.Builder builder =
        new OAuthConfiguration.Builder()
            .flow("token_exchange")
            .tokenUri(TOKEN_URI)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType("urn:ietf:params:oauth:token-type:access_token");
    optionalConfigurer.configure(builder);
    OAuthConfiguration config = builder.build();

    assertEquals(OAuthConfiguration.OAuthFlow.TOKEN_EXCHANGE, config.getFlow());
    assertEquals(URI.create(TOKEN_URI), config.getTokenUri());
    assertEquals(SUBJECT_TOKEN, config.getSubjectToken());
    assertEquals(
        "urn:ietf:params:oauth:token-type:access_token", config.getSubjectTokenType());
    verifier.accept(config);
  }

  static Stream<Arguments> tokenProviderCases() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "client_credentials provider",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("client_credentials")
                            .tokenUri(TOKEN_URI)
                            .clientId(CLIENT_ID)
                            .clientSecret(CLIENT_SECRET)),
            ClientCredentialsTokenProvider.class),
        Arguments.of(
            Named.of(
                "token_exchange provider",
                (BuilderConfigurer)
                    builder ->
                        builder
                            .flow("token_exchange")
                            .tokenUri(TOKEN_URI)
                            .subjectToken(SUBJECT_TOKEN)
                            .subjectTokenType("urn:ietf:params:oauth:token-type:access_token")),
            TokenExchangeTokenProvider.class));
  }

  @ParameterizedTest
  @MethodSource("tokenProviderCases")
  public void testCreateTokenProvider(
      BuilderConfigurer configurer, Class<? extends OAuthTokenProvider> expectedProviderClass)
      throws SQLException {
    OAuthConfiguration.Builder builder = new OAuthConfiguration.Builder();
    configurer.configure(builder);
    OAuthConfiguration config = builder.build();

    OAuthTokenProvider provider = config.createTokenProvider();

    assertNotNull(provider);
    assertTrue(expectedProviderClass.isInstance(provider));
  }

  @Test
  public void testOptionalFieldsAreNullByDefault() throws SQLException {
    OAuthConfiguration config =
        new OAuthConfiguration.Builder()
            .flow("token_exchange")
            .tokenUri(TOKEN_URI)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType("urn:ietf:params:oauth:token-type:access_token")
            .build();

    assertNull(config.getClientId());
    assertNull(config.getClientSecret());
    assertNull(config.getScope());
    assertNull(config.getActorToken());
    assertNull(config.getActorTokenType());
    assertNull(config.getAudience());
    assertNull(config.getResource());
    assertNull(config.getRequestedTokenType());
    assertNull(config.getExchangeScope());
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
            "Unknown OAuth flow: invalid_flow"),
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
            null)); // null means verify exception has message and cause
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
  public void testFlowSpecificValidationErrors(BuilderConfigurer configurer, String expectedMessage)
      throws SQLException {
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
