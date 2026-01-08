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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link TokenExchangeTokenProvider}. */
public class TokenExchangeTokenProviderTest {

  private static final String SUBJECT_TOKEN = "original-subject-token";
  private static final String SUBJECT_TOKEN_TYPE = TokenTypeURI.JWT.toString();
  private static final String TEST_AUDIENCE = "https://api.example.com";
  private static final String TEST_RESOURCE_A = "https://api.example.com/resourceA";
  private static final String TEST_RESOURCE_B = "https://graph.example.com/resourceB";
  private static final String TEST_ACTOR_TOKEN = "actor-token-value";
  private static final String TEST_ACTOR_TOKEN_TYPE =
      "urn:ietf:params:oauth:token-type:access_token";
  private static final String TEST_REQUESTED_TOKEN_TYPE =
      "urn:ietf:params:oauth:token-type:refresh_token";
  private static final String TEST_CLIENT_ID = "test-client-id";
  private static final String TEST_CLIENT_SECRET = "test-client-secret";
  private static final String DEFAULT_SCOPE = "dremio.all";

  private MockWebServer mockServer;
  private URI tokenUri;

  private static final MockResponse mockResponse =
      new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json");

  @BeforeEach
  public void setUp() throws Exception {
    mockServer = new MockWebServer();
    mockServer.start();
    tokenUri = mockServer.url("/oauth/token").uri();
  }

  @AfterEach
  public void tearDown() throws Exception {
    mockServer.shutdown();
  }

  // Helper methods for mock responses

  private void enqueueMockTokenResponse(String token) {
    String body =
        String.format(
            "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}", token);
    mockServer.enqueue(mockResponse.clone().setBody(body));
  }

  // Helper methods for request verification

  private void assertRequestBodyContains(String... expectedSubstrings) throws Exception {
    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());
    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    for (String expected : expectedSubstrings) {
      assertTrue(body.contains(expected), "Request body should contain: " + expected);
    }
  }

  private void assertBasicAuthHeaderPresent() throws Exception {
    RecordedRequest request = mockServer.takeRequest();
    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader, "Authorization header should be present");
    assertTrue(authHeader.startsWith("Basic "), "Authorization should be Basic auth");
  }

  private void assertRequestWithBasicAuth(String... expectedBodySubstrings) throws Exception {
    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());

    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader, "Authorization header should be present");
    assertTrue(authHeader.startsWith("Basic "), "Authorization should be Basic auth");

    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    for (String expected : expectedBodySubstrings) {
      assertTrue(body.contains(expected), "Request body should contain: " + expected);
    }
  }

  private ClientSecretBasic createClientSecretBasic() {
    return new ClientSecretBasic(new ClientID(TEST_CLIENT_ID), new Secret(TEST_CLIENT_SECRET));
  }

  private List<URI> createSingleResourceList() {
    return Collections.singletonList(URI.create(TEST_RESOURCE_A));
  }

  private List<URI> createMultipleResourcesList() {
    return java.util.Arrays.asList(URI.create(TEST_RESOURCE_A), URI.create(TEST_RESOURCE_B));
  }

  private OAuthTokenProviders.TokenExchangeBuilder defaultProviderBuilder() {
    return OAuthTokenProviders.tokenExchange()
        .tokenUri(tokenUri)
        .subjectToken(SUBJECT_TOKEN)
        .subjectTokenType(SUBJECT_TOKEN_TYPE);
  }

  @Test
  public void testTokenCaching() throws Exception {
    String expectedToken = "cached-token";
    enqueueMockTokenResponse(expectedToken);
    enqueueMockTokenResponse("new-not-cached-token");

    TokenExchangeTokenProvider provider = defaultProviderBuilder().build();

    String token1 = provider.getValidToken();
    String token2 = provider.getValidToken();
    String token3 = provider.getValidToken();

    assertEquals(expectedToken, token1);
    assertEquals(expectedToken, token2);
    assertEquals(expectedToken, token3);
    assertEquals(1, mockServer.getRequestCount());
  }

  @Test
  public void testBuilderWithRequiredParametersOnly() throws Exception {
    String expectedToken = "builder-token";
    enqueueMockTokenResponse(expectedToken);

    TokenExchangeTokenProvider provider = defaultProviderBuilder().build();

    String token = provider.getValidToken();
    assertEquals(expectedToken, token);

    assertRequestBodyContains(
        "grant_type=" + GrantType.TOKEN_EXCHANGE,
        "subject_token=" + SUBJECT_TOKEN,
        "subject_token_type=" + SUBJECT_TOKEN_TYPE);
  }

  @Test
  public void testBuilderWithAllParameters() throws Exception {
    String expectedToken = "full-builder-token";
    enqueueMockTokenResponse(expectedToken);

    TokenExchangeTokenProvider provider =
        defaultProviderBuilder()
            .clientAuthentication(createClientSecretBasic())
            .actorToken(TEST_ACTOR_TOKEN)
            .actorTokenType(TEST_ACTOR_TOKEN_TYPE)
            .audience(TEST_AUDIENCE)
            .requestedTokenType(TEST_REQUESTED_TOKEN_TYPE)
            .scope(DEFAULT_SCOPE)
            .resources(createSingleResourceList())
            .build();

    String token = provider.getValidToken();
    assertEquals(expectedToken, token);

    assertRequestWithBasicAuth(
        "grant_type=" + GrantType.TOKEN_EXCHANGE,
        "actor_token=" + TEST_ACTOR_TOKEN,
        "actor_token_type=" + TEST_ACTOR_TOKEN_TYPE,
        "subject_token_type=" + SUBJECT_TOKEN_TYPE,
        "subject_token=" + SUBJECT_TOKEN,
        "audience=" + TEST_AUDIENCE,
        "scope=" + DEFAULT_SCOPE,
        "requested_token_type=" + TEST_REQUESTED_TOKEN_TYPE,
        "resource=" + TEST_RESOURCE_A);
  }

  @Test
  public void testBuilderWithResourceAndClientAuth() throws Exception {
    String expectedToken = "full-config-token";
    enqueueMockTokenResponse(expectedToken);

    TokenExchangeTokenProvider provider =
        defaultProviderBuilder()
            .resources(createSingleResourceList())
            .clientAuthentication(createClientSecretBasic())
            .scope(Scope.parse(DEFAULT_SCOPE))
            .build();

    String token = provider.getValidToken();
    assertEquals(expectedToken, token);

    assertRequestWithBasicAuth(
        "grant_type=" + GrantType.TOKEN_EXCHANGE,
        "subject_token=" + SUBJECT_TOKEN,
        "resource=" + TEST_RESOURCE_A,
        "scope=" + DEFAULT_SCOPE);
  }

  @Test
  public void testBuilderWithMultipleResources() throws Exception {
    String expectedToken = "multi-resource-token";
    enqueueMockTokenResponse(expectedToken);

    TokenExchangeTokenProvider provider =
        defaultProviderBuilder().resources(createMultipleResourcesList()).build();

    String token = provider.getValidToken();
    assertEquals(expectedToken, token);

    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());
    String body = request.getBody().readUtf8();

    // Verify both resources are present as separate parameters (URL-encoded)
    assertTrue(
        body.contains("resource=" + java.net.URLEncoder.encode(TEST_RESOURCE_A, "UTF-8")),
        "Request should contain first resource: " + TEST_RESOURCE_A);
    assertTrue(
        body.contains("resource=" + java.net.URLEncoder.encode(TEST_RESOURCE_B, "UTF-8")),
        "Request should contain second resource: " + TEST_RESOURCE_B);

    // Count occurrences of 'resource=' to verify multiple parameters
    int resourceCount = body.split("resource=", -1).length - 1;
    assertEquals(2, resourceCount, "Request should contain exactly 2 resource parameters");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("provideBuilderValidationCases")
  void testBuilderValidation(
      String testName,
      String tokenUriStr,
      String subjectToken,
      String subjectTokenType,
      Class<? extends Exception> expectedException,
      String expectedMessageFragment) {

    Exception exception =
        assertThrows(
            expectedException,
            () -> {
              OAuthTokenProviders.TokenExchangeBuilder builder =
                  OAuthTokenProviders.tokenExchange();

              if (tokenUriStr != null) {
                builder.tokenUri(tokenUriStr);
              }
              if (subjectToken != null) {
                builder.subjectToken(subjectToken);
              }
              if (subjectTokenType != null) {
                builder.subjectTokenType(subjectTokenType);
              }

              builder.build();
            });

    assertTrue(
        exception.getMessage().contains(expectedMessageFragment),
        "Exception message should contain: " + expectedMessageFragment);
  }

  private static Stream<Arguments> provideBuilderValidationCases() {
    return Stream.of(
        Arguments.of(
            "Missing tokenUri",
            null,
            SUBJECT_TOKEN,
            SUBJECT_TOKEN_TYPE,
            IllegalStateException.class,
            "tokenUri is required"),
        Arguments.of(
            "Missing subjectToken",
            "https://auth.example.com/token",
            null,
            SUBJECT_TOKEN_TYPE,
            IllegalStateException.class,
            "subjectToken is required"),
        Arguments.of(
            "Missing subjectTokenType",
            "https://auth.example.com/token",
            SUBJECT_TOKEN,
            null,
            IllegalStateException.class,
            "subjectTokenType is required"),
        Arguments.of(
            "Invalid tokenUri",
            "not a valid uri ://",
            SUBJECT_TOKEN,
            SUBJECT_TOKEN_TYPE,
            IllegalArgumentException.class,
            "Invalid token URI"));
  }
}
