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
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link TokenExchangeTokenProvider}. */
public class TokenExchangeTokenProviderTest {

  private MockWebServer mockServer;
  private URI tokenUri;

  private static final String SUBJECT_TOKEN = "original-subject-token";
  private static final String SUBJECT_TOKEN_TYPE = TokenTypeURI.JWT.toString();

  static MockResponse mockResponse =
      new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json");

  private static String tokenResponse(String tokenValue) {
    return String.format(
        "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}", tokenValue);
  }

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

  @Test
  public void testSuccessfulTokenExchange() throws Exception {
    String expectedToken = "exchanged-access-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    String audience = "https://api.example.com";

    String actorToken = "actor-token-value";
    String actorTokenType = "urn:ietf:params:oauth:token-type:access_token";

    String requestedTokenType = "urn:ietf:params:oauth:token-type:refresh_token";

    TokenExchangeGrant grant =
        new TokenExchangeGrant(
            new TypelessAccessToken(SUBJECT_TOKEN),
            TokenTypeURI.parse(SUBJECT_TOKEN_TYPE),
            new TypelessAccessToken(actorToken),
            TokenTypeURI.parse(actorTokenType),
            TokenTypeURI.parse(requestedTokenType),
            Collections.singletonList(new Audience(audience)));

    TokenExchangeTokenProvider provider =
        new TokenExchangeTokenProvider(tokenUri, grant, null, Scope.parse("dremio.all"), null);

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());
    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));

    assertTrue(body.contains("subject_token_type=" + SUBJECT_TOKEN_TYPE));
    assertTrue(body.contains("audience=" + audience));

    assertTrue(body.contains("scope=dremio.all"));

    assertTrue(body.contains("requested_token_type=" + requestedTokenType));
  }

  @Test
  public void testTokenCaching() throws Exception {
    String expectedToken = "cached-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));
    mockServer.enqueue(mockResponse.setBody(tokenResponse("new-not-cached-token")));

    TokenExchangeGrant grant =
        new TokenExchangeGrant(
            new TypelessAccessToken(SUBJECT_TOKEN), TokenTypeURI.parse(SUBJECT_TOKEN_TYPE));

    TokenExchangeTokenProvider provider = new TokenExchangeTokenProvider(tokenUri, grant);

    String token1 = provider.getValidToken();
    String token2 = provider.getValidToken();
    String token3 = provider.getValidToken();

    assertEquals(expectedToken, token1);
    assertEquals(expectedToken, token2);
    assertEquals(expectedToken, token3);
    assertEquals(1, mockServer.getRequestCount());
  }

  // Builder pattern tests

  @Test
  public void testBuilderWithRequiredParametersOnly() throws Exception {
    String expectedToken = "builder-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    TokenExchangeTokenProvider provider =
        TokenExchangeTokenProvider.builder()
            .tokenUri(tokenUri)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType(SUBJECT_TOKEN_TYPE)
            .build();

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();
    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
    assertTrue(body.contains("subject_token_type=" + SUBJECT_TOKEN_TYPE));
  }

  @Test
  public void testBuilderWithAllParameters() throws Exception {
    String expectedToken = "full-builder-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    String audience = "https://api.example.com";
    String actorToken = "actor-token-value";
    String actorTokenType = "urn:ietf:params:oauth:token-type:access_token";
    String requestedTokenType = "urn:ietf:params:oauth:token-type:refresh_token";

    TokenExchangeTokenProvider provider =
        TokenExchangeTokenProvider.builder()
            .tokenUri(tokenUri)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType(SUBJECT_TOKEN_TYPE)
            .actorToken(actorToken)
            .actorTokenType(actorTokenType)
            .audience(audience)
            .requestedTokenType(requestedTokenType)
            .scope(Scope.parse("dremio.all"))
            .build();

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();
    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
    assertTrue(body.contains("audience=" + audience));
    assertTrue(body.contains("scope=dremio.all"));
    assertTrue(body.contains("requested_token_type=" + requestedTokenType));
  }

  @Test
  public void testBuilderMissingTokenUri() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                TokenExchangeTokenProvider.builder()
                    .subjectToken(SUBJECT_TOKEN)
                    .subjectTokenType(SUBJECT_TOKEN_TYPE)
                    .build());

    assertEquals("tokenUri is required", exception.getMessage());
  }

  @Test
  public void testBuilderMissingSubjectToken() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                TokenExchangeTokenProvider.builder()
                    .tokenUri(tokenUri)
                    .subjectTokenType(SUBJECT_TOKEN_TYPE)
                    .build());

    assertEquals("subject_token is required", exception.getMessage());
  }

  @Test
  public void testBuilderMissingSubjectTokenType() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                TokenExchangeTokenProvider.builder()
                    .tokenUri(tokenUri)
                    .subjectToken(SUBJECT_TOKEN)
                    .build());

    assertEquals("subject_token_type is required", exception.getMessage());
  }

  @Test
  public void testBuilderWithResourceParameter() throws Exception {
    String expectedToken = "resource-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    String resource = "https://api.example.com/resource";
    List<URI> resources = Collections.singletonList(URI.create(resource));

    TokenExchangeTokenProvider provider =
        TokenExchangeTokenProvider.builder()
            .tokenUri(tokenUri)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType(SUBJECT_TOKEN_TYPE)
            .resources(resources)
            .build();

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();
    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
    assertTrue(body.contains("resource=" + resource));
  }

  @Test
  public void testBuilderWithClientAuthentication() throws Exception {
    String expectedToken = "client-auth-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    String clientId = "test-client-id";
    String clientSecret = "test-client-secret";
    ClientSecretBasic clientAuth =
        new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));

    TokenExchangeTokenProvider provider =
        TokenExchangeTokenProvider.builder()
            .tokenUri(tokenUri)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType(SUBJECT_TOKEN_TYPE)
            .clientAuthentication(clientAuth)
            .build();

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();

    // Verify Basic auth header is present
    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader);
    assertTrue(authHeader.startsWith("Basic "));

    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
  }

  @Test
  public void testBuilderWithResourceAndClientAuth() throws Exception {
    String expectedToken = "full-config-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    String resource = "https://api.example.com/resource";
    List<URI> resources = Collections.singletonList(URI.create(resource));
    String clientId = "test-client-id";
    String clientSecret = "test-client-secret";
    ClientSecretBasic clientAuth =
        new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));

    TokenExchangeTokenProvider provider =
        TokenExchangeTokenProvider.builder()
            .tokenUri(tokenUri)
            .subjectToken(SUBJECT_TOKEN)
            .subjectTokenType(SUBJECT_TOKEN_TYPE)
            .resources(resources)
            .clientAuthentication(clientAuth)
            .scope(Scope.parse("dremio.all"))
            .build();

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();

    // Verify Basic auth header is present
    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader);
    assertTrue(authHeader.startsWith("Basic "));

    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
    assertTrue(body.contains("resource=" + resource));
    assertTrue(body.contains("scope=dremio.all"));
  }

  @Test
  public void testConstructorWithResourceAndClientAuth() throws Exception {
    String expectedToken = "constructor-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));

    String resource = "https://api.example.com/resource";
    List<URI> resources = Collections.singletonList(URI.create(resource));
    String clientId = "test-client-id";
    String clientSecret = "test-client-secret";
    ClientSecretBasic clientAuth =
        new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));

    TokenExchangeGrant grant =
        new TokenExchangeGrant(
            new TypelessAccessToken(SUBJECT_TOKEN), TokenTypeURI.parse(SUBJECT_TOKEN_TYPE));

    TokenExchangeTokenProvider provider =
        new TokenExchangeTokenProvider(
            tokenUri, grant, clientAuth, Scope.parse("dremio.all"), resources);

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();

    // Verify Basic auth header is present
    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader);
    assertTrue(authHeader.startsWith("Basic "));

    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("resource=" + resource));
    assertTrue(body.contains("scope=dremio.all"));
  }
}
