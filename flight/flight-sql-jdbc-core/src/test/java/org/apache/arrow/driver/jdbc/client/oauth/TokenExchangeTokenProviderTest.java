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
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
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

    String clientId = "client-id";
    String clientSecret = "client-secret";

    String resource = "https://api.example.com/resource";

    String requestedTokenType = "urn:ietf:params:oauth:token-type:refresh_token";

    TokenExchangeTokenProvider provider =
        new TokenExchangeTokenProvider(
            tokenUri,
            SUBJECT_TOKEN,
            SUBJECT_TOKEN_TYPE,
            actorToken,
            actorTokenType,
            audience,
            resource,
            requestedTokenType,
            "dremio.all",
            clientId,
            clientSecret);

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());
    String body = URLDecoder.decode(request.getBody().readUtf8(), StandardCharsets.UTF_8);
    assertTrue(body.contains("grant_type=" + GrantType.TOKEN_EXCHANGE));
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));

    assertTrue(body.contains("subject_token_type=" + SUBJECT_TOKEN_TYPE));
    assertTrue(body.contains("audience=" + audience));

    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader);
    assertTrue(authHeader.startsWith("Basic "));

    assertTrue(body.contains("scope=dremio.all"));
    assertTrue(body.contains("resource=" + resource));

    assertTrue(body.contains("requested_token_type=" + requestedTokenType));
  }

  @Test
  public void testTokenCaching() throws Exception {
    String expectedToken = "cached-token";
    mockServer.enqueue(mockResponse.setBody(tokenResponse(expectedToken)));
    mockServer.enqueue(mockResponse.setBody(tokenResponse("new-not-cached-token")));

    TokenExchangeTokenProvider provider =
        new TokenExchangeTokenProvider(
            tokenUri,
            SUBJECT_TOKEN,
            SUBJECT_TOKEN_TYPE,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    String token1 = provider.getValidToken();
    String token2 = provider.getValidToken();
    String token3 = provider.getValidToken();

    assertEquals(expectedToken, token1);
    assertEquals(expectedToken, token2);
    assertEquals(expectedToken, token3);
    assertEquals(1, mockServer.getRequestCount());
  }

  @Test
  public void testErrorResponse() throws Exception {
    mockServer.enqueue(
        new MockResponse()
            .setResponseCode(400)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"error\":\"invalid_grant\",\"error_description\":\"Token expired\"}"));

    TokenExchangeTokenProvider provider =
        new TokenExchangeTokenProvider(
            tokenUri,
            SUBJECT_TOKEN,
            SUBJECT_TOKEN_TYPE,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    SQLException exception = assertThrows(SQLException.class, provider::getValidToken);
    assertTrue(exception.getMessage().contains("invalid_grant"));
  }
}
