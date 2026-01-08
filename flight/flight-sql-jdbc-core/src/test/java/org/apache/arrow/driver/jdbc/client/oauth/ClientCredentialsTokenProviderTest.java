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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

/** Tests for {@link ClientCredentialsTokenProvider}. */
public class ClientCredentialsTokenProviderTest {

  private static final String DEFAULT_CLIENT_ID = "client";
  private static final String DEFAULT_CLIENT_SECRET = "secret";
  private static final String TEST_CLIENT_ID = "test-client";
  private static final String TEST_CLIENT_SECRET = "test-secret";
  private static final String DEFAULT_SCOPE = "read write";
  private static final int DEFAULT_EXPIRES_IN = 3600;
  private static final int SHORT_EXPIRES_IN = 1;

  private MockWebServer mockServer;
  private URI tokenUri;

  static MockResponse mockResponse =
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
    enqueueMockTokenResponse(token, DEFAULT_EXPIRES_IN);
  }

  private void enqueueMockTokenResponse(String token, int expiresIn) {
    String body =
        String.format(
            "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}",
            token, expiresIn);
    mockServer.enqueue(mockResponse.clone().setBody(body));
  }

  private void assertRequestBodyContains(String... expectedSubstrings) throws Exception {
    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());
    String body = request.getBody().readUtf8();
    for (String expected : expectedSubstrings) {
      assertTrue(body.contains(expected), "Request body should contain: " + expected);
    }
  }

  private void assertRequestBodyDoesNotContain(String... unexpectedSubstrings) throws Exception {
    RecordedRequest request = mockServer.takeRequest();
    String body = request.getBody().readUtf8();
    for (String unexpected : unexpectedSubstrings) {
      assertFalse(body.contains(unexpected), "Request body should not contain: " + unexpected);
    }
  }

  private ClientCredentialsTokenProvider buildTestProvider() {
    return OAuthTokenProviders.clientCredentials()
        .tokenUri(tokenUri)
        .clientId(TEST_CLIENT_ID)
        .clientSecret(TEST_CLIENT_SECRET)
        .scope(DEFAULT_SCOPE)
        .build();
  }

  @Test
  public void testSuccessfulTokenRequest() throws Exception {
    String expectedToken = "test-access-token-12345";
    enqueueMockTokenResponse(expectedToken);

    ClientCredentialsTokenProvider provider = buildTestProvider();

    String token = provider.getValidToken();
    assertEquals(expectedToken, token);
    assertRequestBodyContains("grant_type=client_credentials", "scope=read+write");
  }

  @Test
  public void testTokenCaching() throws Exception {
    String expectedToken = "cached-token";
    enqueueMockTokenResponse(expectedToken);
    enqueueMockTokenResponse("new-not-cached-token");

    ClientCredentialsTokenProvider provider = buildTestProvider();

    String token1 = provider.getValidToken();
    String token2 = provider.getValidToken();
    String token3 = provider.getValidToken();

    assertEquals(expectedToken, token1);
    assertEquals(expectedToken, token2);
    assertEquals(expectedToken, token3);
    assertEquals(1, mockServer.getRequestCount());
  }

  @Test
  public void testTokenRefreshAfterExpiration() throws Exception {
    String initialToken = "short-lived-token";
    String refreshedToken = "refreshed-token";
    enqueueMockTokenResponse(initialToken, SHORT_EXPIRES_IN);
    enqueueMockTokenResponse(refreshedToken);

    ClientCredentialsTokenProvider provider = buildTestProvider();

    String token1 = provider.getValidToken();
    String token2 = provider.getValidToken();

    assertEquals(initialToken, token1);
    assertEquals(refreshedToken, token2);
    assertNotEquals(token1, token2);
    assertEquals(2, mockServer.getRequestCount());
  }

  @Test
  public void testBasicAuthHeader() throws Exception {
    enqueueMockTokenResponse("token");

    ClientCredentialsTokenProvider provider =
        OAuthTokenProviders.clientCredentials()
            .tokenUri(tokenUri)
            .clientId(DEFAULT_CLIENT_ID)
            .clientSecret("my secret")
            .build();

    provider.getValidToken();

    RecordedRequest request = mockServer.takeRequest();
    String authHeader = request.getHeader("Authorization");
    assertNotNull(authHeader);
    String[] authHeaderParts = authHeader.split(" ");
    assertEquals(2, authHeaderParts.length);
    assertEquals("Basic", authHeaderParts[0]);
    // OAuth 2.0 RFC 6749 section 2.3.1, specifies that client_id and client_secret are url encoded
    // before applying base64 encoding
    assertEquals("client:my+secret", new String(Base64.getDecoder().decode(authHeaderParts[1])));
  }

  @Test
  public void testConcurrentAccessOnlyOneRequest() throws Exception {
    enqueueMockTokenResponse("concurrent-token");

    ClientCredentialsTokenProvider provider = buildTestProvider();

    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              startLatch.await();
              String token = provider.getValidToken();
              if ("concurrent-token".equals(token)) {
                successCount.incrementAndGet();
              }
            } catch (Exception e) {
              // Ignore
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    assertEquals(threadCount, successCount.get());
    assertEquals(1, mockServer.getRequestCount());
  }

  @Test
  public void testEmptyScopeIsIgnored() throws Exception {
    enqueueMockTokenResponse("token");

    ClientCredentialsTokenProvider provider = OAuthTokenProviders.clientCredentials()
        .tokenUri(tokenUri)
        .clientId(DEFAULT_CLIENT_ID)
        .clientSecret(DEFAULT_CLIENT_SECRET)
        .scope("").build();

    provider.getValidToken();
    assertRequestBodyDoesNotContain("scope");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("provideInvalidBuilderConfigurations")
  void testBuilderValidation(
      String testName,
      String tokenUriStr,
      String clientId,
      String clientSecret,
      Class<? extends Exception> expectedException,
      String expectedMessageFragment) {

    Exception exception =
        assertThrows(
            expectedException,
            () -> {
              OAuthTokenProviders.ClientCredentialsBuilder builder =
                  OAuthTokenProviders.clientCredentials();

              if (tokenUriStr != null) {
                builder.tokenUri(tokenUriStr);
              }
              if (clientId != null) {
                builder.clientId(clientId);
              }
              if (clientSecret != null) {
                builder.clientSecret(clientSecret);
              }

              builder.build();
            });

    assertTrue(
        exception.getMessage().contains(expectedMessageFragment),
        "Exception message should contain: " + expectedMessageFragment);
  }

  private static Stream<Arguments> provideInvalidBuilderConfigurations() {
    return Stream.of(
        Arguments.of(
            "Missing tokenUri",
            null,
            TEST_CLIENT_ID,
            TEST_CLIENT_SECRET,
            IllegalStateException.class,
            "tokenUri is required"),
        Arguments.of(
            "Missing clientId",
            "https://auth.example.com/token",
            null,
            TEST_CLIENT_SECRET,
            IllegalStateException.class,
            "clientId is required"),
        Arguments.of(
            "Missing clientSecret",
            "https://auth.example.com/token",
            TEST_CLIENT_ID,
            null,
            IllegalStateException.class,
            "clientSecret is required"),
        Arguments.of(
            "Invalid tokenUri",
            "not a valid uri ://",
            TEST_CLIENT_ID,
            TEST_CLIENT_SECRET,
            IllegalArgumentException.class,
            "Invalid token URI"));
  }
}
