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

import java.util.Base64;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link ClientCredentialsTokenProvider}. */
public class ClientCredentialsTokenProviderTest {

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

  @Test
  public void testSuccessfulTokenRequest() throws Exception {
    String expectedToken = "test-access-token-12345";
    mockServer.enqueue(
        mockResponse.setBody(
            String.format(
                "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}",
                expectedToken)));

    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "test-client", "test-secret", "read write");

    String token = provider.getValidToken();

    assertEquals(expectedToken, token);
    RecordedRequest request = mockServer.takeRequest();
    assertEquals("POST", request.getMethod());

    String body = request.getBody().readUtf8();
    assertTrue(body.contains("grant_type=client_credentials"));
    assertTrue(body.contains("scope=read+write"));
  }

  @Test
  public void testTokenCaching() throws Exception {
    String expectedToken = "cached-token";
    mockServer.enqueue(
        mockResponse.setBody(
            String.format(
            "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}", expectedToken)));

    // return new token after obtaining cached-token
    mockServer.enqueue(
        mockResponse.setBody(
            "{\"access_token\":\"new-not-cached-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"));
    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "client", "secret", null);

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
    mockServer.enqueue(
        mockResponse.setBody(
            String.format(
                "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":1}",
                initialToken)));
    mockServer.enqueue(
        mockResponse.setBody(
            String.format(
                "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}",
                refreshedToken)));

    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "client", "secret", null);

    String token1 = provider.getValidToken();
    String token2 = provider.getValidToken();

    assertEquals(initialToken, token1);
    assertEquals(refreshedToken, token2);
    assertNotEquals(token1, token2);
    assertEquals(2, mockServer.getRequestCount());
  }

  @Test
  public void testBasicAuthHeader() throws Exception {
    mockServer.enqueue(
        mockResponse.setBody(
            "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"));

    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "client", "my secret", null);

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
    mockServer.enqueue(
        mockResponse.setBody(
            "{\"access_token\":\"concurrent-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"));

    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "client", "secret", null);

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
  public void testDefaultExpirationWhenNotProvided() throws Exception {
    mockServer.enqueue(
        mockResponse.setBody("{\"access_token\":\"no-expiry-token\",\"token_type\":\"Bearer\"}"));

    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "client", "secret", null);

    String token = provider.getValidToken();
    assertEquals("no-expiry-token", token);
  }

  @Test
  public void testEmptyScopeIsIgnored() throws Exception {
    mockServer.enqueue(
        mockResponse.setBody(
            "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"));

    ClientCredentialsTokenProvider provider =
        new ClientCredentialsTokenProvider(tokenUri, "client", "secret", "");

    provider.getValidToken();

    RecordedRequest request = mockServer.takeRequest();
    String body = request.getBody().readUtf8();
    assertFalse(body.contains("scope"));
  }
}
