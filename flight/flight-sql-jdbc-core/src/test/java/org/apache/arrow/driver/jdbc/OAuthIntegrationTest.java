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
package org.apache.arrow.driver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import org.apache.arrow.driver.jdbc.authentication.TokenAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration tests for OAuth authentication flows in the JDBC driver.
 *
 * <p>These tests verify that OAuth tokens obtained from an OAuth server are correctly used in
 * Flight SQL requests.
 */
public class OAuthIntegrationTest {

  private static final String VALID_ACCESS_TOKEN = "valid-oauth-access-token-12345";
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-client-secret";
  private static final String SUBJECT_TOKEN = "original-subject-token";
  private static final String SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";
  private static final String TEST_SCOPE = "dremio.all";

  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();

  @RegisterExtension public static FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;

  static {
    FLIGHT_SERVER_TEST_EXTENSION =
        new FlightServerTestExtension.Builder()
            .authentication(new TokenAuthentication.Builder().token(VALID_ACCESS_TOKEN).build())
            .producer(FLIGHT_SQL_PRODUCER)
            .build();
  }

  @StartStop private final MockWebServer oauthServer = new MockWebServer();
  private URI tokenEndpoint;

  @BeforeAll
  public static void setUpClass() {
    // Register a simple catalog query handler
    FLIGHT_SQL_PRODUCER.addCatalogQuery(
        CommandGetCatalogs.getDefaultInstance(),
        listener -> {
          try (BufferAllocator allocator = new RootAllocator();
              VectorSchemaRoot root =
                  VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator)) {
            root.setRowCount(0);
            listener.start(root);
            listener.putNext();
          } catch (Throwable t) {
            listener.error(t);
          } finally {
            listener.completed();
          }
        });

    // Register a simple schema query handler for getSchemas()
    FLIGHT_SQL_PRODUCER.addCatalogQuery(
        CommandGetDbSchemas.getDefaultInstance(),
        listener -> {
          try (BufferAllocator allocator = new RootAllocator();
              VectorSchemaRoot root =
                  VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
            root.setRowCount(0);
            listener.start(root);
            listener.putNext();
          } catch (Throwable t) {
            listener.error(t);
          } finally {
            listener.completed();
          }
        });
  }

  @AfterAll
  public static void tearDownClass() {
    AutoCloseables.closeNoChecked(FLIGHT_SQL_PRODUCER);
  }

  @BeforeEach
  public void setUp() {
    tokenEndpoint = oauthServer.url("/oauth/token").uri();
  }

  @AfterEach
  public void tearDown() {
    oauthServer.close();
  }

  // Helper methods for mock OAuth responses

  private void enqueueSuccessfulTokenResponse() {
    enqueueSuccessfulTokenResponse(VALID_ACCESS_TOKEN, 3600);
  }

  private void enqueueSuccessfulTokenResponse(String token, int expiresIn) {
    String body =
        String.format(
            "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":%d}",
            token, expiresIn);
    oauthServer.enqueue(
        new MockResponse.Builder()
            .code(200)
            .setHeader("Content-Type", "application/json")
            .body(body)
            .build());
  }

  private void enqueueErrorResponse(String error, String description) {
    String body =
        String.format("{\"error\":\"%s\",\"error_description\":\"%s\"}", error, description);
    oauthServer.enqueue(
        new MockResponse.Builder()
            .code(400)
            .setHeader("Content-Type", "application/json")
            .body(body)
            .build());
  }

  private Properties createBaseProperties() {
    Properties props = new Properties();
    props.put(ArrowFlightConnectionProperty.HOST.camelName(), "localhost");
    props.put(
        ArrowFlightConnectionProperty.PORT.camelName(), FLIGHT_SERVER_TEST_EXTENSION.getPort());
    props.put(ArrowFlightConnectionProperty.USE_ENCRYPTION.camelName(), false);
    return props;
  }

  private String getJdbcUrl() {
    return String.format(
        "jdbc:arrow-flight-sql://localhost:%d", FLIGHT_SERVER_TEST_EXTENSION.getPort());
  }

  // ==================== Client Credentials Flow Tests ====================

  @Test
  public void testClientCredentialsFlowSuccess() throws Exception {
    enqueueSuccessfulTokenResponse();

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "client_credentials");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), CLIENT_ID);
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), CLIENT_SECRET);
    props.put(ArrowFlightConnectionProperty.OAUTH_SCOPE.camelName(), TEST_SCOPE);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      assertFalse(conn.isClosed());
      // Trigger a Flight call to force OAuth token retrieval
      conn.getMetaData().getCatalogs().close();
    }

    // Verify OAuth request was made
    RecordedRequest oauthRequest = oauthServer.takeRequest(5, TimeUnit.SECONDS);
    assertNotNull(oauthRequest, "OAuth request should have been made");
    assertEquals("POST", oauthRequest.getMethod());
    String body = oauthRequest.getBody().utf8();
    assertTrue(body.contains("grant_type=client_credentials"));
    assertTrue(body.contains("scope=" + TEST_SCOPE));
  }

  @Test
  public void testClientCredentialsFlowWithUrlParameters() throws Exception {
    enqueueSuccessfulTokenResponse();

    String url =
        String.format(
            "jdbc:arrow-flight-sql://localhost:%d?useEncryption=false"
                + "&oauth.flow=client_credentials"
                + "&oauth.tokenUri=%s"
                + "&oauth.clientId=%s"
                + "&oauth.clientSecret=%s",
            FLIGHT_SERVER_TEST_EXTENSION.getPort(),
            tokenEndpoint.toString(),
            CLIENT_ID,
            CLIENT_SECRET);

    try (Connection conn = DriverManager.getConnection(url)) {
      conn.getMetaData().getCatalogs().close();
    }

    assertEquals(1, oauthServer.getRequestCount());
  }

  @Test
  public void testClientCredentialsFlowInvalidCredentials() throws Exception {
    enqueueErrorResponse("invalid_client", "Client authentication failed");

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "client_credentials");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), "wrong-client");
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), "wrong-secret");

    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
                conn.getMetaData().getCatalogs().close();
              }
            });
    // Verify the error message contains the OAuth error somewhere in the exception chain
    assertTrue(
        containsInExceptionChain(ex, "invalid_client"),
        "Exception chain should contain 'invalid_client'");
  }

  private boolean containsInExceptionChain(Throwable t, String message) {
    while (t != null) {
      if (t.getMessage() != null && t.getMessage().contains(message)) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  // ==================== Token Exchange Flow Tests ====================

  @Test
  public void testTokenExchangeFlowMinimalParameters() throws Exception {
    enqueueSuccessfulTokenResponse();

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "token_exchange");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN.camelName(), SUBJECT_TOKEN);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.camelName(),
        SUBJECT_TOKEN_TYPE);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      conn.getMetaData().getCatalogs().close();
    }

    RecordedRequest oauthRequest = oauthServer.takeRequest(5, TimeUnit.SECONDS);
    assertNotNull(oauthRequest, "OAuth request should have been made");
    String body = oauthRequest.getBody().utf8();
    assertTrue(
        body.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"),
        "Should contain token exchange grant type");
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
  }

  @Test
  public void testTokenExchangeFlowWithAllParameters() throws Exception {
    enqueueSuccessfulTokenResponse();

    String actorToken = "actor-token-value";
    String actorTokenType = "urn:ietf:params:oauth:token-type:access_token";
    String audience = "https://api.example.com";
    String resource = "https://api.example.com/resource";
    String requestedTokenType = "urn:ietf:params:oauth:token-type:access_token";

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "token_exchange");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), CLIENT_ID);
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), CLIENT_SECRET);
    props.put(ArrowFlightConnectionProperty.OAUTH_SCOPE.camelName(), TEST_SCOPE);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN.camelName(), SUBJECT_TOKEN);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.camelName(),
        SUBJECT_TOKEN_TYPE);
    props.put(ArrowFlightConnectionProperty.OAUTH_EXCHANGE_ACTOR_TOKEN.camelName(), actorToken);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE.camelName(), actorTokenType);
    props.put(ArrowFlightConnectionProperty.OAUTH_EXCHANGE_AUDIENCE.camelName(), audience);
    props.put(ArrowFlightConnectionProperty.OAUTH_RESOURCE.camelName(), resource);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE.camelName(),
        requestedTokenType);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      conn.getMetaData().getCatalogs().close();
    }

    RecordedRequest oauthRequest = oauthServer.takeRequest(5, TimeUnit.SECONDS);
    assertNotNull(oauthRequest, "OAuth request should have been made");
    String body = oauthRequest.getBody().utf8();
    assertTrue(body.contains("subject_token=" + SUBJECT_TOKEN));
    assertTrue(body.contains("actor_token=" + actorToken));
  }

  @Test
  public void testTokenExchangeFlowWithClientAuthentication() throws Exception {
    enqueueSuccessfulTokenResponse();

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "token_exchange");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), CLIENT_ID);
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), CLIENT_SECRET);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN.camelName(), SUBJECT_TOKEN);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.camelName(),
        SUBJECT_TOKEN_TYPE);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      conn.getMetaData().getCatalogs().close();
    }

    RecordedRequest oauthRequest = oauthServer.takeRequest(5, TimeUnit.SECONDS);
    assertNotNull(oauthRequest, "OAuth request should have been made");
    String authHeader = oauthRequest.getHeaders().get("Authorization");
    assertNotNull(authHeader, "Should have Basic auth header for client authentication");
    assertTrue(authHeader.startsWith("Basic "));
  }

  // ==================== Token Caching Tests ====================

  @Test
  public void testTokenCachingAcrossMultipleOperations() throws Exception {
    enqueueSuccessfulTokenResponse();

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "client_credentials");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), CLIENT_ID);
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), CLIENT_SECRET);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      // Execute multiple operations
      conn.isValid(5);
      conn.getMetaData().getCatalogs().close();
      conn.getMetaData().getSchemas().close();
    }

    // Should only have made one OAuth request due to caching
    assertEquals(1, oauthServer.getRequestCount());
  }

  @Test
  public void testTokenRefreshAfterExpiration() throws Exception {
    enqueueSuccessfulTokenResponse(VALID_ACCESS_TOKEN, 1);
    enqueueSuccessfulTokenResponse(VALID_ACCESS_TOKEN, 3600);

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "token_exchange");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN.camelName(), SUBJECT_TOKEN);
    props.put(
        ArrowFlightConnectionProperty.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE.camelName(),
        SUBJECT_TOKEN_TYPE);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      // First operation triggers initial token fetch
      conn.getMetaData().getCatalogs().close();

      // Token with 1s expiry is immediately considered expired (due to 30s buffer)
      // so the next operation should trigger a refresh
      conn.getMetaData().getCatalogs().close();
    }

    // Should have made exactly 2 OAuth requests: initial + refresh
    assertEquals(2, oauthServer.getRequestCount());
  }

  // ==================== Error Handling Tests ====================

  @Test
  public void testMissingRequiredParametersClientCredentials() {
    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "client_credentials");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    // Missing client_id and client_secret

    assertThrows(SQLException.class, () -> DriverManager.getConnection(getJdbcUrl(), props));
  }

  @Test
  public void testMissingRequiredParametersTokenExchange() {
    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "token_exchange");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    // Missing subject_token and subject_token_type

    assertThrows(SQLException.class, () -> DriverManager.getConnection(getJdbcUrl(), props));
  }

  @Test
  public void testInvalidOAuthFlow() {
    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "invalid_flow");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());

    assertThrows(SQLException.class, () -> DriverManager.getConnection(getJdbcUrl(), props));
  }

  @Test
  public void testMalformedTokenEndpoint() {
    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "client_credentials");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), "not-a-valid-uri://");
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), CLIENT_ID);
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), CLIENT_SECRET);

    assertThrows(SQLException.class, () -> DriverManager.getConnection(getJdbcUrl(), props));
  }

  // ==================== Authorization Header Verification ====================

  @Test
  public void testOAuthTokenSentAsBearer() throws Exception {
    enqueueSuccessfulTokenResponse();

    Properties props = createBaseProperties();
    props.put(ArrowFlightConnectionProperty.OAUTH_FLOW.camelName(), "client_credentials");
    props.put(ArrowFlightConnectionProperty.OAUTH_TOKEN_URI.camelName(), tokenEndpoint.toString());
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_ID.camelName(), CLIENT_ID);
    props.put(ArrowFlightConnectionProperty.OAUTH_CLIENT_SECRET.camelName(), CLIENT_SECRET);

    try (Connection conn = DriverManager.getConnection(getJdbcUrl(), props)) {
      conn.getMetaData().getCatalogs().close();
    }

    // Verify the Flight server received the bearer token
    String authHeader =
        FLIGHT_SERVER_TEST_EXTENSION
            .getInterceptorFactory()
            .getHeader(org.apache.arrow.flight.FlightMethod.GET_FLIGHT_INFO, "authorization");
    assertNotNull(authHeader, "Authorization header should be present in Flight requests");
    assertEquals("Bearer " + VALID_ACCESS_TOKEN, authHeader);
  }
}
