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
package org.apache.arrow.driver.jdbc.example;

import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example demonstrating OAuth 2.0 Token Exchange flow (RFC 8693) with the Arrow Flight SQL JDBC
 * driver.
 *
 * <p>Token Exchange is used to exchange one token for another, commonly used for:
 *
 * <ul>
 *   <li>Federated authentication - exchanging an external IdP token for a service token
 *   <li>Delegation - obtaining a token to act on behalf of a user
 *   <li>Impersonation - obtaining a token to act as another user
 * </ul>
 *
 * <h2>Configuration</h2>
 *
 * <p>Set the following environment variables before running:
 *
 * <ul>
 *   <li>{@code FLIGHT_SQL_HOST} - Flight SQL server hostname (default: localhost)
 *   <li>{@code FLIGHT_SQL_PORT} - Flight SQL server port (default: 31337)
 *   <li>{@code OAUTH_TOKEN_URI} - OAuth 2.0 token endpoint URL (required)
 *   <li>{@code OAUTH_SUBJECT_TOKEN} - The subject token to exchange (required)
 *   <li>{@code OAUTH_SUBJECT_TOKEN_TYPE} - Subject token type URI (required, e.g.,
 *       "urn:ietf:params:oauth:token-type:access_token" or "urn:ietf:params:oauth:token-type:jwt")
 *   <li>{@code OAUTH_CLIENT_ID} - OAuth 2.0 client ID (optional, some servers require this)
 *   <li>{@code OAUTH_CLIENT_SECRET} - OAuth 2.0 client secret (optional, required if client ID is
 *       set)
 *   <li>{@code OAUTH_AUDIENCE} - Target audience for the exchanged token (optional)
 *   <li>{@code OAUTH_SCOPE} - OAuth 2.0 scope (optional)
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * # Get a token from your identity provider first
 * export OAUTH_SUBJECT_TOKEN="eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
 * export OAUTH_SUBJECT_TOKEN_TYPE="urn:ietf:params:oauth:token-type:jwt"
 * export OAUTH_TOKEN_URI="https://auth.example.com/oauth/token"
 * export OAUTH_AUDIENCE="flight-sql-service"
 * export FLIGHT_SQL_HOST="flight-sql.example.com"
 *
 * # Optional: If your OAuth server requires client authentication for token exchange
 * export OAUTH_CLIENT_ID="my-client-id"
 * export OAUTH_CLIENT_SECRET="my-client-secret"
 *
 * java --add-opens=java.base/java.nio=ALL-UNNAMED \
 *      -cp flight-sql-jdbc-driver.jar \
 *      org.apache.arrow.driver.jdbc.example.TokenExchangeOAuthExample
 * }</pre>
 */
public class TokenExchangeOAuthExample {

  public static void main(String[] args) {
    // Read configuration from environment variables
    String host = getEnvOrDefault("FLIGHT_SQL_HOST", "localhost");
    String port = getEnvOrDefault("FLIGHT_SQL_PORT", "32010");
    String tokenUri = getRequiredEnv("OAUTH_TOKEN_URI");
    String subjectToken = getRequiredEnv("OAUTH_SUBJECT_TOKEN");

    String subjectTokenType = getEnvOrDefault("OAUTH_SUBJECT_TOKEN_TYPE", TokenTypeURI.JWT.toString());
    // Optional: client credentials (some OAuth servers require this for token exchange)
    String clientId = System.getenv("OAUTH_CLIENT_ID");
    String clientSecret = System.getenv("OAUTH_CLIENT_SECRET");
    String audience = System.getenv("OAUTH_AUDIENCE");
    String scope = "dremio.all offline_access";//System.getenv("OAUTH_SCOPE");

    System.out.println("=== Arrow Flight SQL JDBC - OAuth Token Exchange Example ===");
    System.out.println("Connecting to: " + host + ":" + port);
    System.out.println("Token URI: " + tokenUri);
    System.out.println("Subject Token: " + maskToken(subjectToken));
    System.out.println("Subject Token Type: " + subjectTokenType);
    if (clientId != null) {
      System.out.println("Client ID: " + clientId);
    }
    if (audience != null) {
      System.out.println("Audience: " + audience);
    }
    System.out.println();

    // Build JDBC connection URL
    String jdbcUrl = String.format("jdbc:arrow-flight-sql://%s:%s", host, port);

    // Configure OAuth properties
    Properties properties = new Properties();
    properties.setProperty("oauth.flow", "token_exchange");
    properties.setProperty("oauth.tokenUri", tokenUri);
    properties.setProperty("oauth.exchange.subjectToken", subjectToken);
    properties.setProperty("oauth.exchange.subjectTokenType", subjectTokenType);
    // Optional: client credentials for servers that require authentication
    if (clientId != null && !clientId.isEmpty()) {
      properties.setProperty("oauth.clientId", clientId);
    }
    if (clientSecret != null && !clientSecret.isEmpty()) {
      properties.setProperty("oauth.clientSecret", clientSecret);
    }
    if (audience != null && !audience.isEmpty()) {
      properties.setProperty("oauth.exchange.audience", audience);
    }
    if (scope != null && !scope.isEmpty()) {
      properties.setProperty("oauth.scope", scope);
    }
    // Use encryption by default for production
    properties.setProperty("useEncryption", "false");

    try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
      System.out.println("Successfully connected with OAuth Token Exchange authentication!");
      System.out.println();

      // Execute a sample query
      executeQuery(connection, "SELECT 1 AS test_value");

      // Get database metadata
      System.out.println("Database Product: " + connection.getMetaData().getDatabaseProductName());
      System.out.println(
          "Database Version: " + connection.getMetaData().getDatabaseProductVersion());

    } catch (SQLException e) {
      System.err.println("Connection failed: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void executeQuery(Connection connection, String sql) throws SQLException {
    System.out.println("Executing query: " + sql);
    System.out.println("-".repeat(50));

    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {

      int columnCount = resultSet.getMetaData().getColumnCount();

      // Print column headers
      for (int i = 1; i <= columnCount; i++) {
        System.out.print(resultSet.getMetaData().getColumnName(i));
        if (i < columnCount) {
          System.out.print("\t");
        }
      }
      System.out.println();

      // Print rows
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print("\t");
          }
        }
        System.out.println();
      }
    }
    System.out.println();
  }

  private static String maskToken(String token) {
    if (token == null || token.length() < 10) {
      return "***";
    }
    return token.substring(0, 5) + "..." + token.substring(token.length() - 5);
  }

  private static String getEnvOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  private static String getRequiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      System.err.println("Error: Required environment variable " + name + " is not set.");
      System.exit(1);
    }
    return value;
  }
}
