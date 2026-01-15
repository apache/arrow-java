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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example demonstrating OAuth 2.0 Client Credentials flow with the Arrow Flight SQL JDBC driver.
 *
 * <p>The Client Credentials flow is used for service-to-service authentication where no user
 * interaction is required. The application authenticates using its own credentials (client ID and
 * client secret) to obtain an access token from an OAuth 2.0 authorization server.
 *
 * <h2>Configuration</h2>
 *
 * <p>Set the following environment variables before running:
 *
 * <ul>
 *   <li>{@code FLIGHT_SQL_HOST} - Flight SQL server hostname (default: localhost)
 *   <li>{@code FLIGHT_SQL_PORT} - Flight SQL server port (default: 31337)
 *   <li>{@code OAUTH_TOKEN_URI} - OAuth 2.0 token endpoint URL (required)
 *   <li>{@code OAUTH_CLIENT_ID} - OAuth 2.0 client ID (required)
 *   <li>{@code OAUTH_CLIENT_SECRET} - OAuth 2.0 client secret (required)
 *   <li>{@code OAUTH_SCOPE} - OAuth 2.0 scope (optional)
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * export OAUTH_TOKEN_URI="https://auth.example.com/oauth/token"
 * export OAUTH_CLIENT_ID="my-client-id"
 * export OAUTH_CLIENT_SECRET="my-client-secret"
 * export FLIGHT_SQL_HOST="flight-sql.example.com"
 * export FLIGHT_SQL_PORT="443"
 *
 * java --add-opens=java.base/java.nio=ALL-UNNAMED \
 *      -cp flight-sql-jdbc-driver.jar \
 *      org.apache.arrow.driver.jdbc.example.ClientCredentialsOAuthExample
 * }</pre>
 */
public class ClientCredentialsOAuthExample {

  public static void main(String[] args) {
    // Read configuration from environment variables
    String host = getEnvOrDefault("FLIGHT_SQL_HOST", "localhost");
    String port = getEnvOrDefault("FLIGHT_SQL_PORT", "32010");
    String tokenUri = getEnvOrDefault("OAUTH_TOKEN_URI", "http://localhost:9047/oauth/token");
    String clientId = getRequiredEnv("OAUTH_CLIENT_ID");
    String clientSecret = getRequiredEnv("OAUTH_CLIENT_SECRET");
    String scope = getEnvOrDefault("OAUTH_SCOPE", "dremio.all offline_access");

    System.out.println("=== Arrow Flight SQL JDBC - OAuth Client Credentials Example ===");
    System.out.println("Connecting to: " + host + ":" + port);
    System.out.println("Token URI: " + tokenUri);
    System.out.println("Client ID: " + clientId);
    System.out.println();

    // Build JDBC connection URL
    String jdbcUrl = String.format("jdbc:arrow-flight-sql://%s:%s", host, port);

    // Configure OAuth properties
    Properties properties = new Properties();
    properties.setProperty("oauth.flow", "client_credentials");
    properties.setProperty("oauth.tokenUri", tokenUri);
    properties.setProperty("oauth.clientId", clientId);
    properties.setProperty("oauth.clientSecret", clientSecret);
    if (scope != null && !scope.isEmpty()) {
      properties.setProperty("oauth.scope", scope);
    }
    // Use encryption by default for production
    properties.setProperty("useEncryption", "false");

    try (Connection connection = DriverManager.getConnection(jdbcUrl, properties)) {
      System.out.println("Successfully connected with OAuth authentication!");
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
