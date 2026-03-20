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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Minimal sample app for using the Arrow Flight SQL JDBC driver.
 *
 * <p>Defaults are configured for a local Dremio instance:
 *
 * <ul>
 *   <li>host: {@code localhost}
 *   <li>port: {@code 32010}
 *   <li>user: {@code dremio}
 *   <li>password: {@code dremio123}
 * </ul>
 *
 * <p>Arguments are optional and positional:
 *
 * <pre>
 *   [host] [port] [user] [password] [selectSql] [updateSql]
 * </pre>
 *
 * <p>If {@code updateSql} is omitted, only {@code Statement.executeQuery(...)} is executed.
 */
public final class ArrowFlightJdbcSampleApp {
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 32010;
  private static final String DEFAULT_USER = "dremio";
  private static final String DEFAULT_PASSWORD = "dremio123";
  private static final String DEFAULT_SELECT_SQL = "SELECT 1 AS sample_value";

  private ArrowFlightJdbcSampleApp() {}

  public static void main(final String[] args) throws Exception {
    final String host = getArg(args, 0, DEFAULT_HOST);
    final int port = Integer.parseInt(getArg(args, 1, Integer.toString(DEFAULT_PORT)));
    final String user = getArg(args, 2, DEFAULT_USER);
    final String password = getArg(args, 3, DEFAULT_PASSWORD);
    final String selectSql = getArg(args, 4, DEFAULT_SELECT_SQL);
    final String updateSql = getArg(args, 5, "");

    final String url = String.format("jdbc:arrow-flight-sql://%s:%d", host, port);
    final Properties properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    properties.setProperty("useEncryption", "false");

    System.out.println("Connecting to " + url);
    try (Connection connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement()) {
      runSelect(statement, selectSql);

      if (updateSql.isEmpty()) {
        System.out.println(
            "Skipping Statement.executeUpdate(...) because no updateSql argument was provided.");
      } else {
        runUpdate(statement, updateSql);
      }
    }
  }

  private static void runSelect(final Statement statement, final String selectSql)
      throws SQLException {
    System.out.println("Running Statement.executeQuery: " + selectSql);
    try (ResultSet resultSet = statement.executeQuery(selectSql)) {
      final ResultSetMetaData metadata = resultSet.getMetaData();
      final int columnCount = metadata.getColumnCount();
      int rowCount = 0;
      while (resultSet.next()) {
        rowCount++;
        final StringBuilder rowBuilder = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
          if (i > 1) {
            rowBuilder.append(", ");
          }
          rowBuilder.append(metadata.getColumnLabel(i)).append('=').append(resultSet.getObject(i));
        }
        System.out.println("row " + rowCount + ": " + rowBuilder);
      }
      System.out.println("Statement.executeQuery returned " + rowCount + " row(s)");
    }
  }

  private static void runUpdate(final Statement statement, final String updateSql)
      throws SQLException {
    System.out.println("Running Statement.executeUpdate: " + updateSql);
    final int updateCount = statement.executeUpdate(updateSql);
    System.out.println("Statement.executeUpdate affected " + updateCount + " row(s)");
  }

  private static String getArg(final String[] args, final int index, final String defaultValue) {
    if (index >= args.length) {
      return defaultValue;
    }
    final String arg = args[index];
    return arg == null || arg.isEmpty() ? defaultValue : arg;
  }
}
