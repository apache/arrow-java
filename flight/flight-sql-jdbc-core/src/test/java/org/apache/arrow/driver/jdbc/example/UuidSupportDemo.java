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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

/**
 * Demonstrates UUID support in the Arrow Flight JDBC driver.
 *
 * <p>This example connects to a Flight SQL server and comprehensively tests UUID support using both
 * Statement and PreparedStatement JDBC primitives for all DML operations (INSERT, UPDATE, DELETE,
 * SELECT).
 *
 * <h2>Prerequisites</h2>
 *
 * <ul>
 *   <li>A Flight SQL server running at localhost:32010 (e.g., Dremio)
 *   <li>A table created with: {@code CREATE TABLE nas.uuid_table (id INT, uuid UUID)}
 *   <li>Valid credentials (username: dremio, password: dremio123)
 * </ul>
 *
 * <h2>Building and Running the Demo</h2>
 *
 * <p><b>Important:</b> Arrow's memory management requires access to Java's internal NIO classes.
 * The JVM must be started with the following option:
 *
 * <pre>{@code
 * --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
 * }</pre>
 *
 * <p>From the arrow-java root directory:
 *
 * <pre>{@code
 * # Step 1: Build the project (includes main and test classes)
 * mvn install -DskipTests -pl flight/flight-sql-jdbc-core -am
 *
 * # Step 2: Compile test classes (required since demo is in src/test/java)
 * mvn test-compile -pl flight/flight-sql-jdbc-core
 *
 * # Step 3: Run the demo using Maven
 * MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
 *   mvn exec:java -pl flight/flight-sql-jdbc-core \
 *   -Dexec.mainClass="org.apache.arrow.driver.jdbc.example.UuidSupportDemo" \
 *   -Dexec.classpathScope=test
 * }</pre>
 *
 * <p>Or as a single command:
 *
 * <pre>{@code
 * mvn install -DskipTests -pl flight/flight-sql-jdbc-core -am && \
 *   mvn test-compile -pl flight/flight-sql-jdbc-core && \
 *   MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
 *   mvn exec:java -pl flight/flight-sql-jdbc-core \
 *   -Dexec.mainClass="org.apache.arrow.driver.jdbc.example.UuidSupportDemo" \
 *   -Dexec.classpathScope=test
 * }</pre>
 *
 * <h2>Known Limitations</h2>
 *
 * <p><b>UUID Parameter Binding:</b> When using PreparedStatement with UUID parameters, use {@code
 * setString(index, uuid.toString())} instead of {@code setObject(index, uuid)}. This is because
 * Flight SQL servers typically don't include UUID extension type information in the parameter
 * schema, so the driver cannot determine that the parameter should be a UUID. The server (e.g.,
 * Dremio) will implicitly cast the UUID string to the UUID type.
 */
public class UuidSupportDemo {

  private static final String JDBC_URL = "jdbc:arrow-flight-sql://<flight_sql_server_host>:<flight_sql_server_port>";
  private static final String USERNAME = "";
  private static final String PASSWORD = "";

  // Table schema: CREATE TABLE nas.uuid_table (id INT, uuid UUID)
  private static final String TABLE_NAME = "nas.uuid_table";

  // Well-known test UUIDs for consistent testing
  private static final UUID UUID_1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
  private static final UUID UUID_2 = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
  private static final UUID UUID_3 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
  private static final UUID UUID_4 = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
  private static final UUID UUID_5 = UUID.fromString("00000000-0000-0000-0000-000000000000");

  public static void main(String[] args) {
    System.out.println("=== Arrow Flight JDBC UUID Support Demo ===\n");

    try {
      Class.forName(ArrowFlightJdbcDriver.class.getName());
    } catch (ClassNotFoundException e) {
      System.err.println("Arrow Flight JDBC driver not found on classpath");
      e.printStackTrace();
      return;
    }

    Properties properties = new Properties();
    properties.put("user", USERNAME);
    properties.put("password", PASSWORD);
    properties.put("useEncryption", "false");

    try (Connection connection = DriverManager.getConnection(JDBC_URL, properties)) {
      // Run all demo sections
      demonstrateInsertWithStatement(connection);
      demonstrateInsertWithPreparedStatement(connection);
      demonstrateSelectWithStatement(connection);
      demonstrateSelectWithPreparedStatement(connection);
      demonstrateUpdateWithStatement(connection);
      demonstrateUpdateWithPreparedStatement(connection);
      demonstrateDeleteWithStatement(connection);
      demonstrateDeleteWithPreparedStatement(connection);
      demonstrateNullUuidHandling(connection);
      demonstrateBatchOperations(connection);
      demonstrateUuidComparisons(connection);
      demonstrateUuidRetrieval(connection);

    } catch (SQLException e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  // ==================== INSERT Operations ====================

  private static void demonstrateInsertWithStatement(Connection connection) throws SQLException {
    System.out.println("=== INSERT with Statement ===\n");

    try (Statement stmt = connection.createStatement()) {
      // Insert single row with UUID literal
      String sql1 =
          String.format(
              "INSERT INTO %s (id, uuid) VALUES (1000, UUID '%s')", TABLE_NAME, UUID_1.toString());
      int rows1 = stmt.executeUpdate(sql1);
      System.out.println("Inserted row with UUID literal: " + rows1 + " row(s) affected");
      System.out.println("  ID: 1000, UUID: " + UUID_1);

      // Insert row with NULL UUID
      String sql2 = String.format("INSERT INTO %s (id, uuid) VALUES (1001, NULL)", TABLE_NAME);
      int rows2 = stmt.executeUpdate(sql2);
      System.out.println("Inserted row with NULL UUID: " + rows2 + " row(s) affected");
      System.out.println("  ID: 1001, UUID: NULL");

      // Insert multiple rows in single statement (if supported)
      String sql3 =
          String.format(
              "INSERT INTO %s (id, uuid) VALUES (1002, UUID '%s')", TABLE_NAME, UUID_2.toString());
      int rows3 = stmt.executeUpdate(sql3);
      System.out.println("Inserted additional row: " + rows3 + " row(s) affected");
      System.out.println("  ID: 1002, UUID: " + UUID_2);
    }
    System.out.println();
  }

  private static void demonstrateInsertWithPreparedStatement(Connection connection)
      throws SQLException {
    System.out.println("=== INSERT with PreparedStatement ===\n");

    String insertSql = String.format("INSERT INTO %s (id, uuid) VALUES (?, ?)", TABLE_NAME);

    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      // Insert using setObject() with UUID
      pstmt.setInt(1, 2000);
      pstmt.setObject(2, UUID_3);
      int rows1 = pstmt.executeUpdate();
      System.out.println("Insert with setObject(UUID): " + rows1 + " row(s) affected");
      System.out.println("  ID: 2000, UUID: " + UUID_3);

      // Insert using setString() with UUID string representation
      pstmt.setInt(1, 2001);
      pstmt.setString(2, UUID_4.toString());
      int rows2 = pstmt.executeUpdate();
      System.out.println("Insert with setString(UUID.toString()): " + rows2 + " row(s) affected");
      System.out.println("  ID: 2001, UUID: " + UUID_4);

      // Insert NULL UUID using setNull()
      pstmt.setInt(1, 2002);
      pstmt.setNull(2, Types.OTHER);
      int rows3 = pstmt.executeUpdate();
      System.out.println("Insert with setNull(): " + rows3 + " row(s) affected");
      System.out.println("  ID: 2002, UUID: NULL");

      // Insert NULL UUID using setObject(null)
      pstmt.setInt(1, 2003);
      pstmt.setObject(2, null);
      int rows4 = pstmt.executeUpdate();
      System.out.println("Insert with setObject(null): " + rows4 + " row(s) affected");
      System.out.println("  ID: 2003, UUID: NULL");

      // Insert zero UUID (all zeros)
      pstmt.setInt(1, 2004);
      pstmt.setObject(2, UUID_5);
      int rows5 = pstmt.executeUpdate();
      System.out.println("Insert with zero UUID: " + rows5 + " row(s) affected");
      System.out.println("  ID: 2004, UUID: " + UUID_5);
    }
    System.out.println();
  }

  // ==================== SELECT Operations ====================

  private static void demonstrateSelectWithStatement(Connection connection) throws SQLException {
    System.out.println("=== SELECT with Statement ===\n");

    try (Statement stmt = connection.createStatement()) {
      // Select all rows
      String sql1 = String.format("SELECT id, uuid FROM %s ORDER BY id LIMIT 10", TABLE_NAME);
      try (ResultSet rs = stmt.executeQuery(sql1)) {
        System.out.println("SELECT all rows (limited to 10):");
        printResultSet(rs);
      }

      // Select with UUID equality condition
      String sql2 =
          String.format(
              "SELECT id, uuid FROM %s WHERE uuid = UUID '%s'", TABLE_NAME, UUID_1.toString());
      try (ResultSet rs = stmt.executeQuery(sql2)) {
        System.out.println("SELECT with UUID equality (uuid = " + UUID_1 + "):");
        printResultSet(rs);
      }

      // Select NULL UUIDs
      String sql3 = String.format("SELECT id, uuid FROM %s WHERE uuid IS NULL", TABLE_NAME);
      try (ResultSet rs = stmt.executeQuery(sql3)) {
        System.out.println("SELECT rows with NULL UUID:");
        printResultSet(rs);
      }

      // Select non-NULL UUIDs
      String sql4 =
          String.format("SELECT id, uuid FROM %s WHERE uuid IS NOT NULL LIMIT 5", TABLE_NAME);
      try (ResultSet rs = stmt.executeQuery(sql4)) {
        System.out.println("SELECT rows with non-NULL UUID (limited to 5):");
        printResultSet(rs);
      }
    }
    System.out.println();
  }

  private static void demonstrateSelectWithPreparedStatement(Connection connection)
      throws SQLException {
    System.out.println("=== SELECT with PreparedStatement ===\n");

    // Select by UUID using setObject()
    String sql1 = String.format("SELECT id, uuid FROM %s WHERE uuid = ?", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
      pstmt.setObject(1, UUID_1);
      try (ResultSet rs = pstmt.executeQuery()) {
        System.out.println("SELECT with setObject(UUID) parameter:");
        printResultSet(rs);
      }
    }

    // Select by UUID using setString()
    try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
      pstmt.setString(1, UUID_3.toString());
      try (ResultSet rs = pstmt.executeQuery()) {
        System.out.println("SELECT with setString(UUID.toString()) parameter:");
        printResultSet(rs);
      }
    }

    // Select by ID range and retrieve UUID
    String sql2 =
        String.format("SELECT id, uuid FROM %s WHERE id >= ? AND id <= ? ORDER BY id", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(sql2)) {
      pstmt.setInt(1, 2000);
      pstmt.setInt(2, 2004);
      try (ResultSet rs = pstmt.executeQuery()) {
        System.out.println("SELECT by ID range (2000-2004):");
        printResultSet(rs);
      }
    }
    System.out.println();
  }

  // ==================== UPDATE Operations ====================

  private static void demonstrateUpdateWithStatement(Connection connection) throws SQLException {
    System.out.println("=== UPDATE with Statement ===\n");

    try (Statement stmt = connection.createStatement()) {
      // Update UUID value by ID
      String sql1 =
          String.format(
              "UPDATE %s SET uuid = UUID '%s' WHERE id = 1000", TABLE_NAME, UUID_4.toString());
      int rows1 = stmt.executeUpdate(sql1);
      System.out.println("Update UUID by ID: " + rows1 + " row(s) affected");
      System.out.println("  Updated ID 1000 to UUID: " + UUID_4);

      // Update UUID to NULL
            String sql2 = String.format("UPDATE %s SET uuid = NULL WHERE id = 1002", TABLE_NAME);
            int rows2 = stmt.executeUpdate(sql2);
            System.out.println("Update UUID to NULL: " + rows2 + " row(s) affected");
            System.out.println("  Updated ID 1002 to UUID: NULL");

      // Update based on UUID condition
      String sql3 =
          String.format(
              "UPDATE %s SET uuid = UUID '%s' WHERE uuid = UUID '%s'",
              TABLE_NAME, UUID_1.toString(), UUID_4.toString());
      int rows3 = stmt.executeUpdate(sql3);
      System.out.println("Update UUID based on UUID condition: " + rows3 + " row(s) affected");
    }
    System.out.println();
  }

  private static void demonstrateUpdateWithPreparedStatement(Connection connection)
      throws SQLException {
    System.out.println("=== UPDATE with PreparedStatement ===\n");

    // Update UUID using setObject()
    String sql1 = String.format("UPDATE %s SET uuid = ? WHERE id = 2001", TABLE_NAME);
        try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
          pstmt.setObject(1, UUID_2);
          pstmt.setInt(2, 2000);
          int rows1 = pstmt.executeUpdate();
          System.out.println("Update with setObject(UUID): " + rows1 + " row(s) affected");
          System.out.println("  Updated ID 2000 to UUID: " + UUID_2);
        }

//     Update UUID using setString()
        try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
          pstmt.setString(1, UUID_3.toString());
          pstmt.setInt(2, 2001);
          int rows2 = pstmt.executeUpdate();
          System.out.println("Update with setString(UUID.toString()): " + rows2 + " row(s) affected");
          System.out.println("  Updated ID 2001 to UUID: " + UUID_3);
        }

//     Update UUID to NULL using setNull()
        try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
          pstmt.setNull(1, Types.OTHER);
          pstmt.setInt(2, 2004);
          int rows3 = pstmt.executeUpdate();
          System.out.println("Update with setNull(): " + rows3 + " row(s) affected");
          System.out.println("  Updated ID 2004 to UUID: NULL");
        }

    // Update based on UUID parameter condition
    String sql2 = String.format("UPDATE %s SET uuid = ? WHERE uuid = ?", TABLE_NAME);
        try (PreparedStatement pstmt = connection.prepareStatement(sql2)) {
          pstmt.setObject(1, UUID_5);
          pstmt.setObject(2, UUID_2);
          int rows4 = pstmt.executeUpdate();
          System.out.println("Update UUID based on UUID condition: " + rows4 + " row(s) affected");
          System.out.println("  Changed UUID " + UUID_2 + " to " + UUID_5);
        }
    System.out.println();
  }

  // ==================== DELETE Operations ====================

  private static void demonstrateDeleteWithStatement(Connection connection) throws SQLException {
    System.out.println("=== DELETE with Statement ===\n");

    try (Statement stmt = connection.createStatement()) {
      // Delete by specific UUID
      String sql1 =
          String.format("DELETE FROM %s WHERE uuid = UUID '%s'", TABLE_NAME, UUID_5.toString());
      int rows1 = stmt.executeUpdate(sql1);
      System.out.println("Delete by UUID: " + rows1 + " row(s) affected");
      System.out.println("  Deleted rows with UUID: " + UUID_5);

      // Delete rows with NULL UUID
      String sql2 = String.format("DELETE FROM %s WHERE uuid IS NULL AND id >= 1000", TABLE_NAME);
      int rows2 = stmt.executeUpdate(sql2);
      System.out.println("Delete rows with NULL UUID (id >= 1000): " + rows2 + " row(s) affected");

      // Delete by ID (cleanup)
      String sql3 = String.format("DELETE FROM %s WHERE id = 1001", TABLE_NAME);
      int rows3 = stmt.executeUpdate(sql3);
      System.out.println("Delete by ID: " + rows3 + " row(s) affected");
    }
    System.out.println();
  }

  private static void demonstrateDeleteWithPreparedStatement(Connection connection)
      throws SQLException {
    System.out.println("=== DELETE with PreparedStatement ===\n");

    // Delete by UUID using setObject()
    String sql1 = String.format("DELETE FROM %s WHERE uuid = ?", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
      pstmt.setObject(1, UUID_3);
      int rows1 = pstmt.executeUpdate();
      System.out.println("Delete with setObject(UUID): " + rows1 + " row(s) affected");
      System.out.println("  Deleted rows with UUID: " + UUID_3);
    }

    // Delete by UUID using setString()
    try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
      pstmt.setString(1, UUID_1.toString());
      int rows2 = pstmt.executeUpdate();
      System.out.println("Delete with setString(UUID.toString()): " + rows2 + " row(s) affected");
      System.out.println("  Deleted rows with UUID: " + UUID_1);
    }

    // Delete by ID range (cleanup)
    String sql2 = String.format("DELETE FROM %s WHERE id >= ? AND id <= ?", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(sql2)) {
      pstmt.setInt(1, 1000);
      pstmt.setInt(2, 2999);
      int rows3 = pstmt.executeUpdate();
      System.out.println("Delete by ID range (cleanup): " + rows3 + " row(s) affected");
    }
    System.out.println();
  }

  // ==================== NULL UUID Handling ====================

  private static void demonstrateNullUuidHandling(Connection connection) throws SQLException {
    System.out.println("=== NULL UUID Handling ===\n");

    // Insert rows with NULL UUIDs for testing
    String insertSql = String.format("INSERT INTO %s (id, uuid) VALUES (?, ?)", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setInt(1, 3000);
      pstmt.setNull(2, Types.OTHER);
      pstmt.executeUpdate();

      pstmt.setInt(1, 3001);
      pstmt.setObject(2, null);
      pstmt.executeUpdate();
      System.out.println("Inserted 2 rows with NULL UUIDs (IDs: 3000, 3001)");
    }

    // Query and verify NULL handling
    String selectSql =
        String.format("SELECT id, uuid FROM %s WHERE id >= 3000 AND id <= 3001", TABLE_NAME);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectSql)) {
      System.out.println("\nRetrieving NULL UUID values:");
      while (rs.next()) {
        int id = rs.getInt("id");

        // Test getObject()
        Object uuidObj = rs.getObject("uuid");
        boolean wasNull1 = rs.wasNull();

        // Test getString()
        String uuidStr = rs.getString("uuid");
        boolean wasNull2 = rs.wasNull();

        // Test getObject(UUID.class)
        UUID uuid = rs.getObject("uuid", UUID.class);
        boolean wasNull3 = rs.wasNull();

        System.out.println("  ID: " + id);
        System.out.println("    getObject(): " + uuidObj + " (wasNull: " + wasNull1 + ")");
        System.out.println("    getString(): " + uuidStr + " (wasNull: " + wasNull2 + ")");
        System.out.println("    getObject(UUID.class): " + uuid + " (wasNull: " + wasNull3 + ")");
      }
    }

    // Cleanup
    String deleteSql = String.format("DELETE FROM %s WHERE id >= 3000 AND id <= 3001", TABLE_NAME);
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(deleteSql);
      System.out.println("\nCleaned up NULL UUID test rows");
    }
    System.out.println();
  }

  // ==================== Batch Operations ====================

  private static void demonstrateBatchOperations(Connection connection) throws SQLException {
    System.out.println("=== Batch Operations with PreparedStatement ===\n");

    String insertSql = String.format("INSERT INTO %s (id, uuid) VALUES (?, ?)", TABLE_NAME);

    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      // Add multiple rows to batch
      List<UUID> batchUuids = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        UUID uuid = UUID.randomUUID();
        batchUuids.add(uuid);
        pstmt.setInt(1, 4000 + i);
        pstmt.setObject(2, uuid);
        pstmt.addBatch();
        System.out.println("Added to batch: ID=" + (4000 + i) + ", UUID=" + uuid);
      }

      // Execute batch
      int[] results = pstmt.executeBatch();
      System.out.println("\nBatch execution results:");
      int totalAffected = 0;
      for (int i = 0; i < results.length; i++) {
        System.out.println("  Statement " + (i + 1) + ": " + results[i] + " row(s)");
        totalAffected += results[i];
      }
      System.out.println("Total rows affected: " + totalAffected);

      // Verify batch insert
      String selectSql =
          String.format(
              "SELECT id, uuid FROM %s WHERE id >= 4000 AND id <= 4004 ORDER BY id", TABLE_NAME);
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(selectSql)) {
        System.out.println("\nVerifying batch insert:");
        int idx = 0;
        while (rs.next()) {
          int id = rs.getInt("id");
          UUID retrievedUuid = rs.getObject("uuid", UUID.class);
          UUID expectedUuid = batchUuids.get(idx);
          boolean match = expectedUuid.equals(retrievedUuid);
          System.out.println(
              "  ID: " + id + ", UUID: " + retrievedUuid + " (match: " + match + ")");
          idx++;
        }
      }
    }

    // Cleanup batch test data
    String deleteSql = String.format("DELETE FROM %s WHERE id >= 4000 AND id <= 4004", TABLE_NAME);
    try (Statement stmt = connection.createStatement()) {
      int deleted = stmt.executeUpdate(deleteSql);
      System.out.println("\nCleaned up batch test rows: " + deleted + " row(s)");
    }
    System.out.println();
  }

  // ==================== UUID Comparisons ====================

  private static void demonstrateUuidComparisons(Connection connection) throws SQLException {
    System.out.println("=== UUID Comparisons in WHERE Clauses ===\n");

    // Insert test data
    String insertSql = String.format("INSERT INTO %s (id, uuid) VALUES (?, ?)", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setInt(1, 5000);
      pstmt.setObject(2, UUID_1);
      pstmt.executeUpdate();

      pstmt.setInt(1, 5001);
      pstmt.setObject(2, UUID_2);
      pstmt.executeUpdate();

      pstmt.setInt(1, 5002);
      pstmt.setObject(2, UUID_1); // Duplicate UUID
      pstmt.executeUpdate();

      pstmt.setInt(1, 5003);
      pstmt.setNull(2, Types.OTHER);
      pstmt.executeUpdate();
      System.out.println("Inserted test data for comparison tests");
    }

    // Equality comparison
    String sql1 = String.format("SELECT id, uuid FROM %s WHERE uuid = ?", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(sql1)) {
      pstmt.setObject(1, UUID_1);
      try (ResultSet rs = pstmt.executeQuery()) {
        System.out.println("\nEquality (uuid = " + UUID_1 + "):");
        printResultSet(rs);
      }
    }

    // Inequality comparison
    String sql2 =
        String.format(
            "SELECT id, uuid FROM %s WHERE uuid <> ? AND id >= 5000 AND id <= 5003", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(sql2)) {
      pstmt.setObject(1, UUID_1);
      try (ResultSet rs = pstmt.executeQuery()) {
        System.out.println("Inequality (uuid <> " + UUID_1 + "):");
        printResultSet(rs);
      }
    }

    // IS NULL comparison
    String sql3 =
        String.format(
            "SELECT id, uuid FROM %s WHERE uuid IS NULL AND id >= 5000 AND id <= 5003", TABLE_NAME);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql3)) {
      System.out.println("IS NULL comparison:");
      printResultSet(rs);
    }

    // IS NOT NULL comparison
    String sql4 =
        String.format(
            "SELECT id, uuid FROM %s WHERE uuid IS NOT NULL AND id >= 5000 AND id <= 5003",
            TABLE_NAME);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql4)) {
      System.out.println("IS NOT NULL comparison:");
      printResultSet(rs);
    }

    // IN clause with UUIDs
    String sql5 =
        String.format(
            "SELECT id, uuid FROM %s WHERE uuid IN (UUID '%s', UUID '%s') AND id >= 5000",
            TABLE_NAME, UUID_1.toString(), UUID_2.toString());
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql5)) {
      System.out.println("IN clause (uuid IN (" + UUID_1 + ", " + UUID_2 + ")):");
      printResultSet(rs);
    }

    // Cleanup
    String deleteSql = String.format("DELETE FROM %s WHERE id >= 5000 AND id <= 5003", TABLE_NAME);
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(deleteSql);
      System.out.println("Cleaned up comparison test rows");
    }
    System.out.println();
  }

  // ==================== UUID Retrieval Methods ====================

  private static void demonstrateUuidRetrieval(Connection connection) throws SQLException {
    System.out.println("=== UUID Retrieval Methods ===\n");

    // Insert test row
    String insertSql = String.format("INSERT INTO %s (id, uuid) VALUES (?, ?)", TABLE_NAME);
    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setInt(1, 6000);
      pstmt.setObject(2, UUID_1);
      pstmt.executeUpdate();
      System.out.println("Inserted test row: ID=6000, UUID=" + UUID_1);
    }

    // Demonstrate all retrieval methods
    String selectSql = String.format("SELECT id, uuid FROM %s WHERE id = 6000", TABLE_NAME);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectSql)) {
      if (rs.next()) {
        System.out.println("\nRetrieval methods for UUID column:");

        // Method 1: getObject() - returns java.util.UUID
        Object obj = rs.getObject("uuid");
        System.out.println("  getObject(): " + obj);
        System.out.println("    Type: " + (obj != null ? obj.getClass().getName() : "null"));

        // Method 2: getObject(UUID.class) - returns java.util.UUID
        UUID uuid = rs.getObject("uuid", UUID.class);
        System.out.println("  getObject(UUID.class): " + uuid);

        // Method 3: getString() - returns hyphenated string format
        String str = rs.getString("uuid");
        System.out.println("  getString(): " + str);

        // Method 4: getBytes() - returns 16-byte array
        byte[] bytes = rs.getBytes("uuid");
        System.out.println("  getBytes(): " + bytesToHex(bytes) + " (" + bytes.length + " bytes)");

        // Verify consistency
        System.out.println("\nConsistency checks:");
        System.out.println("  getObject() instanceof UUID: " + (obj instanceof UUID));
        System.out.println("  getObject().equals(UUID_1): " + UUID_1.equals(obj));
        System.out.println(
            "  UUID.fromString(getString()).equals(UUID_1): "
                + UUID_1.equals(UUID.fromString(str)));

        // Verify metadata
        ResultSetMetaData meta = rs.getMetaData();
        System.out.println("\nColumn metadata:");
        System.out.println("  Column name: " + meta.getColumnName(2));
        System.out.println(
            "  Column type: " + meta.getColumnType(2) + " (Types.OTHER=" + Types.OTHER + ")");
        System.out.println("  Column type name: " + meta.getColumnTypeName(2));
        System.out.println("  Column class name: " + meta.getColumnClassName(2));
      }
    }

    // Cleanup
    String deleteSql = String.format("DELETE FROM %s WHERE id = 6000", TABLE_NAME);
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(deleteSql);
      System.out.println("\nCleaned up retrieval test row");
    }
    System.out.println();
  }

  // ==================== Helper Methods ====================

  private static void printResultSet(ResultSet rs) throws SQLException {
    int count = 0;
    while (rs.next()) {
      count++;
      int id = rs.getInt("id");
      UUID uuid = rs.getObject("uuid", UUID.class);
      boolean wasNull = rs.wasNull();
      System.out.println("  Row " + count + ": ID=" + id + ", UUID=" + (wasNull ? "NULL" : uuid));
    }
    if (count == 0) {
      System.out.println("  (no rows)");
    }
    System.out.println();
  }

  private static String bytesToHex(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
