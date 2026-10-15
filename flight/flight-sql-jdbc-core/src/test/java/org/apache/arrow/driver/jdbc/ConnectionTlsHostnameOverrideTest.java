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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.FlightSqlTestCertificates;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.PartitionedFlightSqlProducer;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests the hostnameOverride connection property. The test certificate is issued for {@code
 * localhost} and has no IP address in its subject alternative names, so connecting by IP address
 * only passes verification when the hostname override is used.
 */
public class ConnectionTlsHostnameOverrideTest {

  private static final String IP_ADDRESS = "127.0.0.1";
  private static final String CERTIFICATE_HOSTNAME = "localhost";
  private static final String USER = "user1";
  private static final String PASSWORD = "pass1";

  @RegisterExtension public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;
  private static final FlightSqlTestCertificates.CertKeyPair certKey;
  private static final String tlsRootCertsPath;

  static {
    certKey = FlightSqlTestCertificates.exampleTlsCerts().get(0);
    tlsRootCertsPath = certKey.cert.getPath();

    FLIGHT_SERVER_TEST_EXTENSION =
        new FlightServerTestExtension.Builder()
            .authentication(new UserPasswordAuthentication.Builder().user(USER, PASSWORD).build())
            .useEncryption(certKey.cert, certKey.key)
            .producer(new MockFlightSqlProducer())
            .build();
  }

  @BeforeAll
  public static void registerDriver() throws SQLException {
    DriverManager.registerDriver(new ArrowFlightJdbcDriver());
  }

  private static String tlsUrl(final String host, final int port) throws Exception {
    return String.format(
        "jdbc:arrow-flight-sql://%s:%d?useEncryption=true&useSystemTrustStore=false&%s=%s",
        host,
        port,
        ArrowFlightConnectionProperty.TLS_ROOT_CERTS.camelName(),
        URLEncoder.encode(tlsRootCertsPath, "UTF-8"));
  }

  private static String serverUrl(final String host) throws Exception {
    return tlsUrl(host, FLIGHT_SERVER_TEST_EXTENSION.getPort())
        + String.format("&user=%s&password=%s", USER, PASSWORD);
  }

  @Test
  public void testConnectionByIpAddressFailsWithoutHostnameOverride() {
    assertThrows(
        SQLException.class,
        () -> {
          try (Connection connection = DriverManager.getConnection(serverUrl(IP_ADDRESS))) {
            connection.isValid(0);
          }
        });
  }

  @Test
  public void testConnectionByIpAddressSucceedsWithHostnameOverrideInUrl() throws Exception {
    try (Connection connection =
        DriverManager.getConnection(
            serverUrl(IP_ADDRESS) + "&hostnameOverride=" + CERTIFICATE_HOSTNAME)) {
      assertTrue(connection.isValid(0));
    }
  }

  @Test
  public void testConnectionByIpAddressSucceedsWithHostnameOverrideInProperties() throws Exception {
    final Properties properties = new Properties();
    properties.setProperty(
        ArrowFlightConnectionProperty.HOSTNAME_OVERRIDE.camelName(), CERTIFICATE_HOSTNAME);

    try (Connection connection = DriverManager.getConnection(serverUrl(IP_ADDRESS), properties)) {
      assertTrue(connection.isValid(0));
    }
  }

  @Test
  public void testServerCertificateIsVerifiedAgainstHostnameOverride() {
    // The connection host matches the certificate, but verification uses the override.
    assertThrows(
        SQLException.class,
        () -> {
          try (Connection connection =
              DriverManager.getConnection(
                  serverUrl(CERTIFICATE_HOSTNAME) + "&hostnameOverride=other.example.com")) {
            connection.isValid(0);
          }
        });
  }

  @Test
  public void testHostnameOverrideIsIgnoredWithoutEncryption() throws Exception {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FlightServer server =
            FlightServer.builder(
                    allocator,
                    Location.forGrpcInsecure(CERTIFICATE_HOSTNAME, 0),
                    new MockFlightSqlProducer())
                .build()
                .start();
        Connection connection =
            DriverManager.getConnection(
                String.format(
                    "jdbc:arrow-flight-sql://%s:%d?useEncryption=false&hostnameOverride=%s",
                    IP_ADDRESS, server.getPort(), "other.example.com"))) {
      assertTrue(connection.isValid(0));
    }
  }

  @Test
  public void testHostnameOverrideIsAppliedToEndpointOnConnectionHost() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    final Ticket ticket = new Ticket("partition".getBytes(StandardCharsets.UTF_8));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot partition = VectorSchemaRoot.create(schema, allocator);
        FlightServer dataServer =
            FlightServer.builder(
                    allocator,
                    Location.forGrpcTls(CERTIFICATE_HOSTNAME, 0),
                    new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(ticket, partition))
                .useTls(certKey.cert, certKey.key)
                .build()
                .start()) {
      partition.setRowCount(1);
      ((IntVector) partition.getVector(0)).set(0, 42);

      // The endpoint is on the same IP address as the connection, so it must be verified against
      // the hostname override as well.
      final FlightEndpoint endpoint =
          new FlightEndpoint(ticket, Location.forGrpcTls(IP_ADDRESS, dataServer.getPort()));

      try (PartitionedFlightSqlProducer rootProducer =
              new PartitionedFlightSqlProducer(schema, endpoint);
          FlightServer rootServer =
              FlightServer.builder(
                      allocator, Location.forGrpcTls(CERTIFICATE_HOSTNAME, 0), rootProducer)
                  .useTls(certKey.cert, certKey.key)
                  .build()
                  .start();
          Connection connection =
              DriverManager.getConnection(
                  tlsUrl(IP_ADDRESS, rootServer.getPort())
                      + "&hostnameOverride="
                      + CERTIFICATE_HOSTNAME);
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SELECT partitioned_data")) {
        assertTrue(resultSet.next());
        assertEquals(42, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }
  }
}
