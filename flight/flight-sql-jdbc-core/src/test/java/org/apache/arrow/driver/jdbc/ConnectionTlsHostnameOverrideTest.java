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
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Properties;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.FlightSqlTestCertificates;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.PartitionedFlightSqlProducer;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the hostnameOverride connection property. The test certificate is issued for {@code
 * localhost} and has no IP address in its subject alternative names, so connecting by IP address
 * only passes verification when the hostname override is used. Servers bind to the IP address
 * explicitly so the tests don't depend on how {@code localhost} resolves.
 */
public class ConnectionTlsHostnameOverrideTest {

  private static final String IP_ADDRESS = "127.0.0.1";
  private static final String CERTIFICATE_HOSTNAME = "localhost";
  // Connecting with credentials performs a handshake, so the TLS connection is made eagerly.
  private static final String USER = "user1";
  private static final String PASSWORD = "pass1";

  private static final FlightSqlTestCertificates.CertKeyPair certKey =
      FlightSqlTestCertificates.exampleTlsCerts().get(0);
  private static final String tlsRootCertsPath = certKey.cert.getPath();

  private static BufferAllocator allocator;
  private static FlightServer tlsServer;

  @BeforeAll
  public static void setUp() throws Exception {
    DriverManager.registerDriver(new ArrowFlightJdbcDriver());
    allocator = new RootAllocator(Long.MAX_VALUE);
    tlsServer =
        FlightServer.builder(
                allocator, Location.forGrpcTls(IP_ADDRESS, 0), new MockFlightSqlProducer())
            .headerAuthenticator(
                new UserPasswordAuthentication.Builder()
                    .user(USER, PASSWORD)
                    .build()
                    .authenticate())
            .useTls(certKey.cert, certKey.key)
            .build()
            .start();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    tlsServer.close();
    allocator.close();
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
    return tlsUrl(host, tlsServer.getPort())
        + String.format("&user=%s&password=%s", USER, PASSWORD);
  }

  /** Asserts that connecting fails because the server certificate was rejected. */
  private static void assertCertificateRejected(final String url) {
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> {
              try (Connection connection = DriverManager.getConnection(url)) {
                connection.isValid(0);
              }
            });
    assertCausedByCertificateException(exception);
  }

  /** Asserts that the server certificate being rejected caused {@code exception}. */
  private static void assertCausedByCertificateException(final Throwable exception) {
    final Deque<Throwable> pending = new ArrayDeque<>();
    pending.add(exception);
    while (!pending.isEmpty()) {
      final Throwable throwable = pending.pop();
      if (throwable instanceof CertificateException) {
        return;
      }
      if (throwable.getCause() != null) {
        pending.add(throwable.getCause());
      }
      pending.addAll(Arrays.asList(throwable.getSuppressed()));
    }
    fail("Expected the server certificate to be rejected", exception);
  }

  @Test
  public void testConnectionByIpAddressFailsWithoutHostnameOverride() throws Exception {
    assertCertificateRejected(serverUrl(IP_ADDRESS));
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
  public void testServerCertificateIsVerifiedAgainstHostnameOverride() throws Exception {
    // Same connection as the passing override tests, but the override doesn't match the
    // certificate, so verification must use the override's value.
    assertCertificateRejected(serverUrl(IP_ADDRESS) + "&hostnameOverride=other.example.com");
  }

  @Test
  public void testBlankHostnameOverrideIsIgnored() throws Exception {
    // A blank value behaves as if the property were unset rather than being sent to gRPC.
    assertCertificateRejected(serverUrl(IP_ADDRESS) + "&hostnameOverride=");
  }

  @Test
  public void testHostnameOverrideIsIgnoredWithoutEncryption() throws Exception {
    try (FlightServer server =
            FlightServer.builder(
                    allocator, Location.forGrpcInsecure(IP_ADDRESS, 0), new MockFlightSqlProducer())
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
  public void testHostnameOverrideIsAppliedToEndpointOnConnectionLocation() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    final Ticket ticket = new Ticket("partition".getBytes(StandardCharsets.UTF_8));

    try (VectorSchemaRoot partition = VectorSchemaRoot.create(schema, allocator)) {
      partition.setRowCount(1);
      ((IntVector) partition.getVector(0)).set(0, 42);

      try (SelfServingFlightSqlProducer producer =
              new SelfServingFlightSqlProducer(schema, ticket, partition);
          FlightServer server =
              FlightServer.builder(allocator, Location.forGrpcTls(IP_ADDRESS, 0), producer)
                  .useTls(certKey.cert, certKey.key)
                  .build()
                  .start()) {
        // The endpoint is at the same location as the connection, so it must be verified against
        // the hostname override as well.
        producer.location = Location.forGrpcTls(IP_ADDRESS, server.getPort());

        try (Connection connection =
                DriverManager.getConnection(
                    tlsUrl(IP_ADDRESS, server.getPort())
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

  @Test
  public void testHostnameOverrideIsNotAppliedToEndpointOnDifferentPort() throws Exception {
    final Schema schema =
        new Schema(
            Arrays.asList(Field.nullablePrimitive("int_column", new ArrowType.Int(32, true))));
    final Ticket ticket = new Ticket("partition".getBytes(StandardCharsets.UTF_8));

    try (VectorSchemaRoot partition = VectorSchemaRoot.create(schema, allocator);
        FlightServer dataServer =
            FlightServer.builder(
                    allocator,
                    Location.forGrpcTls(IP_ADDRESS, 0),
                    new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(ticket, partition))
                .useTls(certKey.cert, certKey.key)
                .build()
                .start()) {
      partition.setRowCount(1);
      ((IntVector) partition.getVector(0)).set(0, 42);

      // The endpoint is on the connection's IP address but another port, so it is verified
      // against the IP address, which the certificate does not cover.
      final FlightEndpoint endpoint =
          new FlightEndpoint(ticket, Location.forGrpcTls(IP_ADDRESS, dataServer.getPort()));

      try (PartitionedFlightSqlProducer rootProducer =
              new PartitionedFlightSqlProducer(schema, endpoint);
          FlightServer rootServer =
              FlightServer.builder(allocator, Location.forGrpcTls(IP_ADDRESS, 0), rootProducer)
                  .useTls(certKey.cert, certKey.key)
                  .build()
                  .start();
          Connection connection =
              DriverManager.getConnection(
                  tlsUrl(IP_ADDRESS, rootServer.getPort())
                      + "&hostnameOverride="
                      + CERTIFICATE_HOSTNAME);
          Statement statement = connection.createStatement()) {
        final SQLException exception =
            assertThrows(
                SQLException.class,
                () -> {
                  try (ResultSet resultSet = statement.executeQuery("SELECT partitioned_data")) {
                    resultSet.next();
                  }
                });
        assertCausedByCertificateException(exception);
      }
    }
  }

  /**
   * Returns a single endpoint at {@link #location}, which is set once the server has started, and
   * serves its data, so the endpoint is at the same location as the connection.
   */
  private static final class SelfServingFlightSqlProducer extends PartitionedFlightSqlProducer {
    private final Schema schema;
    private final Ticket ticket;
    private final PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer dataProducer;
    private volatile Location location;

    SelfServingFlightSqlProducer(
        final Schema schema, final Ticket ticket, final VectorSchemaRoot data) {
      super(schema);
      this.schema = schema;
      this.ticket = ticket;
      this.dataProducer = new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(ticket, data);
    }

    private FlightInfo flightInfo(final FlightDescriptor descriptor) {
      return FlightInfo.builder(
              schema, descriptor, Collections.singletonList(new FlightEndpoint(ticket, location)))
          .build();
    }

    @Override
    public FlightInfo getFlightInfoStatement(
        final FlightSql.CommandStatementQuery command,
        final CallContext context,
        final FlightDescriptor descriptor) {
      return flightInfo(descriptor);
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
        final FlightSql.CommandPreparedStatementQuery command,
        final CallContext context,
        final FlightDescriptor descriptor) {
      return flightInfo(descriptor);
    }

    @Override
    public void getStream(
        final CallContext context, final Ticket ticket, final ServerStreamListener listener) {
      dataProducer.getStream(context, ticket, listener);
    }
  }
}
