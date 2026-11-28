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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.arrow.driver.jdbc.authentication.UserPasswordAuthentication;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.apache.arrow.driver.jdbc.utils.FallbackFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.FlightSqlTestCertificates;
import org.apache.arrow.driver.jdbc.utils.PartitionedFlightSqlProducer;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the followEndpointLocations connection property. The servers record the calls they receive,
 * so the tests can check which connection each call came from and what it was sent.
 */
public class ConnectionFollowEndpointLocationsTest {

  private static final String USER = "user1";
  private static final String PASSWORD = "pass1";
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Ticket TICKET = new Ticket("data".getBytes(StandardCharsets.UTF_8));
  // Issued for localhost, which the servers bind to.
  private static final FlightSqlTestCertificates.CertKeyPair CERT_KEY =
      FlightSqlTestCertificates.exampleTlsCerts().get(0);

  private BufferAllocator allocator;
  private VectorSchemaRoot data;

  @BeforeAll
  public static void setUpDriver() throws SQLException {
    DriverManager.registerDriver(new ArrowFlightJdbcDriver());
  }

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    data =
        VectorSchemaRoot.create(
            new Schema(
                Collections.singletonList(
                    Field.nullable("int_column", Types.MinorType.INT.getType()))),
            allocator);
    data.setRowCount(1);
    ((IntVector) data.getVector(0)).set(0, 1);
  }

  @AfterEach
  public void tearDown() {
    data.close();
    allocator.close();
  }

  @Test
  public void testOtherLocationIsFollowedByDefault() throws Exception {
    final List<String> dataServerCalls = Collections.synchronizedList(new ArrayList<>());
    try (FlightServer dataServer = dataServer(dataServerCalls);
        PartitionedFlightSqlProducer producer =
            new PartitionedFlightSqlProducer(
                data.getSchema(), new FlightEndpoint(TICKET, dataServer.getLocation()));
        FlightServer rootServer = server(producer, (call, headers) -> {});
        Connection connection = DriverManager.getConnection(url(rootServer, ""));
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1")) {
      assertTrue(resultSet.next());
      // The other server is sent the bearer token from the connection's handshake.
      assertFalse(dataServerCalls.isEmpty());
      assertThat(dataServerCalls, everyItem(startsWith("Bearer ")));
    }
  }

  @Test
  public void testOtherLocationIsNotFollowed() throws Exception {
    final List<String> dataServerCalls = Collections.synchronizedList(new ArrayList<>());
    try (FlightServer dataServer = dataServer(dataServerCalls);
        PartitionedFlightSqlProducer producer =
            new PartitionedFlightSqlProducer(
                data.getSchema(), new FlightEndpoint(TICKET, dataServer.getLocation()));
        FlightServer rootServer = server(producer, (call, headers) -> {});
        Connection connection =
            DriverManager.getConnection(url(rootServer, "&followEndpointLocations=false"));
        Statement statement = connection.createStatement()) {
      final SQLException e =
          assertThrows(SQLException.class, () -> statement.executeQuery("SELECT 1"));
      assertThat(e.getMessage(), containsString("followEndpointLocations"));
      assertTrue(dataServerCalls.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testOwnLocation(final boolean followEndpointLocations) throws Exception {
    final Set<SocketAddress> clients = ConcurrentHashMap.newKeySet();
    try (FallbackFlightSqlProducer producer = new FallbackFlightSqlProducer(data);
        FlightServer rootServer =
            server(
                producer,
                (call, headers) ->
                    clients.add(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)))) {
      producer.setLocation(rootServer.getLocation());
      try (Connection connection =
              DriverManager.getConnection(
                  url(rootServer, "&followEndpointLocations=" + followEndpointLocations));
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("own location")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
      }
      // Following the location opens a second connection to the same server.
      assertEquals(followEndpointLocations ? 2 : 1, clients.size());
    }
  }

  @Test
  public void testPlaintextOwnLocationIsFetchedOverTlsConnection() throws Exception {
    final Set<SocketAddress> clients = ConcurrentHashMap.newKeySet();
    try (FallbackFlightSqlProducer producer = new FallbackFlightSqlProducer(data);
        FlightServer rootServer =
            tlsServer(
                producer,
                (call, headers) ->
                    clients.add(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)))) {
      // The server only accepts TLS, so fetching this location over plaintext would fail.
      producer.setLocation(Location.forGrpcInsecure("localhost", rootServer.getPort()));
      try (Connection connection =
              DriverManager.getConnection(tlsUrl(rootServer, "&followEndpointLocations=false"));
          Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("own location")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
      }
      assertEquals(1, clients.size());
    }
  }

  @Test
  public void testUpperCaseTlsLocationIsFollowedOverTls() throws Exception {
    final List<String> dataServerCalls = Collections.synchronizedList(new ArrayList<>());
    try (FlightServer dataServer =
            tlsServerBuilder(
                    new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(TICKET, data),
                    (call, headers) ->
                        dataServerCalls.add(String.valueOf(headers.get(AUTHORIZATION))))
                .build()
                .start();
        PartitionedFlightSqlProducer producer =
            new PartitionedFlightSqlProducer(
                data.getSchema(),
                new FlightEndpoint(
                    TICKET,
                    new Location(URI.create("GRPC+TLS://localhost:" + dataServer.getPort()))));
        FlightServer rootServer = tlsServer(producer, (call, headers) -> {});
        Connection connection = DriverManager.getConnection(tlsUrl(rootServer, ""));
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1")) {
      // Schemes are case-insensitive, so this is not refused as an unencrypted location.
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getInt(1));
      assertFalse(dataServerCalls.isEmpty());
    }
  }

  /** Starts a server that serves {@link #TICKET} and records the authorization of each call. */
  private FlightServer dataServer(final List<String> calls) throws Exception {
    return serverBuilder(
            new PartitionedFlightSqlProducer.DataOnlyFlightSqlProducer(TICKET, data),
            (call, headers) -> calls.add(String.valueOf(headers.get(AUTHORIZATION))))
        .build()
        .start();
  }

  /** Starts a server that requires {@link #USER} and {@link #PASSWORD}. */
  private FlightServer server(
      final FlightProducer producer, final BiConsumer<ServerCall<?, ?>, Metadata> onCall)
      throws Exception {
    return serverBuilder(producer, onCall)
        .headerAuthenticator(
            new UserPasswordAuthentication.Builder().user(USER, PASSWORD).build().authenticate())
        .build()
        .start();
  }

  /** Starts a TLS server that requires {@link #USER} and {@link #PASSWORD}. */
  private FlightServer tlsServer(
      final FlightProducer producer, final BiConsumer<ServerCall<?, ?>, Metadata> onCall)
      throws Exception {
    return tlsServerBuilder(producer, onCall)
        .headerAuthenticator(
            new UserPasswordAuthentication.Builder().user(USER, PASSWORD).build().authenticate())
        .build()
        .start();
  }

  private FlightServer.Builder tlsServerBuilder(
      final FlightProducer producer, final BiConsumer<ServerCall<?, ?>, Metadata> onCall)
      throws IOException {
    return serverBuilder(Location.forGrpcTls("localhost", 0), producer, onCall)
        .useTls(CERT_KEY.cert, CERT_KEY.key);
  }

  private FlightServer.Builder serverBuilder(
      final FlightProducer producer, final BiConsumer<ServerCall<?, ?>, Metadata> onCall) {
    return serverBuilder(Location.forGrpcInsecure("localhost", 0), producer, onCall);
  }

  private FlightServer.Builder serverBuilder(
      final Location location,
      final FlightProducer producer,
      final BiConsumer<ServerCall<?, ?>, Metadata> onCall) {
    return FlightServer.builder(allocator, location, producer)
        .transportHint(
            "grpc.builderConsumer",
            (Consumer<NettyServerBuilder>)
                builder ->
                    builder.intercept(
                        new ServerInterceptor() {
                          @Override
                          public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                              final ServerCall<ReqT, RespT> call,
                              final Metadata headers,
                              final ServerCallHandler<ReqT, RespT> next) {
                            onCall.accept(call, headers);
                            return next.startCall(call, headers);
                          }
                        }));
  }

  private static String url(final FlightServer server, final String extraParameters) {
    return String.format(
        "jdbc:arrow-flight-sql://%s:%d/?useEncryption=false&user=%s&password=%s%s",
        server.getLocation().getUri().getHost(), server.getPort(), USER, PASSWORD, extraParameters);
  }

  private static String tlsUrl(final FlightServer server, final String extraParameters)
      throws Exception {
    return String.format(
        "jdbc:arrow-flight-sql://%s:%d/?useEncryption=true&useSystemTrustStore=false&%s=%s"
            + "&user=%s&password=%s%s",
        server.getLocation().getUri().getHost(),
        server.getPort(),
        ArrowFlightConnectionProperty.TLS_ROOT_CERTS.camelName(),
        URLEncoder.encode(CERT_KEY.cert.getPath(), "UTF-8"),
        USER,
        PASSWORD,
        extraParameters);
  }
}
