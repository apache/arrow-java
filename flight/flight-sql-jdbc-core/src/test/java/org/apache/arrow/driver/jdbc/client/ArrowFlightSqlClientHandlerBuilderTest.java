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
package org.apache.arrow.driver.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Optional;
import org.apache.arrow.driver.jdbc.FlightServerTestExtension;
import org.apache.arrow.driver.jdbc.utils.CoreMockedSqlProducers;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Test the behavior of ArrowFlightSqlClientHandler.Builder. */
public class ArrowFlightSqlClientHandlerBuilderTest {

  @RegisterExtension
  public static final FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION =
      FlightServerTestExtension.createStandardTestExtension(
          CoreMockedSqlProducers.getLegacyProducer());

  private static BufferAllocator allocator;

  @BeforeAll
  public static void setup() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterAll
  public static void tearDown() {
    allocator.close();
  }

  @Test
  public void testRetainCookiesOnAuthOff() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withBufferAllocator(allocator)
            .withUsername(FlightServerTestExtension.DEFAULT_USER)
            .withPassword(FlightServerTestExtension.DEFAULT_PASSWORD)
            .withEncryption(false)
            .withRetainCookies(true)
            .withRetainAuth(false);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder =
          new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertNotSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testRetainCookiesOffAuthOff() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withBufferAllocator(allocator)
            .withUsername(FlightServerTestExtension.DEFAULT_USER)
            .withPassword(FlightServerTestExtension.DEFAULT_PASSWORD)
            .withEncryption(false)
            .withRetainCookies(false)
            .withRetainAuth(false);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder =
          new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertNotSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertNotSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testRetainCookiesOnAuthOn() throws Exception {
    // Arrange
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(FLIGHT_SERVER_TEST_EXTENSION.getHost())
            .withPort(FLIGHT_SERVER_TEST_EXTENSION.getPort())
            .withBufferAllocator(allocator)
            .withUsername(FlightServerTestExtension.DEFAULT_USER)
            .withPassword(FlightServerTestExtension.DEFAULT_PASSWORD)
            .withEncryption(false)
            .withRetainCookies(true)
            .withRetainAuth(true);

    try (ArrowFlightSqlClientHandler rootHandler = rootBuilder.build()) {
      // Act
      final ArrowFlightSqlClientHandler.Builder testBuilder =
          new ArrowFlightSqlClientHandler.Builder(rootBuilder);

      // Assert
      assertSame(rootBuilder.cookieFactory, testBuilder.cookieFactory);
      assertSame(rootBuilder.authFactory, testBuilder.authFactory);
    }
  }

  @Test
  public void testDefaults() {
    final ArrowFlightSqlClientHandler.Builder builder = new ArrowFlightSqlClientHandler.Builder();

    // Validate all non-mandatory fields against defaults in ArrowFlightConnectionProperty.
    assertNull(builder.username);
    assertNull(builder.password);
    assertTrue(builder.useEncryption);
    assertFalse(builder.disableCertificateVerification);
    assertNull(builder.hostnameOverride);
    assertNull(builder.trustStorePath);
    assertNull(builder.trustStorePassword);
    assertTrue(builder.useSystemTrustStore);
    assertNull(builder.token);
    assertTrue(builder.retainAuth);
    assertTrue(builder.retainCookies);
    assertTrue(builder.followEndpointLocations);
    assertNull(builder.tlsRootCertificatesPath);
    assertNull(builder.clientCertificatePath);
    assertNull(builder.clientKeyPath);
    assertEquals(Optional.empty(), builder.catalog);
    assertNull(builder.flightClientCache);
    assertNull(builder.connectTimeout);
    assertNull(builder.driverVersion);
  }

  @Test
  public void testHostnameOverrideIsNotCopied() {
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder().withHostnameOverride("flight.example.com");

    final ArrowFlightSqlClientHandler.Builder copiedBuilder =
        new ArrowFlightSqlClientHandler.Builder(rootBuilder);

    // The override is only valid for the original host and port, so a copy must not carry it to
    // another location by default.
    assertNull(copiedBuilder.hostnameOverride);
  }

  @Test
  public void testFollowEndpointLocationsIsCopied() {
    final ArrowFlightSqlClientHandler.Builder rootBuilder =
        new ArrowFlightSqlClientHandler.Builder().withFollowEndpointLocations(false);

    // Unlike the hostname override, this applies to every location, so a copy must keep it.
    assertFalse(new ArrowFlightSqlClientHandler.Builder(rootBuilder).followEndpointLocations);
  }

  @Test
  public void testHostnameOverrideForEndpointOnSameHost() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("127.0.0.1")
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertEquals(
        "flight.example.com",
        builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://127.0.0.1:1234")));
  }

  @Test
  public void testHostnameOverrideForEndpointOnDifferentPortIsCleared() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("127.0.0.1")
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertNull(builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://127.0.0.1:5678")));
    assertNull(builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://127.0.0.1")));
  }

  @Test
  public void testHostnameOverrideForEndpointMatchesHostCaseInsensitively() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("Tunnel.Local")
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertEquals(
        "flight.example.com",
        builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://tunnel.local:1234")));
  }

  @Test
  public void testHostnameOverrideForEndpointOnIpv6Host() {
    // The JDBC URL is parsed with URI#getHost, which keeps the brackets, like endpoint locations.
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(URI.create("jdbc://[::1]:1234").getHost())
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertEquals(
        "flight.example.com",
        builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://[::1]:1234")));
  }

  @Test
  public void testHostnameOverrideForEndpointOnIpv6HostWithoutBrackets() {
    // A host set through properties or a DataSource may not have brackets.
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("::1")
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertEquals(
        "flight.example.com",
        builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://[::1]:1234")));
  }

  @Test
  public void testHostnameOverrideForEndpointOnDifferentHostIsCleared() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("127.0.0.1")
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertNull(builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://10.0.0.1:1234")));
    assertNull(
        builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://other.example.com:1234")));
  }

  @Test
  public void testHostnameOverrideForEndpointWithoutEncryption() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("127.0.0.1")
            .withPort(1234)
            .withEncryption(false)
            .withHostnameOverride("flight.example.com");

    assertNull(builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://127.0.0.1:1234")));
  }

  @Test
  public void testIsConnectionLocation() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("Tunnel.Local")
            .withPort(1234)
            .withHostnameOverride("flight.example.com");

    assertTrue(builder.isConnectionLocation(URI.create("grpc+tls://tunnel.local:1234")));
    assertTrue(builder.isConnectionLocation(URI.create("grpc+tls://FLIGHT.example.com:1234")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+tls://tunnel.local:5678")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+tls://tunnel.local")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+tls://other.example.com:1234")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+unix:///tmp/flight.sock")));
  }

  @Test
  public void testIsConnectionLocationIgnoresSchemeCase() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder().withHost("localhost").withPort(1234);

    assertTrue(builder.isConnectionLocation(URI.create("GRPC+TLS://localhost:1234")));
    assertTrue(builder.isConnectionLocation(URI.create("Grpc+Tcp://localhost:1234")));
    assertTrue(builder.isConnectionLocation(URI.create("GRPC://localhost:1234")));
    assertFalse(builder.isConnectionLocation(URI.create("//localhost:1234")));
    assertFalse(
        builder
            .withEncryption(false)
            .isConnectionLocation(URI.create("GRPC+TLS://localhost:1234")));
  }

  @Test
  public void testIsConnectionLocationWithHostnameOverridePort() {
    // The override replaces the whole authority, so its own port is used instead.
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("127.0.0.1")
            .withPort(1234)
            .withHostnameOverride("flight.example.com:32010");

    assertTrue(builder.isConnectionLocation(URI.create("grpc+tls://flight.example.com:32010")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+tls://flight.example.com:1234")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+tls://flight.example.com")));
  }

  @Test
  public void testIsConnectionLocationOnIpv6Host() {
    // The JDBC URL is parsed with URI#getHost, which keeps the brackets.
    assertTrue(
        new ArrowFlightSqlClientHandler.Builder()
            .withHost(URI.create("jdbc://[::1]:1234").getHost())
            .withPort(1234)
            .isConnectionLocation(URI.create("grpc+tls://[::1]:1234")));
    assertTrue(
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("::1")
            .withPort(1234)
            .isConnectionLocation(URI.create("grpc+tls://[::1]:1234")));
  }

  @Test
  public void testIsConnectionLocationIgnoresHostnameOverrideWithoutEncryption() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder()
            .withHost("127.0.0.1")
            .withPort(1234)
            .withEncryption(false)
            .withHostnameOverride("flight.example.com");

    assertTrue(builder.isConnectionLocation(URI.create("grpc+tcp://127.0.0.1:1234")));
    assertFalse(builder.isConnectionLocation(URI.create("grpc+tcp://flight.example.com:1234")));
  }

  @Test
  public void testBlankHostnameOverrideIsUnset() {
    assertNull(new ArrowFlightSqlClientHandler.Builder().withHostnameOverride("").hostnameOverride);
    assertNull(
        new ArrowFlightSqlClientHandler.Builder().withHostnameOverride("  ").hostnameOverride);
  }

  @Test
  public void testBlankTrustStorePathIsUnset() {
    assertNull(new ArrowFlightSqlClientHandler.Builder().withTrustStorePath(null).trustStorePath);
    assertNull(new ArrowFlightSqlClientHandler.Builder().withTrustStorePath("").trustStorePath);
    assertNull(new ArrowFlightSqlClientHandler.Builder().withTrustStorePath("  ").trustStorePath);
    assertEquals(
        "/path/to/truststore.jks",
        new ArrowFlightSqlClientHandler.Builder()
            .withTrustStorePath("/path/to/truststore.jks")
            .trustStorePath);
  }

  @Test
  public void testBlankTlsRootCertificatesPathIsUnset() {
    assertNull(
        new ArrowFlightSqlClientHandler.Builder()
            .withTlsRootCertificates(null)
            .tlsRootCertificatesPath);
    assertNull(
        new ArrowFlightSqlClientHandler.Builder()
            .withTlsRootCertificates("")
            .tlsRootCertificatesPath);
    assertNull(
        new ArrowFlightSqlClientHandler.Builder()
            .withTlsRootCertificates("  ")
            .tlsRootCertificatesPath);
    assertEquals(
        "/path/to/ca.pem",
        new ArrowFlightSqlClientHandler.Builder()
            .withTlsRootCertificates("/path/to/ca.pem")
            .tlsRootCertificatesPath);
  }

  @Test
  public void testHostnameOverrideIsTrimmed() {
    assertEquals(
        "flight.example.com",
        new ArrowFlightSqlClientHandler.Builder()
            .withHostnameOverride(" flight.example.com ")
            .hostnameOverride);
  }

  @Test
  public void testHostnameOverrideForEndpointWithoutOverride() {
    final ArrowFlightSqlClientHandler.Builder builder =
        new ArrowFlightSqlClientHandler.Builder().withHost("127.0.0.1").withPort(1234);

    assertNull(builder.getHostnameOverrideForEndpoint(URI.create("grpc+tls://127.0.0.1:1234")));
  }

  @Test
  public void testCatalog() {
    ArrowFlightSqlClientHandler.Builder rootBuilder = new ArrowFlightSqlClientHandler.Builder();

    rootBuilder.withCatalog(null);
    assertFalse(rootBuilder.catalog.isPresent());

    rootBuilder.withCatalog("");
    assertTrue(rootBuilder.catalog.isPresent());

    rootBuilder.withCatalog("   ");
    assertTrue(rootBuilder.catalog.isPresent());

    final String noSpaces = "noSpaces";
    rootBuilder.withCatalog(noSpaces);
    assertTrue(rootBuilder.catalog.isPresent());
    assertEquals(noSpaces, rootBuilder.catalog.get());

    final String nameWithSpaces = "  spaces ";
    rootBuilder.withCatalog(nameWithSpaces);
    assertTrue(rootBuilder.catalog.isPresent());
    assertEquals(nameWithSpaces, rootBuilder.catalog.get());
  }
}
