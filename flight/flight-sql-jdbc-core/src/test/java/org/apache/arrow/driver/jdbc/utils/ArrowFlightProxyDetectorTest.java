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
package org.apache.arrow.driver.jdbc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link ArrowFlightProxyDetector}. */
class ArrowFlightProxyDetectorTest {

  private static final InetSocketAddress TARGET =
      InetSocketAddress.createUnresolved("dev.iomete.cloud", 443);

  // Use IP literals — InetSocketAddress resolves them without DNS, avoiding test-env failures.
  // HttpConnectProxiedSocketAddress requires a resolved proxy address.
  private static final String PROXY_HOST = "192.168.1.1";
  private static final int PROXY_PORT = 8080;

  @Nested
  // force disabled nullifies explicit proxy; force disabled overrides ProxySelector
  class ForceDisabled {

    @Test
    void givenForceDisabled_whenProxyFor_thenReturnsNull() throws IOException {
      // GIVEN: force disabled with explicit proxy configured
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(PROXY_HOST, PROXY_PORT, null, "force", noProxySelector());

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertNull(result);
    }

    @Test
    void givenForceDisabledAndProxySelectorReturnsProxy_whenProxyFor_thenReturnsNull()
        throws IOException {
      // GIVEN: force disabled overrides even ProxySelector
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              null, null, null, "force", proxySelectorReturning("192.168.1.2", 3128));

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertNull(result);
    }
  }

  @Nested
  // returns configured proxy address; preserves target address
  class ExplicitProxy {

    @Test
    void givenExplicitProxy_whenProxyFor_thenReturnsConfiguredProxy() throws IOException {
      // GIVEN: explicit proxyHost + proxyPort
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(PROXY_HOST, PROXY_PORT, null, null, noProxySelector());

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertProxyAddress(result, PROXY_HOST, PROXY_PORT);
    }

    @Test
    void givenExplicitProxy_whenProxyFor_thenTargetAddressPreserved() throws IOException {
      // GIVEN: explicit proxy
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(PROXY_HOST, PROXY_PORT, null, null, noProxySelector());

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN: target address is forwarded through the proxy
      HttpConnectProxiedSocketAddress httpProxy =
          assertInstanceOf(HttpConnectProxiedSocketAddress.class, result);
      assertEquals(TARGET, httpProxy.getTargetAddress());
    }
  }

  @Nested
  // matching pattern bypasses proxy; non-matching pattern allows proxy; multiple pipe-separated
  // patterns; case-insensitive matching
  class BypassPattern {

    @Test
    void givenBypassMatchesTarget_whenProxyFor_thenReturnsNull() throws IOException {
      // GIVEN: bypass pattern matches target, explicit proxy configured
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST, PROXY_PORT, "*.iomete.cloud", null, noProxySelector());

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN: bypass wins over explicit proxy
      assertNull(result);
    }

    @Test
    void givenBypassDoesNotMatchTarget_whenProxyFor_thenReturnsProxy() throws IOException {
      // GIVEN: bypass pattern does NOT match target
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST, PROXY_PORT, "*.internal.net", null, noProxySelector());

      // WHEN
      ProxiedSocketAddress result =
          sut.proxyFor(InetSocketAddress.createUnresolved("api.example.com", 443));

      // THEN
      assertProxyAddress(result, PROXY_HOST, PROXY_PORT);
    }

    @Test
    void givenMultipleBypassPatterns_whenProxyFor_thenAnyMatchBypasses() throws IOException {
      // GIVEN: pipe-separated patterns (http.nonProxyHosts format)
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST,
              PROXY_PORT,
              "localhost|*.internal|10.*|exact.host.com",
              null,
              noProxySelector());

      // THEN: each pattern type is tested
      assertNull(sut.proxyFor(InetSocketAddress.createUnresolved("localhost", 443)), "exact match");
      assertNull(
          sut.proxyFor(InetSocketAddress.createUnresolved("db.internal", 5432)), "suffix wildcard");
      assertNull(
          sut.proxyFor(InetSocketAddress.createUnresolved("10.0.0.1", 443)), "prefix wildcard");
      assertNull(
          sut.proxyFor(InetSocketAddress.createUnresolved("exact.host.com", 443)),
          "exact FQDN match");

      // non-matching host goes through proxy
      assertNotNull(
          sut.proxyFor(InetSocketAddress.createUnresolved("external.com", 443)),
          "non-matching host should use proxy");
    }

    @Test
    void givenBypassPatternIsCaseInsensitive_whenProxyFor_thenMatchesRegardlessOfCase()
        throws IOException {
      // GIVEN: mixed-case bypass vs mixed-case target
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST, PROXY_PORT, "*.Example.COM", null, noProxySelector());

      // THEN
      assertNull(sut.proxyFor(InetSocketAddress.createUnresolved("API.EXAMPLE.COM", 443)));
      assertNull(sut.proxyFor(InetSocketAddress.createUnresolved("api.example.com", 443)));
    }
  }

  @Nested
  // uses selector when no explicit proxy; null when selector returns DIRECT; picks first non-DIRECT
  // from multi-proxy list; handles null ProxySelector
  class ProxySelectorFallback {

    @Test
    void givenNoExplicitProxyAndSelectorReturnsProxy_whenProxyFor_thenUsesSelector()
        throws IOException {
      // GIVEN: no explicit proxy, ProxySelector returns an HTTP proxy
      String selectorHost = "192.168.1.2";
      int selectorPort = 3128;
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              null, null, null, null, proxySelectorReturning(selectorHost, selectorPort));

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertProxyAddress(result, selectorHost, selectorPort);
    }

    @Test
    void givenNoExplicitProxyAndSelectorReturnsDirect_whenProxyFor_thenReturnsNull()
        throws IOException {
      // GIVEN: ProxySelector returns DIRECT (no proxy)
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(null, null, null, null, noProxySelector());

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertNull(result);
    }

    @Test
    void givenNoExplicitProxyAndSelectorReturnsMultiple_whenProxyFor_thenUsesFirstNonDirect()
        throws IOException {
      // GIVEN: ProxySelector returns [DIRECT, HTTP proxy]
      ProxySelector selector = mock(ProxySelector.class);
      InetSocketAddress proxyAddr = new InetSocketAddress("192.168.1.3", 9090);
      List<Proxy> proxies = List.of(Proxy.NO_PROXY, new Proxy(Proxy.Type.HTTP, proxyAddr));
      when(selector.select(any(URI.class))).thenReturn(proxies);

      ArrowFlightProxyDetector sut = new ArrowFlightProxyDetector(null, null, null, null, selector);

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN: skips DIRECT, uses the HTTP proxy
      assertProxyAddress(result, "192.168.1.3", 9090);
    }

    @Test
    void givenNullProxySelector_whenProxyFor_thenReturnsNull() throws IOException {
      // GIVEN: null ProxySelector (can happen if JVM has no default)
      ArrowFlightProxyDetector sut = new ArrowFlightProxyDetector(null, null, null, null, null);

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertNull(result);
    }
  }

  @Nested
  // returns null when nothing is configured
  class NoProxyConfigured {

    @Test
    void givenNothingConfigured_whenProxyFor_thenReturnsNull() throws IOException {
      // GIVEN: no proxy, no bypass, not disabled, selector returns DIRECT
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(null, null, null, null, noProxySelector());

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN
      assertNull(result);
    }
  }

  @Nested
  // forceDisabled > bypass > explicit > selector
  class PriorityOrder {

    @Test
    void givenForceDisabledWithExplicitProxyAndBypass_whenProxyFor_thenForceDisabledWins()
        throws IOException {
      // GIVEN: all settings configured, force disabled takes highest priority
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST,
              PROXY_PORT,
              "*.other.com",
              "force",
              proxySelectorReturning("192.168.1.2", 3128));

      // THEN
      assertNull(sut.proxyFor(TARGET));
    }

    @Test
    void givenBypassMatchesAndExplicitProxy_whenProxyFor_thenBypassWins() throws IOException {
      // GIVEN: target matches bypass, explicit proxy also configured
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST,
              PROXY_PORT,
              "*.iomete.cloud",
              null,
              proxySelectorReturning("192.168.1.2", 3128));

      // THEN: bypass takes priority over explicit proxy
      assertNull(sut.proxyFor(TARGET));
    }

    @Test
    void givenExplicitProxyAndSelector_whenProxyFor_thenExplicitWins() throws IOException {
      // GIVEN: both explicit proxy and ProxySelector configured
      ArrowFlightProxyDetector sut =
          new ArrowFlightProxyDetector(
              PROXY_HOST, PROXY_PORT, null, null, proxySelectorReturning("192.168.1.2", 3128));

      // WHEN
      ProxiedSocketAddress result = sut.proxyFor(TARGET);

      // THEN: explicit proxy takes priority over selector
      assertProxyAddress(result, PROXY_HOST, PROXY_PORT);
    }
  }

  private static ProxySelector noProxySelector() {
    ProxySelector selector = mock(ProxySelector.class);
    when(selector.select(any(URI.class))).thenReturn(Collections.singletonList(Proxy.NO_PROXY));
    return selector;
  }

  private static ProxySelector proxySelectorReturning(String host, int port) {
    ProxySelector selector = mock(ProxySelector.class);
    InetSocketAddress proxyAddr = new InetSocketAddress(host, port);
    Proxy proxy = new Proxy(Proxy.Type.HTTP, proxyAddr);
    when(selector.select(any(URI.class))).thenReturn(Collections.singletonList(proxy));
    return selector;
  }

  private static void assertProxyAddress(
      ProxiedSocketAddress result, String expectedHost, int expectedPort) {
    assertNotNull(result);
    HttpConnectProxiedSocketAddress httpProxy =
        assertInstanceOf(HttpConnectProxiedSocketAddress.class, result);
    InetSocketAddress proxyAddr = (InetSocketAddress) httpProxy.getProxyAddress();
    assertEquals(expectedHost, proxyAddr.getHostString());
    assertEquals(expectedPort, proxyAddr.getPort());
  }
}
