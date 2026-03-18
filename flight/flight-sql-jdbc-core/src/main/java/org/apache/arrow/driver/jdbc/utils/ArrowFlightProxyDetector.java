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

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * gRPC {@link ProxyDetector} for Arrow Flight JDBC connections.
 *
 * <p>Resolution priority:
 *
 * <ol>
 *   <li>{@code proxyDisable=force} → no proxy (always)
 *   <li>Target host matches {@code proxyBypassPattern} → no proxy
 *   <li>Explicit {@code proxyHost} + {@code proxyPort} → use that proxy
 *   <li>{@link ProxySelector} default → use first non-DIRECT proxy found
 *   <li>No proxy
 * </ol>
 *
 * <p>{@code proxyBypassPattern} follows the {@code http.nonProxyHosts} format: {@code |}-separated
 * glob patterns where {@code *} matches any sequence of characters. Matching is case-insensitive.
 */
public final class ArrowFlightProxyDetector implements ProxyDetector {

  @Nullable private final String proxyHost;
  @Nullable private final Integer proxyPort;
  @Nullable private final String bypassPattern;
  private final boolean forceDisabled;
  @Nullable private final ProxySelector proxySelector;

  /** Production constructor — falls back to {@link ProxySelector#getDefault()}. */
  public ArrowFlightProxyDetector(
      @Nullable String proxyHost,
      @Nullable Integer proxyPort,
      @Nullable String bypassPattern,
      @Nullable String proxyDisable) {
    this(proxyHost, proxyPort, bypassPattern, proxyDisable, ProxySelector.getDefault());
  }

  /** Package-private constructor for testing — accepts an explicit {@link ProxySelector}. */
  ArrowFlightProxyDetector(
      @Nullable String proxyHost,
      @Nullable Integer proxyPort,
      @Nullable String bypassPattern,
      @Nullable String proxyDisable,
      @Nullable ProxySelector proxySelector) {
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
    this.bypassPattern = bypassPattern;
    this.forceDisabled = "force".equalsIgnoreCase(proxyDisable);
    this.proxySelector = proxySelector;
  }

  @Override
  @Nullable
  public ProxiedSocketAddress proxyFor(SocketAddress targetAddress) throws IOException {
    if (forceDisabled) {
      return null;
    }
    InetSocketAddress inetTarget = (InetSocketAddress) targetAddress;
    if (matchesBypass(inetTarget.getHostString())) {
      return null;
    }
    InetSocketAddress proxyAddr = resolveProxy(inetTarget);
    if (proxyAddr == null) {
      return null;
    }
    return HttpConnectProxiedSocketAddress.newBuilder()
        .setTargetAddress(inetTarget)
        .setProxyAddress(proxyAddr)
        .build();
  }

  /**
   * Returns true if {@code host} matches any pattern in the bypass list.
   *
   * <p>Patterns use {@code http.nonProxyHosts} format: {@code |}-separated globs, where {@code *}
   * is a wildcard for any sequence of characters.
   */
  private boolean matchesBypass(String host) {
    if (bypassPattern == null || bypassPattern.isEmpty()) {
      return false;
    }
    for (String pattern : bypassPattern.split("\\|")) {
      String trimmed = pattern.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      // Convert glob to regex: escape dots, replace * with .*
      String regex = trimmed.replace(".", "\\.").replace("*", ".*");
      if (host.matches("(?i)" + regex)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Resolves which proxy address to use for the given target.
   *
   * <p>Tries explicit proxy first, then {@link ProxySelector}.
   *
   * @return resolved proxy address, or {@code null} if no proxy should be used
   */
  @Nullable
  private InetSocketAddress resolveProxy(InetSocketAddress target) {
    if (proxyHost != null && proxyPort != null) {
      // new InetSocketAddress(String, int) resolves IP literals without DNS;
      // for hostnames it attempts DNS — required since HttpConnectProxiedSocketAddress
      // validates the proxy address is resolved.
      return new InetSocketAddress(proxyHost, proxyPort);
    }
    if (proxySelector == null) {
      return null;
    }
    URI uri;
    try {
      uri = new URI("https", null, target.getHostString(), target.getPort(), null, null, null);
    } catch (URISyntaxException e) {
      return null;
    }
    List<Proxy> proxies = proxySelector.select(uri);
    for (Proxy proxy : proxies) {
      if (proxy.type() != Proxy.Type.DIRECT && proxy.address() instanceof InetSocketAddress) {
        return (InetSocketAddress) proxy.address();
      }
    }
    return null;
  }
}
