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
package org.apache.arrow.flight.grpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLException;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * A wrapper around gRPC's Netty builder.
 *
 * <p>It is recommended to use the Netty channel builder directly with {@link
 * org.apache.arrow.flight.FlightGrpcUtils#createFlightClient(BufferAllocator, ManagedChannel)}.
 * However, this class provides an adapter that implements the existing Flight-specific builder
 * interface but allows usage of the Netty builder as well.
 */
public class NettyClientBuilder {
  /**
   * The maximum number of trace events to keep on the gRPC Channel. This value disables channel
   * tracing.
   */
  private static final int MAX_CHANNEL_TRACE_EVENTS = 0;

  protected BufferAllocator allocator;
  protected Location location;
  protected boolean forceTls = false;
  protected int maxInboundMessageSize = Integer.MAX_VALUE;
  protected InputStream trustedCertificates = null;
  protected InputStream clientCertificate = null;
  protected InputStream clientKey = null;
  protected String overrideHostname = null;
  protected List<FlightClientMiddleware.Factory> middleware = new ArrayList<>();
  protected boolean verifyServer = true;

  public NettyClientBuilder() {}

  public NettyClientBuilder(BufferAllocator allocator, Location location) {
    this.allocator = Preconditions.checkNotNull(allocator);
    this.location = Preconditions.checkNotNull(location);
  }

  /** Force the client to connect over TLS. */
  public NettyClientBuilder useTls() {
    this.forceTls = true;
    return this;
  }

  /** Override the hostname checked for TLS. Use with caution in production. */
  public NettyClientBuilder overrideHostname(final String hostname) {
    this.overrideHostname = hostname;
    return this;
  }

  /** Set the maximum inbound message size. */
  public NettyClientBuilder maxInboundMessageSize(int maxSize) {
    Preconditions.checkArgument(maxSize > 0);
    this.maxInboundMessageSize = maxSize;
    return this;
  }

  /** Set the trusted TLS certificates. */
  public NettyClientBuilder trustedCertificates(final InputStream stream) {
    this.trustedCertificates = Preconditions.checkNotNull(stream);
    return this;
  }

  /** Set the trusted TLS certificates. */
  public NettyClientBuilder clientCertificate(
      final InputStream clientCertificate, final InputStream clientKey) {
    Preconditions.checkNotNull(clientKey);
    this.clientCertificate = Preconditions.checkNotNull(clientCertificate);
    this.clientKey = Preconditions.checkNotNull(clientKey);
    return this;
  }

  public BufferAllocator allocator() {
    return allocator;
  }

  public NettyClientBuilder allocator(BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator);
    return this;
  }

  public NettyClientBuilder location(Location location) {
    this.location = Preconditions.checkNotNull(location);
    return this;
  }

  public List<FlightClientMiddleware.Factory> middleware() {
    return Collections.unmodifiableList(middleware);
  }

  public NettyClientBuilder intercept(FlightClientMiddleware.Factory factory) {
    middleware.add(factory);
    return this;
  }

  public NettyClientBuilder verifyServer(boolean verifyServer) {
    this.verifyServer = verifyServer;
    return this;
  }

  /** Create the client from this builder. */
  public NettyChannelBuilder build() {
    final NettyChannelBuilder builder;

    switch (location.getUri().getScheme()) {
      case LocationSchemes.GRPC:
      case LocationSchemes.GRPC_INSECURE:
      case LocationSchemes.GRPC_TLS:
        {
          builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
          break;
        }
      case LocationSchemes.GRPC_DOMAIN_SOCKET:
        {
          // The implementation is platform-specific, so we have to find the classes at runtime
          builder = NettyChannelBuilder.forAddress(location.toSocketAddress());
          try {
            try {
              // Linux
              builder.channelType(
                  Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                      .asSubclass(ServerChannel.class));
              final EventLoopGroup elg =
                  Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                      .asSubclass(EventLoopGroup.class)
                      .getDeclaredConstructor()
                      .newInstance();
              builder.eventLoopGroup(elg);
            } catch (ClassNotFoundException e) {
              // BSD
              builder.channelType(
                  Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                      .asSubclass(ServerChannel.class));
              final EventLoopGroup elg =
                  Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                      .asSubclass(EventLoopGroup.class)
                      .getDeclaredConstructor()
                      .newInstance();
              builder.eventLoopGroup(elg);
            }
          } catch (ClassNotFoundException
              | InstantiationException
              | IllegalAccessException
              | NoSuchMethodException
              | InvocationTargetException e) {
            throw new UnsupportedOperationException(
                "Could not find suitable Netty native transport implementation for domain socket address.");
          }
          break;
        }
      default:
        throw new IllegalArgumentException(
            "Scheme is not supported: " + location.getUri().getScheme());
    }

    if (this.forceTls || LocationSchemes.GRPC_TLS.equals(location.getUri().getScheme())) {
      builder.useTransportSecurity();

      final boolean hasTrustedCerts = this.trustedCertificates != null;
      final boolean hasKeyCertPair = this.clientCertificate != null && this.clientKey != null;
      if (!this.verifyServer && (hasTrustedCerts || hasKeyCertPair)) {
        throw new IllegalArgumentException(
            "FlightClient has been configured to disable server verification, "
                + "but certificate options have been specified.");
      }

      final SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();

      if (!this.verifyServer) {
        sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
      } else if (this.trustedCertificates != null
          || this.clientCertificate != null
          || this.clientKey != null) {
        if (this.trustedCertificates != null) {
          sslContextBuilder.trustManager(this.trustedCertificates);
        }
        if (this.clientCertificate != null && this.clientKey != null) {
          sslContextBuilder.keyManager(this.clientCertificate, this.clientKey);
        }
      }
      try {
        builder.sslContext(sslContextBuilder.build());
      } catch (SSLException e) {
        throw new RuntimeException(e);
      }

      if (this.overrideHostname != null) {
        builder.overrideAuthority(this.overrideHostname);
      }
    } else {
      builder.usePlaintext();
    }

    builder
        .maxTraceEvents(MAX_CHANNEL_TRACE_EVENTS)
        .maxInboundMessageSize(maxInboundMessageSize)
        .maxInboundMetadataSize(maxInboundMessageSize);
    return builder;
  }
}
