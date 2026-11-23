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
package org.apache.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.flight.grpc.RequestContextAdapter;
import org.junit.jupiter.api.Test;

public class TestServerSessionMiddleware {
  private static final String SESSION_ID = "6f1a3c8e-2b4d-4f7a-9c0e-1d2e3f4a5b6c";
  private static final CallInfo CALL_INFO = new CallInfo(FlightMethod.DO_ACTION);

  /** Creates a factory holding one session with {@link #SESSION_ID}. */
  private static ServerSessionMiddleware.Factory factoryWithSession() {
    final ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(() -> SESSION_ID);
    final ServerSessionMiddleware middleware =
        factory.onCallStarted(CALL_INFO, new FlightCallHeaders(), new RequestContextAdapter());
    assertFalse(middleware.hasSession());
    assertEquals(SESSION_ID, middleware.getSession().id);
    return factory;
  }

  private static ServerSessionMiddleware callWithCookie(
      ServerSessionMiddleware.Factory factory, String... cookieHeaders) {
    final FlightCallHeaders headers = new FlightCallHeaders();
    for (String cookieHeader : cookieHeaders) {
      headers.insert("cookie", cookieHeader);
    }
    return factory.onCallStarted(CALL_INFO, headers, new RequestContextAdapter());
  }

  @Test
  public void sessionCookieAlone() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    final ServerSessionMiddleware middleware =
        callWithCookie(factory, ServerSessionMiddleware.sessionCookieName + "=" + SESSION_ID);
    assertTrue(middleware.hasSession());
    assertEquals(SESSION_ID, middleware.getSession().id);
  }

  /** The session cookie is found when other cookies share the header (RFC 6265 "; " list). */
  @Test
  public void sessionCookieAfterOtherCookies() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    final ServerSessionMiddleware middleware =
        callWithCookie(
            factory,
            "AWSALB=abc; "
                + ServerSessionMiddleware.sessionCookieName
                + "="
                + SESSION_ID
                + "; x=y");
    assertTrue(middleware.hasSession());
    assertEquals(SESSION_ID, middleware.getSession().id);
  }

  /** A cookie value may contain '=' (for example base64 padding). */
  @Test
  public void sessionCookieValueWithEquals() {
    final ServerSessionMiddleware.Factory factory =
        new ServerSessionMiddleware.Factory(() -> "c2Vzc2lvbg==");
    final ServerSessionMiddleware created =
        factory.onCallStarted(CALL_INFO, new FlightCallHeaders(), new RequestContextAdapter());
    final ServerSessionMiddleware.Session session = created.getSession();
    final ServerSessionMiddleware middleware =
        callWithCookie(factory, ServerSessionMiddleware.sessionCookieName + "=c2Vzc2lvbg==");
    assertTrue(middleware.hasSession());
    assertSame(session, middleware.getSession());
  }

  /** A malformed entry earlier in the list does not hide the session cookie. */
  @Test
  public void malformedCookieIsSkipped() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    final ServerSessionMiddleware middleware =
        callWithCookie(
            factory, "garbage; " + ServerSessionMiddleware.sessionCookieName + "=" + SESSION_ID);
    assertTrue(middleware.hasSession());
    assertEquals(SESSION_ID, middleware.getSession().id);
  }

  @Test
  public void quotedSessionCookieValue() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    final ServerSessionMiddleware middleware =
        callWithCookie(
            factory, ServerSessionMiddleware.sessionCookieName + "=\"" + SESSION_ID + "\"");
    assertTrue(middleware.hasSession());
    assertEquals(SESSION_ID, middleware.getSession().id);
  }

  @Test
  public void sessionCookieInSecondHeader() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    final ServerSessionMiddleware middleware =
        callWithCookie(
            factory, "other=1", ServerSessionMiddleware.sessionCookieName + "=" + SESSION_ID);
    assertTrue(middleware.hasSession());
    assertEquals(SESSION_ID, middleware.getSession().id);
  }

  @Test
  public void noSessionCookie() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    assertFalse(callWithCookie(factory, "other=1; another=2").hasSession());
    assertFalse(
        callWithCookie(factory, ServerSessionMiddleware.sessionCookieName + "=").hasSession());
  }

  @Test
  public void unknownSessionCookie() {
    final ServerSessionMiddleware.Factory factory = factoryWithSession();
    final FlightRuntimeException e =
        assertThrows(
            FlightRuntimeException.class,
            () -> callWithCookie(factory, ServerSessionMiddleware.sessionCookieName + "=unknown"));
    assertEquals(FlightStatusCode.NOT_FOUND, e.status().code());
  }
}
