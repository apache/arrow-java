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
package org.apache.arrow.driver.jdbc.client.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Tests for {@link OAuthCredentialWriter}. */
@ExtendWith(MockitoExtension.class)
public class OAuthCredentialWriterTest {

  @Mock private OAuthTokenProvider mockTokenProvider;

  @Test
  public void testConstructorRejectsNullTokenProvider() {
    assertThrows(NullPointerException.class, () -> new OAuthCredentialWriter(null));
  }

  @Test
  public void testAcceptWritesBearerTokenToHeaders() throws SQLException {
    String testToken = "test-access-token-12345";
    when(mockTokenProvider.getValidToken()).thenReturn(testToken);

    OAuthCredentialWriter writer = new OAuthCredentialWriter(mockTokenProvider);
    CallHeaders headers = new FlightCallHeaders();

    writer.accept(headers);

    verify(mockTokenProvider).getValidToken();
    assertEquals(
        Auth2Constants.BEARER_PREFIX + testToken, headers.get(Auth2Constants.AUTHORIZATION_HEADER));
  }

  @Test
  public void testAcceptThrowsOAuthTokenExceptionOnSQLException() throws SQLException {
    SQLException sqlException = new SQLException("Token fetch failed");
    when(mockTokenProvider.getValidToken()).thenThrow(sqlException);

    OAuthCredentialWriter writer = new OAuthCredentialWriter(mockTokenProvider);
    CallHeaders headers = new FlightCallHeaders();

    OAuthTokenException exception =
        assertThrows(OAuthTokenException.class, () -> writer.accept(headers));

    assertEquals("Failed to obtain OAuth token", exception.getMessage());
    assertEquals(sqlException, exception.getCause());
  }

  @Test
  public void testAcceptCallsTokenProviderEachTime() throws SQLException {
    when(mockTokenProvider.getValidToken())
        .thenReturn("token1")
        .thenReturn("token2")
        .thenReturn("token3");

    OAuthCredentialWriter writer = new OAuthCredentialWriter(mockTokenProvider);

    CallHeaders headers1 = new FlightCallHeaders();
    writer.accept(headers1);
    assertEquals("Bearer token1", headers1.get(Auth2Constants.AUTHORIZATION_HEADER));

    CallHeaders headers2 = new FlightCallHeaders();
    writer.accept(headers2);
    assertEquals("Bearer token2", headers2.get(Auth2Constants.AUTHORIZATION_HEADER));

    CallHeaders headers3 = new FlightCallHeaders();
    writer.accept(headers3);
    assertEquals("Bearer token3", headers3.get(Auth2Constants.AUTHORIZATION_HEADER));
  }
}
