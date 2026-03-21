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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;

/** Arrow Flight JBCS's implementation {@link PreparedStatement}. */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement
    implements ArrowFlightInfoStatement {

  private final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement;
  private final Map<Integer, Timestamp> rawTimestamps = new HashMap<>();

  private ArrowFlightPreparedStatement(
      final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement,
      final StatementHandle handle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.preparedStatement = Preconditions.checkNotNull(preparedStatement);
  }

  static ArrowFlightPreparedStatement newPreparedStatement(
      final ArrowFlightConnection connection,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStmt,
      final StatementHandle statementHandle,
      final Signature signature,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    return new ArrowFlightPreparedStatement(
        connection,
        preparedStmt,
        statementHandle,
        signature,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public synchronized void close() throws SQLException {
    this.preparedStatement.close();
    super.close();
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    if (x != null) {
      rawTimestamps.put(parameterIndex, (Timestamp) x.clone());
    } else {
      rawTimestamps.remove(parameterIndex);
    }
    super.setTimestamp(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    if (x != null) {
      rawTimestamps.put(parameterIndex, (Timestamp) x.clone());
    } else {
      rawTimestamps.remove(parameterIndex);
    }
    super.setTimestamp(parameterIndex, x, cal);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    rawTimestamps.remove(parameterIndex);
    super.setObject(parameterIndex, x, targetSqlType);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    rawTimestamps.remove(parameterIndex);
    super.setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    rawTimestamps.remove(parameterIndex);
    super.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
  }

  @Override
  public void clearParameters() throws SQLException {
    rawTimestamps.clear();
    super.clearParameters();
  }

  /**
   * Returns the raw java.sql.Timestamp objects set via setTimestamp(), keyed by 1-based parameter
   * index. These preserve sub-millisecond precision (getNanos()) that Avatica's TypedValue
   * serialization discards.
   */
  Map<Integer, Timestamp> getRawTimestamps() {
    return Collections.unmodifiableMap(rawTimestamps);
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    return preparedStatement.executeQuery();
  }
}
