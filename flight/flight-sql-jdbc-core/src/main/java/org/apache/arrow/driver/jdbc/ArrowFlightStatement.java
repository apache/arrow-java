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

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.Meta.StatementType;

/** A SQL statement for querying data from an Arrow Flight server. */
public class ArrowFlightStatement extends AvaticaStatement implements ArrowFlightMetaStatement {

  ArrowFlightStatement(
      final ArrowFlightConnection connection,
      final StatementHandle handle,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability) {
    super(connection, handle, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final String query,
      final long maxRowCount,
      final int maxRowsInFirstFrame,
      final PrepareCallback callback)
      throws SQLException {
    // Keep Avatica Statement.execute(String) behavior: Avatica calls Meta.prepareAndExecute,
    // which resolves to this statement hook.
    this.closeStatement();

    return ArrowFlightPreparedStatement.builder(getConnection())
        .withQuery(query)
        .withExistingStatement(this)
        .build()
        .prepareAndExecute(callback);
  }

  @Override
  public ResultSet executeQuery(final String sql) throws SQLException {
    checkOpen();
    updateCount = -1;
    switchToDirectStatementMode();
    try {
      final Meta.Signature signature =
          ArrowFlightMetaImpl.buildSignature(sql, StatementType.SELECT);
      setSignature(signature);
      return executeQueryInternal(signature, false);
    } catch (Exception exception) {
      throw wrapStatementExecutionException(sql, exception);
    }
  }

  @Override
  public long executeLargeUpdate(final String sql) throws SQLException {
    checkOpen();
    clearOpenResultSet();
    updateCount = -1;
    switchToDirectStatementMode();

    try {
      final long updatedCount = getConnection().getClientHandler().executeUpdate(sql);
      setSignature(ArrowFlightMetaImpl.buildSignature(sql, StatementType.IS_DML));
      updateCount = updatedCount;
      return updatedCount;
    } catch (Exception exception) {
      throw wrapStatementExecutionException(sql, exception);
    }
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final Meta.Signature signature = getSignature();
    if (signature == null) {
      return null;
    }

    // A Statement handle can point to either this direct statement instance or a prepared
    // statement instance created by Avatica Statement.execute(String) through
    // Meta.prepareAndExecute.
    final AvaticaStatement currentStatement = connection.statementMap.get(handle.id);
    if (currentStatement instanceof ArrowFlightPreparedStatement) {
      // Prepared path: reuse the current statement implementation associated with the handle.
      final FlightInfo flightInfo =
          ((ArrowFlightPreparedStatement) currentStatement).executeFlightInfoQuery();
      updateSignatureColumnsFromFlightInfo(signature, flightInfo);
      return flightInfo;
    }

    // Direct Statement.executeQuery(String) / executeUpdate(String) path.
    final FlightInfo flightInfo = connection.getClientHandler().getInfo(signature.sql);
    updateSignatureColumnsFromFlightInfo(signature, flightInfo);
    return flightInfo;
  }

  private void updateSignatureColumnsFromFlightInfo(
      final Meta.Signature signature, final FlightInfo flightInfo) {
    final Schema resultSetSchema = flightInfo.getSchemaOptional().orElse(null);
    if (resultSetSchema != null) {
      signature.columns.addAll(
          ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields()));
      setSignature(signature);
    }
  }

  private SQLException wrapStatementExecutionException(final String sql, final Exception exception)
      throws SQLException {
    if (!(exception instanceof SQLException)) {
      return AvaticaConnection.HELPER.createException(
          "Error while executing SQL \"" + sql + "\": " + exception.getMessage(), exception);
    }
    final SQLException sqlException = (SQLException) exception;
    final String prefix = "Error while executing SQL \"" + sql + "\"";
    final String message = sqlException.getMessage();
    if (message != null && message.startsWith(prefix)) {
      return sqlException;
    }
    final Throwable cause = sqlException.getCause();
    if (cause instanceof FlightRuntimeException) {
      final FlightStatusCode statusCode = ((FlightRuntimeException) cause).status().code();
      if (statusCode == FlightStatusCode.UNAVAILABLE) {
        return sqlException;
      }
    }
    return AvaticaConnection.HELPER.createException(prefix + ": " + message, sqlException);
  }

  private void clearOpenResultSet() throws SQLException {
    synchronized (this) {
      if (openResultSet != null) {
        final AvaticaResultSet resultSet = openResultSet;
        openResultSet = null;
        try {
          resultSet.close();
        } catch (Exception exception) {
          throw AvaticaConnection.HELPER.createException(
              "Error while closing previous result set", exception);
        }
      }
    }
  }

  private void switchToDirectStatementMode() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final AvaticaStatement existingStatement = connection.statementMap.get(handle.id);
    if (existingStatement == this) {
      return;
    }
    if (existingStatement instanceof ArrowFlightPreparedStatement) {
      // Release resources from previously attached statement implementation before switching back
      // to direct statement mode for executeQuery/executeUpdate.
      ((ArrowFlightPreparedStatement) existingStatement).closeStatement();
    }
    connection.statementMap.put(handle.id, this);
  }
}
