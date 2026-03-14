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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.driver.jdbc.utils.ConvertUtils;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.TypedValue;

/** Metadata handler for Arrow Flight. */
public class ArrowFlightMetaImpl extends MetaImpl {
  /**
   * Constructs a {@link MetaImpl} object specific for Arrow Flight.
   *
   * @param connection A {@link AvaticaConnection}.
   */
  public ArrowFlightMetaImpl(final AvaticaConnection connection) {
    super(connection);
    setDefaultConnectionProperties();
  }

  @Override
  public void closeStatement(final StatementHandle statementHandle) {
    AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
    if (statement instanceof ArrowFlightPreparedStatement) {
      ((ArrowFlightPreparedStatement) statement).closePreparedResources();
    }
  }

  @Override
  public void commit(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final long maxRowCount) {
    return getPreparedStatementInstance(statementHandle)
        .executeWithTypedValues(statementHandle, typedValues, maxRowCount);
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final int maxRowsInFirstFrame) {
    return execute(statementHandle, typedValues, (long) maxRowsInFirstFrame);
  }

  @Override
  public ExecuteBatchResult executeBatch(
      final StatementHandle statementHandle, final List<List<TypedValue>> parameterValuesList)
      throws IllegalStateException {
    return getPreparedStatementInstance(statementHandle)
        .executeBatchWithTypedValues(statementHandle, parameterValuesList);
  }

  @Override
  public Frame fetch(
      final StatementHandle statementHandle, final long offset, final int fetchMaxRowCount) {
    /*
     * ArrowFlightMetaImpl does not use frames.
     * Instead, we have accessors that contain a VectorSchemaRoot with
     * the results.
     */
    throw AvaticaConnection.HELPER.wrap(
        String.format("%s does not use frames.", this), AvaticaConnection.HELPER.unsupported());
  }

  ArrowFlightPreparedStatement createPreparedStatement(
      final String query,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    return ArrowFlightPreparedStatement.builder((ArrowFlightConnection) connection)
        .withQuery(query)
        .withGeneratedHandle()
        .withResultSetType(resultSetType)
        .withResultSetConcurrency(resultSetConcurrency)
        .withResultSetHoldability(resultSetHoldability)
        .build();
  }

  @Override
  public StatementHandle prepare(
      final ConnectionHandle connectionHandle, final String query, final long maxRowCount) {
    try {
      return createPreparedStatement(
              query,
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              connection.getHoldability())
          .handle;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle statementHandle,
      final String query,
      final long maxRowCount,
      final PrepareCallback prepareCallback)
      throws NoSuchStatementException {
    return prepareAndExecute(
        statementHandle, query, maxRowCount, -1 /* Not used */, prepareCallback);
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final StatementHandle handle,
      final String query,
      final long maxRowCount,
      final int maxRowsInFirstFrame,
      final PrepareCallback callback)
      throws NoSuchStatementException {
    try {
      final AvaticaStatement statement = connection.statementMap.get(handle.id);
      if (!(statement instanceof ArrowFlightStatement)
          && !(statement instanceof ArrowFlightPreparedStatement)) {
        throw new IllegalStateException("Prepared statement not found: " + handle);
      }
      if (statement instanceof ArrowFlightPreparedStatement) {
        ((ArrowFlightPreparedStatement) statement).closePreparedResources();
      }
      final ArrowFlightPreparedStatement preparedStatement =
          ArrowFlightPreparedStatement.builder((ArrowFlightConnection) connection)
              .withQuery(query)
              .withExistingStatement(statement)
              .build();
      return preparedStatement.prepareAndExecute(callback);
    } catch (SQLTimeoutException e) {
      // So far AvaticaStatement(executeInternal) only handles NoSuchStatement and
      // Runtime
      // Exceptions.
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new NoSuchStatementException(handle);
    }
  }

  @Override
  public ExecuteBatchResult prepareAndExecuteBatch(
      final StatementHandle statementHandle, final List<String> queries)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public void rollback(final ConnectionHandle connectionHandle) {
    // TODO Fill this stub.
  }

  @Override
  public boolean syncResults(
      final StatementHandle statementHandle, final QueryState queryState, final long offset)
      throws NoSuchStatementException {
    // TODO Fill this stub.
    return false;
  }

  @Override
  public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
    final ConnectionProperties result = super.connectionSync(ch, connProps);
    final String newCatalog = this.connProps.getCatalog();
    if (newCatalog != null) {
      try {
        ((ArrowFlightConnection) connection).getClientHandler().setCatalog(newCatalog);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  void setDefaultConnectionProperties() {
    // TODO Double-check this.
    connProps
        .setDirty(false)
        .setAutoCommit(true)
        .setReadOnly(true)
        .setCatalog(null)
        .setSchema(null)
        .setTransactionIsolation(Connection.TRANSACTION_NONE);
  }

  private ArrowFlightPreparedStatement getPreparedStatementInstance(
      StatementHandle statementHandle) {
    AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
    if (!(statement instanceof ArrowFlightPreparedStatement)) {
      throw new IllegalStateException("Prepared statement not found: " + statementHandle);
    }
    return (ArrowFlightPreparedStatement) statement;
  }

  ArrowFlightPreparedStatement getPreparedStatementInstanceOrNull(StatementHandle statementHandle) {
    AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
    if (statement instanceof ArrowFlightPreparedStatement) {
      return (ArrowFlightPreparedStatement) statement;
    }
    return null;
  }

  public static Signature buildDefaultSignature() {
    return buildSignature(null, null, null);
  }

  /** Builds an Avatica signature from Arrow result and parameter schemas. */
  public static Signature buildSignature(
      final String sql, final Schema resultSetSchema, final Schema parameterSchema) {
    List<ColumnMetaData> columnMetaData =
        resultSetSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields());
    List<AvaticaParameter> parameters =
        parameterSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields());
    StatementType statementType =
        resultSetSchema == null || resultSetSchema.getFields().isEmpty()
            ? StatementType.IS_DML
            : StatementType.SELECT;
    return new Signature(
        columnMetaData,
        sql,
        parameters,
        Collections.emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        statementType);
  }
}
