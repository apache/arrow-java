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
    getMetaStatement(statementHandle).closeStatement();
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
    return getMetaStatement(statementHandle).execute(statementHandle, typedValues, maxRowCount);
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
    return getMetaStatement(statementHandle).executeBatch(statementHandle, parameterValuesList);
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

  @Override
  public StatementHandle prepare(
      final ConnectionHandle connectionHandle, final String query, final long maxRowCount) {
    try {
      // This is the Avatica entry point used by Connection.prepareStatement(String).
      ArrowFlightPreparedStatement stmt =
          (ArrowFlightPreparedStatement)
              connection.prepareStatement(
                  query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      return stmt.handle;
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
    // This is the Avatica entry point used by Statement.execute(String).
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
      // This is the Avatica entry point used by Statement.execute(String).
      return getMetaStatement(handle)
          .prepareAndExecute(query, maxRowCount, maxRowsInFirstFrame, callback);
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

  private ArrowFlightMetaStatement getMetaStatement(StatementHandle statementHandle) {
    AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
    if (statement instanceof ArrowFlightMetaStatement) {
      return (ArrowFlightMetaStatement) statement;
    }
    throw new IllegalStateException("Statement not found: " + statementHandle);
  }

  public static Signature buildDefaultSignature() {
    return buildSignature(null, StatementType.SELECT);
  }

  public static Signature buildSignature(final String sql, final StatementType type) {
    return buildSignature(sql, null, null, type);
  }

  /** Builds an Avatica signature from Arrow result and parameter schemas. */
  public static Signature buildSignature(
      final String sql, final Schema resultSetSchema, final Schema parameterSchema) {
    StatementType statementType =
        resultSetSchema == null || resultSetSchema.getFields().isEmpty()
            ? StatementType.IS_DML
            : StatementType.SELECT;
    return buildSignature(sql, resultSetSchema, parameterSchema, statementType);
  }

  private static Signature buildSignature(
      final String sql,
      final Schema resultSetSchema,
      final Schema parameterSchema,
      final StatementType statementType) {
    List<ColumnMetaData> columnMetaData =
        resultSetSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToColumnMetaDataList(resultSetSchema.getFields());
    List<AvaticaParameter> parameters =
        parameterSchema == null
            ? new ArrayList<>()
            : ConvertUtils.convertArrowFieldsToAvaticaParameters(parameterSchema.getFields());
    return new Signature(
        columnMetaData,
        sql,
        parameters,
        Collections.emptyMap(),
        null, // unnecessary, as SQL requests use ArrowFlightJdbcCursor
        statementType);
  }
}
