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

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.driver.jdbc.client.ArrowFlightSqlClientHandler;
import org.apache.arrow.driver.jdbc.utils.AvaticaParameterBinder;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.remote.TypedValue;

/** Arrow Flight JDBC's implementation {@link java.sql.PreparedStatement}. */
public class ArrowFlightPreparedStatement extends AvaticaPreparedStatement
    implements ArrowFlightMetaStatement {

  private ArrowFlightSqlClientHandler.PreparedStatement preparedStatement;

  private ArrowFlightPreparedStatement(
      final ArrowFlightConnection connection,
      final StatementHandle handle,
      final Signature signature,
      final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement,
      final int resultSetType,
      final int resultSetConcurrency,
      final int resultSetHoldability)
      throws SQLException {
    super(connection, handle, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.preparedStatement = Preconditions.checkNotNull(preparedStatement);
    this.handle.signature = signature;
    setSignature(signature);
  }

  static Builder builder(final ArrowFlightConnection connection) {
    return new Builder(connection);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  ExecuteResult prepareAndExecute(final PrepareCallback callback) throws SQLException {
    ensurePrepared();
    final StatementType statementType = preparedStatement.getType();
    final long updateCount =
        statementType.equals(StatementType.UPDATE) ? preparedStatement.executeUpdate() : -1;
    synchronized (callback.getMonitor()) {
      callback.clear();
      callback.assign(handle.signature, null, updateCount);
    }
    callback.execute();
    final MetaResultSet metaResultSet =
        MetaResultSet.create(handle.connectionId, handle.id, false, handle.signature, null);
    return new ExecuteResult(Collections.singletonList(metaResultSet));
  }

  @Override
  public ExecuteResult prepareAndExecute(
      final String query,
      final long maxRowCount,
      final int maxRowsInFirstFrame,
      final PrepareCallback callback)
      throws SQLException {

    return ArrowFlightPreparedStatement.builder(getConnection())
        .withQuery(query)
        .withExistingStatement(this)
        .build()
        .prepareAndExecute(callback);
  }

  Schema getDataSetSchema() {
    ensurePrepared();
    return preparedStatement.getDataSetSchema();
  }

  @Override
  public synchronized void close() throws SQLException {
    super.close();
  }

  void closePreparedResources() {
    if (preparedStatement != null) {
      preparedStatement.close();
      preparedStatement = null;
    }
  }

  ExecuteResult executeWithTypedValues(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final long maxRowCount) {
    ensurePrepared();
    Preconditions.checkArgument(
        connection.id.equals(statementHandle.connectionId), "Connection IDs are not consistent");
    new AvaticaParameterBinder(
            preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator())
        .bind(typedValues);

    if (statementHandle.signature == null
        || statementHandle.signature.statementType == StatementType.IS_DML) {
      long updatedCount = preparedStatement.executeUpdate();
      return new ExecuteResult(
          Collections.singletonList(
              MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updatedCount)));
    }

    // TODO Why is maxRowCount ignored?
    return new ExecuteResult(
        Collections.singletonList(
            MetaResultSet.create(
                statementHandle.connectionId,
                statementHandle.id,
                true,
                statementHandle.signature,
                null)));
  }

  ExecuteBatchResult executeBatchWithTypedValues(
      final StatementHandle statementHandle, final List<List<TypedValue>> parameterValuesList) {
    ensurePrepared();
    Preconditions.checkArgument(
        connection.id.equals(statementHandle.connectionId), "Connection IDs are not consistent");
    final AvaticaParameterBinder binder =
        new AvaticaParameterBinder(
            preparedStatement, ((ArrowFlightConnection) connection).getBufferAllocator());
    for (int i = 0; i < parameterValuesList.size(); i++) {
      binder.bind(parameterValuesList.get(i), i);
    }

    long[] updatedCounts = {preparedStatement.executeUpdate()};
    return new ExecuteBatchResult(updatedCounts);
  }

  @Override
  public ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final long maxRowCount) {
    return executeWithTypedValues(statementHandle, typedValues, maxRowCount);
  }

  @Override
  public ExecuteBatchResult executeBatch(
      final StatementHandle statementHandle, final List<List<TypedValue>> parameterValuesList) {
    return executeBatchWithTypedValues(statementHandle, parameterValuesList);
  }

  @Override
  public void closeStatement() {
    closePreparedResources();
  }

  @Override
  public FlightInfo executeFlightInfoQuery() throws SQLException {
    ensurePrepared();
    return preparedStatement.executeQuery();
  }

  private void ensurePrepared() {
    if (preparedStatement == null) {
      throw new IllegalStateException("PreparedStatement is already closed.");
    }
  }

  static final class Builder {
    private final ArrowFlightConnection connection;
    private StatementHandle handle;
    private String query;
    private Integer resultSetType;
    private Integer resultSetConcurrency;
    private Integer resultSetHoldability;
    private boolean generateHandle;

    private Builder(final ArrowFlightConnection connection) {
      this.connection = Preconditions.checkNotNull(connection);
    }

    Builder withQuery(final String query) {
      this.query = Preconditions.checkNotNull(query);
      return this;
    }

    Builder withGeneratedHandle() {
      this.generateHandle = true;
      this.handle = null;
      return this;
    }

    Builder withExistingStatement(final AvaticaStatement statement) throws SQLException {
      Preconditions.checkNotNull(statement);
      this.generateHandle = false;
      this.handle = Preconditions.checkNotNull(statement.handle);
      this.resultSetType = statement.getResultSetType();
      this.resultSetConcurrency = statement.getResultSetConcurrency();
      this.resultSetHoldability = statement.getResultSetHoldability();
      return this;
    }

    Builder withResultSetType(final int resultSetType) {
      this.resultSetType = resultSetType;
      return this;
    }

    Builder withResultSetConcurrency(final int resultSetConcurrency) {
      this.resultSetConcurrency = resultSetConcurrency;
      return this;
    }

    Builder withResultSetHoldability(final int resultSetHoldability) {
      this.resultSetHoldability = resultSetHoldability;
      return this;
    }

    ArrowFlightPreparedStatement build() throws SQLException {
      Preconditions.checkNotNull(query);
      Preconditions.checkNotNull(resultSetType);
      Preconditions.checkNotNull(resultSetConcurrency);
      Preconditions.checkNotNull(resultSetHoldability);
      if (!generateHandle && handle == null) {
        throw new IllegalStateException("PreparedStatement builder requires a handle.");
      }

      final ArrowFlightSqlClientHandler.PreparedStatement preparedStatement =
          connection.getClientHandler().prepare(query);
      final Signature signature =
          ArrowFlightMetaImpl.buildSignature(
              query, preparedStatement.getDataSetSchema(), preparedStatement.getParameterSchema());

      return new ArrowFlightPreparedStatement(
          connection,
          generateHandle ? null : handle,
          signature,
          preparedStatement,
          resultSetType,
          resultSetConcurrency,
          resultSetHoldability);
    }
  }
}
