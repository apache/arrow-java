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
import java.sql.Statement;
import java.util.List;
import org.apache.arrow.flight.FlightInfo;
import org.apache.calcite.avatica.Meta.ExecuteBatchResult;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.remote.TypedValue;

/** Statement capabilities used by {@link ArrowFlightMetaImpl}. */
interface ArrowFlightMetaStatement extends Statement {

  @Override
  ArrowFlightConnection getConnection() throws SQLException;

  FlightInfo executeFlightInfoQuery() throws SQLException;

  /**
   * Avatica routes {@link Statement#execute(String)} through Meta.prepareAndExecute(...), so plain
   * statements still need this hook even when they support direct executeQuery/executeUpdate paths.
   */
  ExecuteResult prepareAndExecute(
      String query, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback)
      throws SQLException;

  default ExecuteResult execute(
      final StatementHandle statementHandle,
      final List<TypedValue> typedValues,
      final long maxRowCount) {
    throw new IllegalStateException(
        "Statement operation is not supported for handle: " + statementHandle);
  }

  default ExecuteBatchResult executeBatch(
      final StatementHandle statementHandle, final List<List<TypedValue>> parameterValuesList) {
    throw new IllegalStateException(
        "Statement operation is not supported for handle: " + statementHandle);
  }

  default void closeStatement() {}
}
