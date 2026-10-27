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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.calcite.avatica.Meta.PrepareCallback;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.junit.jupiter.api.Test;

public class ArrowFlightStatementApiTest {

  @Test
  public void testMetaOrchestrationMethodsAreNotPublicStatementApis() {
    assertNotPublic(
        ArrowFlightStatement.class,
        "prepareAndExecute",
        String.class,
        long.class,
        int.class,
        PrepareCallback.class);
    assertNotPublic(
        ArrowFlightPreparedStatement.class,
        "prepareAndExecute",
        String.class,
        long.class,
        int.class,
        PrepareCallback.class);
    assertNotPublic(
        ArrowFlightPreparedStatement.class,
        "execute",
        StatementHandle.class,
        List.class,
        long.class);
    assertNotPublic(
        ArrowFlightPreparedStatement.class, "executeBatch", StatementHandle.class, List.class);
    assertNotPublic(ArrowFlightPreparedStatement.class, "closeStatement");
    assertNotPublic(
        ArrowFlightStatement.class,
        "prepareAndExecuteInternal",
        String.class,
        long.class,
        int.class,
        PrepareCallback.class);
    assertNotPublic(
        ArrowFlightPreparedStatement.class,
        "prepareAndExecuteInternal",
        String.class,
        long.class,
        int.class,
        PrepareCallback.class);
    assertNotPublic(
        ArrowFlightPreparedStatement.class,
        "executeWithTypedValues",
        StatementHandle.class,
        List.class,
        long.class);
    assertNotPublic(
        ArrowFlightPreparedStatement.class,
        "executeBatchWithTypedValues",
        StatementHandle.class,
        List.class);
    assertNotPublic(ArrowFlightPreparedStatement.class, "closePreparedResources");
  }

  private static void assertNotPublic(
      final Class<?> statementClass, final String methodName, final Class<?>... parameterTypes) {
    assertThrows(
        NoSuchMethodException.class,
        () -> statementClass.getMethod(methodName, parameterTypes),
        methodName + " must remain an internal orchestration method");
  }
}
