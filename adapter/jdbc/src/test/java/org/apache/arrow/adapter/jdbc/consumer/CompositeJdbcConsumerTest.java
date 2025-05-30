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
package org.apache.arrow.adapter.jdbc.consumer;

import org.apache.arrow.adapter.jdbc.consumer.exceptions.JdbcConsumerException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class CompositeJdbcConsumerTest {
    // Faulty consumer that simulates a runtime exception during consume()
    // This is used to test that the CompositeJdbcConsumer wraps it properly
    static class FaultyConsumer extends BaseConsumer {
        public FaultyConsumer(ValueVector vector, int index) {
            super(vector, index);
        }

        @Override
        public void consume(ResultSet rs) throws SQLException {
            throw new NullPointerException("Simulating NPE");
        }
    }

    @Test
    public void testHandlesJdbcConsumerExceptionGracefully() throws SQLException {
        // Setup: create an IntVector to simulate a consumer with a valid vector
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        IntVector intVector = new IntVector("int", allocator);
        intVector.allocateNew();

        // Simulate a failing consumer with valid vector (to test arrowType extraction)
        JdbcConsumer mockConsumer = new FaultyConsumer(intVector, 1);
        CompositeJdbcConsumer composite = new CompositeJdbcConsumer(new JdbcConsumer[]{mockConsumer});

        // Use null ResultSet to simulate failure scenario
        ResultSet dummyRs = null;

        // Verify: the failure is caught and wrapped in JdbcConsumerException
        JdbcConsumerException thrownEx = assertThrows(JdbcConsumerException.class, () -> {
            composite.consume(dummyRs);
        });
        assertTrue(thrownEx.getMessage().contains("Exception while consuming JDBC value"));
        assertNull(thrownEx.getFieldInfo());
        assertNotNull(thrownEx.getArrowType()); // Should be non-null since vector was valid
    }

    // Faulty consumer that has a null ValueVector (to test arrowType = null)
    public static class FaultyConsumerWIthNullVector extends BaseConsumer {
        public FaultyConsumerWIthNullVector(int index) {
            super(null, index);
        }

        @Override
        public void consume(ResultSet rs) throws SQLException {
            throw new NullPointerException("Simulating NPE");
        }
    }

    @Test
    public void testJdbcConsumerExceptionWhenArrowTypeIsNull() throws SQLException {
        // Setup: consumer with null vector
        JdbcConsumer mockConsumer = new FaultyConsumerWIthNullVector(1);
        CompositeJdbcConsumer composite = new CompositeJdbcConsumer(new JdbcConsumer[]{mockConsumer});
        ResultSet dummyRs = null;

        // Verify: when the consumer fails and the vector is null,
        // arrowType in JdbcConsumerException should also be null
        JdbcConsumerException thrownEx = assertThrows(JdbcConsumerException.class, () -> {
            composite.consume(dummyRs);
        });
        assertNull(thrownEx.getArrowType());
    }
}
