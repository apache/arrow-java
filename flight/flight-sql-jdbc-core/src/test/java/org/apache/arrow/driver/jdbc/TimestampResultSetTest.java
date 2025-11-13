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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.TimeZone;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Timestamps have a lot of nuances in JDBC. This class is here to test that timestamp behavior is
 * correct for different types of Timestamp vectors as well as different methods of retrieving the
 * timestamps in JDBC.
 */
public class TimestampResultSetTest {
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();

  @RegisterExtension public static FlightServerTestExtension FLIGHT_SERVER_TEST_EXTENSION;

  static {
    FLIGHT_SERVER_TEST_EXTENSION =
        FlightServerTestExtension.createStandardTestExtension(FLIGHT_SQL_PRODUCER);
  }

  private static final String QUERY_STRING = "SELECT * FROM TIMESTAMPS";
  private static final Schema QUERY_SCHEMA =
      new Schema(
          ImmutableList.of(
              Field.nullable("no_tz", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
              Field.nullable("utc", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
              Field.nullable("utc+1", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "GMT+1")),
              Field.nullable("utc-1", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "GMT-1"))));

  @BeforeAll
  public static void setup() throws SQLException {
    Instant firstDay2025 = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();

    FLIGHT_SQL_PRODUCER.addSelectQuery(
        QUERY_STRING,
        QUERY_SCHEMA,
        Collections.singletonList(
            listener -> {
              try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                  final VectorSchemaRoot root = VectorSchemaRoot.create(QUERY_SCHEMA, allocator)) {
                listener.start(root);
                root.getFieldVectors()
                    .forEach(v -> ((TimeStampVector) v).setSafe(0, firstDay2025.toEpochMilli()));
                root.setRowCount(1);
                listener.putNext();
              } catch (final Throwable throwable) {
                listener.error(throwable);
              } finally {
                listener.completed();
              }
            }));
  }

  /**
   * This test doesn't yet test anything other than ensuring all ResultSet methods to retrieve a
   * timestamp succeed.
   *
   * <p>This is a good starting point to add more tests to ensure the values are correct when we
   * change the "local calendar" either through changing the JVM default or through the connection
   * property.
   */
  @Test
  public void test() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try (Connection connection = FLIGHT_SERVER_TEST_EXTENSION.getConnection("UTC")) {
      try (PreparedStatement s = connection.prepareStatement(QUERY_STRING)) {
        try (ResultSet rs = s.executeQuery()) {
          int numCols = rs.getMetaData().getColumnCount();
          try {
            rs.next();
            for (int i = 1; i <= numCols; i++) {
              int type = rs.getMetaData().getColumnType(i);
              String name = rs.getMetaData().getColumnName(i);
              System.out.println(name);
              System.out.print("- getDate:\t\t\t\t\t\t\t");
              System.out.print(rs.getDate(i));
              System.out.println();
              System.out.print("- getTimestamp:\t\t\t\t\t\t");
              System.out.print(rs.getTimestamp(i));
              System.out.println();
              System.out.print("- getString:\t\t\t\t\t\t");
              System.out.print(rs.getString(i));
              System.out.println();
              System.out.print("- getObject:\t\t\t\t\t\t");
              System.out.print(rs.getObject(i));
              System.out.println();
              System.out.print("- getObject(Timestamp.class):\t\t");
              System.out.print(rs.getObject(i, Timestamp.class));
              System.out.println();
              System.out.print("- getTimestamp(default Calendar):\t");
              System.out.print(rs.getTimestamp(i, Calendar.getInstance()));
              System.out.println();
              System.out.print("- getTimestamp(UTC Calendar):\t\t");
              System.out.print(
                  rs.getTimestamp(i, Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
              System.out.println();
              System.out.print("- getObject(LocalDateTime.class):\t");
              System.out.print(rs.getObject(i, LocalDateTime.class));
              System.out.println();
              if (type == Types.TIMESTAMP_WITH_TIMEZONE) {
                System.out.print("- getObject(Instant.class):\t\t\t");
                System.out.print(rs.getObject(i, Instant.class));
                System.out.println();
                System.out.print("- getObject(OffsetDateTime.class):\t");
                System.out.print(rs.getObject(i, OffsetDateTime.class));
                System.out.println();
                System.out.print("- getObject(ZonedDateTime.class):\t");
                System.out.print(rs.getObject(i, ZonedDateTime.class));
                System.out.println();
              }
              System.out.println();
            }
            System.out.println();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
