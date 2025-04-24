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
package org.apache.arrow.driver.jdbc.accessor.impl.calendar;

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorGetter.Getter;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorGetter.Holder;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorGetter.createGetter;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DateUtility;

/** Accessor for the Arrow types extending from {@link TimeStampVector}. */
public class ArrowFlightJdbcTimeStampVectorAccessor extends ArrowFlightJdbcAccessor {

  private final TimeZone timeZone;
  private final Getter getter;
  private final TimeUnit timeUnit;
  private final LongToLocalDateTime longToLocalDateTime;
  private final Holder holder;
  private final boolean isZoned;

  /** Functional interface used to convert a number (in any time resolution) to LocalDateTime. */
  interface LongToLocalDateTime {
    LocalDateTime fromLong(long value);
  }

  /** Instantiate a ArrowFlightJdbcTimeStampVectorAccessor for given vector. */
  public ArrowFlightJdbcTimeStampVectorAccessor(
      TimeStampVector vector,
      IntSupplier currentRowSupplier,
      ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.holder = new Holder();
    this.getter = createGetter(vector);

    // whether the vector included TZ info
    this.isZoned = getVectorIsZoned(vector);
    // non-null, either the vector TZ or default to UTC
    this.timeZone = getTimeZoneForVector(vector);
    this.timeUnit = getTimeUnitForVector(vector);
    this.longToLocalDateTime = getLongToLocalDateTimeForVector(vector, this.timeZone);
  }

  @Override
  public Class<?> getObjectClass() {
    return Timestamp.class;
  }

  @Override
  public <T> T getObject(final Class<T> type) throws SQLException {
    final Object value;
    if (!this.isZoned
        & Set.of(OffsetDateTime.class, ZonedDateTime.class, Instant.class).contains(type)) {
      throw new SQLException(
          "Vectors without timezones can't be converted to objects with offset/tz info.");
    } else if (type == OffsetDateTime.class) {
      value = getOffsetDateTime();
    } else if (type == LocalDateTime.class) {
      value = getLocalDateTime(null);
    } else if (type == ZonedDateTime.class) {
      value = getZonedDateTime();
    } else if (type == Instant.class) {
      value = getInstant();
    } else if (type == Timestamp.class) {
      value = getObject();
    } else {
      throw new SQLException("Object type not supported for TimeStamp Vector");
    }

    return !type.isPrimitive() && wasNull ? null : type.cast(value);
  }

  @Override
  public Object getObject() {
    return this.getTimestamp(null);
  }

  private ZonedDateTime getZonedDateTime() {
    LocalDateTime localDateTime = getLocalDateTime(null);
    if (localDateTime == null) {
      return null;
    }

    return localDateTime.atZone(this.timeZone.toZoneId());
  }

  private OffsetDateTime getOffsetDateTime() {
    LocalDateTime localDateTime = getLocalDateTime(null);
    if (localDateTime == null) {
      return null;
    }
    ZoneOffset offset = this.timeZone.toZoneId().getRules().getOffset(localDateTime);
    return localDateTime.atOffset(offset);
  }

  private Instant getInstant() {
    LocalDateTime localDateTime = getLocalDateTime(null);
    if (localDateTime == null) {
      return null;
    }
    ZoneOffset offset = this.timeZone.toZoneId().getRules().getOffset(localDateTime);
    return localDateTime.toInstant(offset);
  }

  private LocalDateTime getLocalDateTime(Calendar calendar) {
    getter.get(getCurrentRow(), holder);
    this.wasNull = holder.isSet == 0;
    this.wasNullConsumer.setWasNull(this.wasNull);
    if (this.wasNull) {
      return null;
    }

    long value = holder.value;

    LocalDateTime localDateTime = this.longToLocalDateTime.fromLong(value);

    // Adjust timestamp to desired calendar (if provided) only if the column includes TZ info,
    // otherwise treat as wall-clock time
    if (calendar != null && this.isZoned) {
      TimeZone timeZone = calendar.getTimeZone();
      long millis = this.timeUnit.toMillis(value);
      localDateTime =
          localDateTime.minus(
              timeZone.getOffset(millis) - this.timeZone.getOffset(millis), ChronoUnit.MILLIS);
    }
    return localDateTime;
  }

  @Override
  public Date getDate(Calendar calendar) {
    LocalDateTime localDateTime = getLocalDateTime(calendar);
    if (localDateTime == null) {
      return null;
    }

    return new Date(getTimestampWithOffset(calendar, localDateTime).getTime());
  }

  @Override
  public Time getTime(Calendar calendar) {
    LocalDateTime localDateTime = getLocalDateTime(calendar);
    if (localDateTime == null) {
      return null;
    }

    return new Time(getTimestampWithOffset(calendar, localDateTime).getTime());
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) {
    LocalDateTime localDateTime = getLocalDateTime(calendar);
    if (localDateTime == null) {
      return null;
    }

    return getTimestampWithOffset(calendar, localDateTime);
  }

  /**
   * Apply offset to LocalDateTime to get a Timestamp with legacy behavior. Previously we applied
   * the offset to the LocalDateTime even if the underlying Vector did not have a TZ. In order to
   * support java.time.* accessors, we fixed this so we only apply the offset if the underlying
   * vector includes TZ info. In order to maintain backward compatibility, we apply the offset if
   * needed for getDate, getTime, and getTimestamp.
   */
  private Timestamp getTimestampWithOffset(Calendar calendar, LocalDateTime localDateTime) {
    if (calendar != null && !isZoned) {
      TimeZone timeZone = calendar.getTimeZone();
      long millis = Timestamp.valueOf(localDateTime).getTime();
      localDateTime =
          localDateTime.minus(
              timeZone.getOffset(millis) - this.timeZone.getOffset(millis), ChronoUnit.MILLIS);
    }
    return Timestamp.valueOf(localDateTime);
  }

  protected static TimeUnit getTimeUnitForVector(TimeStampVector vector) {
    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    switch (arrowType.getUnit()) {
      case NANOSECOND:
        return TimeUnit.NANOSECONDS;
      case MICROSECOND:
        return TimeUnit.MICROSECONDS;
      case MILLISECOND:
        return TimeUnit.MILLISECONDS;
      case SECOND:
        return TimeUnit.SECONDS;
      default:
        throw new UnsupportedOperationException("Invalid Arrow time unit");
    }
  }

  protected static LongToLocalDateTime getLongToLocalDateTimeForVector(
      TimeStampVector vector, TimeZone timeZone) {
    String timeZoneID = timeZone.getID();

    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    switch (arrowType.getUnit()) {
      case NANOSECOND:
        return nanoseconds -> DateUtility.getLocalDateTimeFromEpochNano(nanoseconds, timeZoneID);
      case MICROSECOND:
        return microseconds -> DateUtility.getLocalDateTimeFromEpochMicro(microseconds, timeZoneID);
      case MILLISECOND:
        return milliseconds -> DateUtility.getLocalDateTimeFromEpochMilli(milliseconds, timeZoneID);
      case SECOND:
        return seconds ->
            DateUtility.getLocalDateTimeFromEpochMilli(
                TimeUnit.SECONDS.toMillis(seconds), timeZoneID);
      default:
        throw new UnsupportedOperationException("Invalid Arrow time unit");
    }
  }

  protected static TimeZone getTimeZoneForVector(TimeStampVector vector) {
    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    String timezoneName = Objects.requireNonNullElse(arrowType.getTimezone(), "UTC");
    return TimeZone.getTimeZone(timezoneName);
  }

  protected static boolean getVectorIsZoned(TimeStampVector vector) {
    ArrowType.Timestamp arrowType =
        (ArrowType.Timestamp) vector.getField().getFieldType().getType();

    return arrowType.getTimezone() != null;
  }
}
