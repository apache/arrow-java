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
package org.apache.arrow.vector.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.TemporalUnit;
import java.util.Base64;
import java.util.Map;
import org.apache.arrow.vector.PeriodDuration;

/**
 * Serializes arbitrary values to a compact JSON string using only Jackson's streaming API
 * (jackson-core). This replaces the previous use of jackson-databind for the {@code toString()}
 * representations of {@link JsonStringHashMap} and {@link JsonStringArrayList}.
 */
final class JsonStringSerializer {

  private static final JsonFactory FACTORY = new JsonFactory();

  private JsonStringSerializer() {}

  /** Serializes the given value to a compact JSON string. */
  static String serialize(Object value) {
    try (StringWriter writer = new StringWriter();
        JsonGenerator generator = FACTORY.createGenerator(writer)) {
      writeValue(generator, value);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot serialize value to JSON string", e);
    }
  }

  private static void writeValue(JsonGenerator generator, Object value) throws IOException {
    if (value == null) {
      generator.writeNull();
    } else if (value instanceof Map) {
      generator.writeStartObject();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        generator.writeFieldName(String.valueOf(entry.getKey()));
        writeValue(generator, entry.getValue());
      }
      generator.writeEndObject();
    } else if (value instanceof Iterable) {
      generator.writeStartArray();
      for (Object element : (Iterable<?>) value) {
        writeValue(generator, element);
      }
      generator.writeEndArray();
    } else if (value instanceof byte[]) {
      generator.writeString(Base64.getEncoder().encodeToString((byte[]) value));
    } else if (value instanceof Boolean) {
      generator.writeBoolean((Boolean) value);
    } else if (value instanceof BigDecimal) {
      generator.writeNumber((BigDecimal) value);
    } else if (value instanceof BigInteger) {
      generator.writeNumber((BigInteger) value);
    } else if (value instanceof Double) {
      generator.writeNumber((Double) value);
    } else if (value instanceof Float) {
      generator.writeNumber((Float) value);
    } else if (value instanceof Number) {
      generator.writeNumber(((Number) value).longValue());
    } else if (value instanceof LocalDateTime) {
      // ---- BEGIN java.time legacy-compatibility block ----
      // The former toString() path used a jackson-databind ObjectMapper configured with
      // JavaTimeModule (see the deleted ObjectMapperFactory). With WRITE_DATES_AS_TIMESTAMPS
      // enabled (the default), that mapper rendered java.time values in a Jackson-specific
      // numeric form rather than ISO-8601. The three branches below (LocalDateTime, Duration,
      // PeriodDuration) reproduce that exact legacy output so toString() is byte-for-byte
      // unchanged for struct/map/list vectors containing temporal children (timestamp, date,
      // time, duration, interval-day, interval-month-day-nano).
      //
      // TO REVERT to cleaner native ISO-8601 output: delete this entire block (down to the
      // matching END marker) plus the writeLocalDateTime/writeDuration/writePeriodDuration
      // helpers below and their now-unused imports. The generic fallback then emits
      // value.toString(): LocalDateTime -> "2021-01-02T03:04:05", Duration -> "PT1M30S",
      // PeriodDuration -> "P1Y2M3D PT...". RISK: this only changes the human-readable
      // toString()/contentToString() representation used for debugging, logging and display.
      // It does NOT affect vector data, the IPC binary format, or the JSON file wire format
      // (JsonFileWriter emits raw epoch numbers directly, not via this class). The risk is
      // limited to any downstream code or test that string-matches the legacy toString() of
      // temporal values inside complex vectors.
      writeLocalDateTime(generator, (LocalDateTime) value);
    } else if (value instanceof Duration) {
      writeDuration(generator, (Duration) value);
    } else if (value instanceof PeriodDuration) {
      writePeriodDuration(generator, (PeriodDuration) value);
      // ---- END java.time legacy-compatibility block ----
    } else if (value instanceof CharSequence || value instanceof Character) {
      generator.writeString(value.toString());
    } else {
      generator.writeString(value.toString());
    }
  }

  /**
   * Reproduces JavaTimeModule's timestamp serialization of {@link LocalDateTime} as an array {@code
   * [year, month, day, hour, minute(, second(, nano))]}, where the second is omitted when both
   * second and nano are zero, and the nano is omitted when zero. Part of the legacy java.time
   * compatibility block in {@link #writeValue}; see that block's comment to revert.
   */
  private static void writeLocalDateTime(JsonGenerator generator, LocalDateTime value)
      throws IOException {
    generator.writeStartArray();
    generator.writeNumber(value.getYear());
    generator.writeNumber(value.getMonthValue());
    generator.writeNumber(value.getDayOfMonth());
    generator.writeNumber(value.getHour());
    generator.writeNumber(value.getMinute());
    if (value.getSecond() != 0 || value.getNano() != 0) {
      generator.writeNumber(value.getSecond());
      if (value.getNano() != 0) {
        generator.writeNumber(value.getNano());
      }
    }
    generator.writeEndArray();
  }

  /**
   * Reproduces JavaTimeModule's timestamp serialization of {@link Duration} as a decimal number of
   * seconds with nanosecond precision (e.g. {@code 90.000000000}, {@code -1.500000000}), with the
   * special case {@code 0.0} for a zero duration. Part of the legacy java.time compatibility block
   * in {@link #writeValue}; see that block's comment to revert.
   */
  private static void writeDuration(JsonGenerator generator, Duration value) throws IOException {
    long seconds = value.getSeconds();
    int nanos = value.getNano();
    if (seconds == 0 && nanos == 0) {
      generator.writeNumber(BigDecimal.valueOf(0, 1));
    } else {
      generator.writeNumber(BigDecimal.valueOf(seconds).add(BigDecimal.valueOf(nanos, 9)));
    }
  }

  /**
   * Reproduces the former ObjectMapper's (bean) serialization of {@link PeriodDuration} as {@code
   * {"period": "P1Y2M3D", "duration": <seconds>, "units": ["YEARS", ...]}}. The {@code units} array
   * reflects {@link PeriodDuration#getUnits()} and is emitted only to preserve the exact legacy
   * output. Part of the legacy java.time compatibility block in {@link #writeValue}; see that
   * block's comment to revert.
   */
  private static void writePeriodDuration(JsonGenerator generator, PeriodDuration value)
      throws IOException {
    generator.writeStartObject();
    generator.writeStringField("period", value.getPeriod().toString());
    generator.writeFieldName("duration");
    writeDuration(generator, value.getDuration());
    generator.writeArrayFieldStart("units");
    for (TemporalUnit unit : value.getUnits()) {
      generator.writeString(((Enum<?>) unit).name());
    }
    generator.writeEndArray();
    generator.writeEndObject();
  }
}
