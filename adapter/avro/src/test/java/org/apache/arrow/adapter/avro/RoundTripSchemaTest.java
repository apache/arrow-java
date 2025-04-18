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
package org.apache.arrow.adapter.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

public class RoundTripSchemaTest {

  private void doRoundTripTest(List<Field> fields) {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), false);

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Compare string representations - equality not defined for logical types
    assertEquals(fields, arrowSchema.getFields());
  }

  // Schema round trip for primitive types, nullable and non-nullable

  @Test
  public void testRoundTripNullType() {

    List<Field> fields =
        Arrays.asList(new Field("nullType", FieldType.notNullable(new ArrowType.Null()), null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripBooleanType() {

    List<Field> fields =
        Arrays.asList(
            new Field("nullableBool", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("nonNullableBool", FieldType.notNullable(new ArrowType.Bool()), null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripIntegerTypes() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), false);

    // Only round trip types with direct equivalent in Avro

    List<Field> fields =
        Arrays.asList(
            new Field("nullableInt32", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("nonNullableInt32", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("nullableInt64", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field(
                "nonNullableInt64", FieldType.notNullable(new ArrowType.Int(64, true)), null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripFloatingPointTypes() {

    // Only round trip types with direct equivalent in Avro

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableFloat32",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                null),
            new Field(
                "nonNullableFloat32",
                FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                null),
            new Field(
                "nullableFloat64",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null),
            new Field(
                "nonNullableFloat64",
                FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripStringTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field("nullableUtf8", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("nonNullableUtf8", FieldType.notNullable(new ArrowType.Utf8()), null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripBinaryTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field("nullableBinary", FieldType.nullable(new ArrowType.Binary()), null),
            new Field("nonNullableBinary", FieldType.notNullable(new ArrowType.Binary()), null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripFixedSizeBinaryTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableFixedSizeBinary",
                FieldType.nullable(new ArrowType.FixedSizeBinary(10)),
                null),
            new Field(
                "nonNullableFixedSizeBinary",
                FieldType.notNullable(new ArrowType.FixedSizeBinary(10)),
                null));

    doRoundTripTest(fields);
  }

  // Schema round trip for logical types, nullable and non-nullable

  @Test
  public void testRoundTripDecimalTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableDecimal128", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
            new Field(
                "nonNullableDecimal1281",
                FieldType.notNullable(new ArrowType.Decimal(10, 2, 128)),
                null),
            new Field(
                "nonNullableDecimal1282",
                FieldType.notNullable(new ArrowType.Decimal(15, 5, 128)),
                null),
            new Field(
                "nonNullableDecimal1283",
                FieldType.notNullable(new ArrowType.Decimal(20, 10, 128)),
                null),
            new Field(
                "nullableDecimal256", FieldType.nullable(new ArrowType.Decimal(55, 15, 256)), null),
            new Field(
                "nonNullableDecimal2561",
                FieldType.notNullable(new ArrowType.Decimal(55, 25, 256)),
                null),
            new Field(
                "nonNullableDecimal2562",
                FieldType.notNullable(new ArrowType.Decimal(25, 8, 256)),
                null),
            new Field(
                "nonNullableDecimal2563",
                FieldType.notNullable(new ArrowType.Decimal(60, 50, 256)),
                null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripDateTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableDateDay", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
            new Field(
                "nonNullableDateDay",
                FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)),
                null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripTimeTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableTimeMillis",
                FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
                null),
            new Field(
                "nonNullableTimeMillis",
                FieldType.notNullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
                null),
            new Field(
                "nullableTimeMicros",
                FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                null),
            new Field(
                "nonNullableTimeMicros",
                FieldType.notNullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripZoneAwareTimestampTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableTimestampMillisTz",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                null),
            new Field(
                "nonNullableTimestampMillisTz",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                null),
            new Field(
                "nullableTimestampMicrosTz",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                null),
            new Field(
                "nonNullableTimestampMicrosTz",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                null),
            new Field(
                "nullableTimestampNanosTz",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                null),
            new Field(
                "nonNullableTimestampNanosTz",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                null));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripLocalTimestampTypes() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableTimestampMillis",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                null),
            new Field(
                "nonNullableTimestampMillis",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                null),
            new Field(
                "nullableTimestampMicros",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                null),
            new Field(
                "nonNullableTimestampMicros",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
                null),
            new Field(
                "nullableTimestampNanos",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
                null),
            new Field(
                "nonNullableTimestampNanos",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
                null));

    doRoundTripTest(fields);
  }

  // Schema round trip for complex types, where the contents are primitive and logical types

  @Test
  public void testRoundTripListType() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableIntList",
                FieldType.nullable(new ArrowType.List()),
                Arrays.asList(
                    new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null))),
            new Field(
                "nullableDoubleList",
                FieldType.nullable(new ArrowType.List()),
                Arrays.asList(
                    new Field(
                        "$data$",
                        FieldType.notNullable(
                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null))),
            new Field(
                "nonNullableDecimalList",
                FieldType.notNullable(new ArrowType.List()),
                Arrays.asList(
                    new Field(
                        "$data$", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null))),
            new Field(
                "nonNullableTimestampList",
                FieldType.notNullable(new ArrowType.List()),
                Arrays.asList(
                    new Field(
                        "$data$",
                        FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                        null))));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripMapType() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableMapWithNullableInt",
                FieldType.nullable(new ArrowType.Map(false)),
                Arrays.asList(
                    new Field(
                        "entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        Arrays.asList(
                            new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                            new Field(
                                "value", FieldType.nullable(new ArrowType.Int(32, true)), null))))),
            new Field(
                "nullableMapWithNonNullableDouble",
                FieldType.nullable(new ArrowType.Map(false)),
                Arrays.asList(
                    new Field(
                        "entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        Arrays.asList(
                            new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                            new Field(
                                "value",
                                FieldType.notNullable(
                                    new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                                null))))),
            new Field(
                "nonNullableMapWithNullableDecimal",
                FieldType.notNullable(new ArrowType.Map(false)),
                Arrays.asList(
                    new Field(
                        "entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        Arrays.asList(
                            new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                            new Field(
                                "value",
                                FieldType.nullable(new ArrowType.Decimal(10, 2, 128)),
                                null))))),
            new Field(
                "nonNullableMapWithNonNullableTimestamp",
                FieldType.notNullable(new ArrowType.Map(false)),
                Arrays.asList(
                    new Field(
                        "entries",
                        FieldType.notNullable(new ArrowType.Struct()),
                        Arrays.asList(
                            new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                            new Field(
                                "value",
                                FieldType.notNullable(
                                    new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                                null))))));

    doRoundTripTest(fields);
  }

  @Test
  public void testRoundTripStructType() {

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableRecord",
                FieldType.nullable(new ArrowType.Struct()),
                Arrays.asList(
                    new Field("field1", FieldType.nullable(new ArrowType.Int(32, true)), null),
                    new Field(
                        "field2",
                        FieldType.notNullable(
                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null),
                    new Field(
                        "field3", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
                    new Field(
                        "field4",
                        FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                        null))),
            new Field(
                "nonNullableRecord",
                FieldType.notNullable(new ArrowType.Struct()),
                Arrays.asList(
                    new Field("field1", FieldType.nullable(new ArrowType.Int(32, true)), null),
                    new Field(
                        "field2",
                        FieldType.notNullable(
                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null),
                    new Field(
                        "field3", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
                    new Field(
                        "field4",
                        FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                        null))));

    doRoundTripTest(fields);
  }
}
