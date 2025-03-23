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
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

public class ArrowToAvroSchemaTest {

  // Schema conversion for primitive types, nullable and non-nullable

  @Test
  public void testConvertNullType() {
    List<Field> fields =
        Arrays.asList(new Field("nullType", FieldType.notNullable(new ArrowType.Null()), null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(1, schema.getFields().size());

    assertEquals(Schema.Type.NULL, schema.getField("nullType").schema().getType());
  }

  @Test
  public void testConvertBooleanTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field("nullableBool", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("nonNullableBool", FieldType.notNullable(new ArrowType.Bool()), null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(2, schema.getFields().size());

    assertEquals(Schema.Type.UNION, schema.getField("nullableBool").schema().getType());
    assertEquals(2, schema.getField("nullableBool").schema().getTypes().size());
    assertEquals(
        Schema.Type.BOOLEAN, schema.getField("nullableBool").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableBool").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.BOOLEAN, schema.getField("nonNullableBool").schema().getType());
  }

  @Test
  public void testConvertIntegralTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field("nullableInt8", FieldType.nullable(new ArrowType.Int(8, true)), null),
            new Field("nonNullableInt8", FieldType.notNullable(new ArrowType.Int(8, true)), null),
            new Field("nullableUInt8", FieldType.nullable(new ArrowType.Int(8, false)), null),
            new Field("nonNullableUInt8", FieldType.notNullable(new ArrowType.Int(8, false)), null),
            new Field("nullableInt16", FieldType.nullable(new ArrowType.Int(16, true)), null),
            new Field("nonNullableInt16", FieldType.notNullable(new ArrowType.Int(16, true)), null),
            new Field("nullableUInt16", FieldType.nullable(new ArrowType.Int(16, false)), null),
            new Field(
                "nonNullableUInt16", FieldType.notNullable(new ArrowType.Int(16, false)), null),
            new Field("nullableInt32", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("nonNullableInt32", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("nullableUInt32", FieldType.nullable(new ArrowType.Int(32, false)), null),
            new Field(
                "nonNullableUInt32", FieldType.notNullable(new ArrowType.Int(32, false)), null),
            new Field("nullableInt64", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("nonNullableInt64", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("nullableUInt64", FieldType.nullable(new ArrowType.Int(64, false)), null),
            new Field(
                "nonNullableUInt64", FieldType.notNullable(new ArrowType.Int(64, false)), null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(16, schema.getFields().size());

    assertEquals(Schema.Type.UNION, schema.getField("nullableInt8").schema().getType());
    assertEquals(2, schema.getField("nullableInt8").schema().getTypes().size());
    assertEquals(
        Schema.Type.INT, schema.getField("nullableInt8").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableInt8").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.INT, schema.getField("nonNullableInt8").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableUInt8").schema().getType());
    assertEquals(2, schema.getField("nullableUInt8").schema().getTypes().size());
    assertEquals(
        Schema.Type.INT, schema.getField("nullableUInt8").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableUInt8").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.INT, schema.getField("nonNullableUInt8").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableInt16").schema().getType());
    assertEquals(2, schema.getField("nullableInt16").schema().getTypes().size());
    assertEquals(
        Schema.Type.INT, schema.getField("nullableInt16").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableInt16").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.INT, schema.getField("nonNullableInt16").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableUInt16").schema().getType());
    assertEquals(2, schema.getField("nullableUInt16").schema().getTypes().size());
    assertEquals(
        Schema.Type.INT, schema.getField("nullableUInt16").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableUInt16").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.INT, schema.getField("nonNullableUInt16").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableInt32").schema().getType());
    assertEquals(2, schema.getField("nullableInt32").schema().getTypes().size());
    assertEquals(
        Schema.Type.INT, schema.getField("nullableInt32").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableInt32").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.INT, schema.getField("nonNullableInt32").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableUInt32").schema().getType());
    assertEquals(2, schema.getField("nullableUInt32").schema().getTypes().size());
    assertEquals(
        Schema.Type.LONG, schema.getField("nullableUInt32").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableUInt32").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.LONG, schema.getField("nonNullableUInt32").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableInt64").schema().getType());
    assertEquals(2, schema.getField("nullableInt64").schema().getTypes().size());
    assertEquals(
        Schema.Type.LONG, schema.getField("nullableInt64").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableInt64").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.LONG, schema.getField("nonNullableInt64").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableUInt64").schema().getType());
    assertEquals(2, schema.getField("nullableUInt64").schema().getTypes().size());
    assertEquals(
        Schema.Type.LONG, schema.getField("nullableUInt64").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableUInt64").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.LONG, schema.getField("nonNullableUInt64").schema().getType());
  }

  @Test
  public void testConvertFloatingPointTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableFloat16",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
                null),
            new Field(
                "nonNullableFloat16",
                FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
                null),
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

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(6, schema.getFields().size());

    assertEquals(Schema.Type.UNION, schema.getField("nullableFloat16").schema().getType());
    assertEquals(2, schema.getField("nullableFloat16").schema().getTypes().size());
    assertEquals(
        Schema.Type.FLOAT, schema.getField("nullableFloat16").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableFloat16").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.FLOAT, schema.getField("nonNullableFloat16").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableFloat32").schema().getType());
    assertEquals(2, schema.getField("nullableFloat32").schema().getTypes().size());
    assertEquals(
        Schema.Type.FLOAT, schema.getField("nullableFloat32").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableFloat32").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.FLOAT, schema.getField("nonNullableFloat32").schema().getType());

    assertEquals(Schema.Type.UNION, schema.getField("nullableFloat64").schema().getType());
    assertEquals(2, schema.getField("nullableFloat64").schema().getTypes().size());
    assertEquals(
        Schema.Type.DOUBLE,
        schema.getField("nullableFloat64").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableFloat64").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.DOUBLE, schema.getField("nonNullableFloat64").schema().getType());
  }

  @Test
  public void testConvertStringTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field("nullableUtf8", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("nonNullableUtf8", FieldType.notNullable(new ArrowType.Utf8()), null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(2, schema.getFields().size());

    assertEquals(Schema.Type.UNION, schema.getField("nullableUtf8").schema().getType());
    assertEquals(2, schema.getField("nullableUtf8").schema().getTypes().size());
    assertEquals(
        Schema.Type.STRING, schema.getField("nullableUtf8").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableUtf8").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.STRING, schema.getField("nonNullableUtf8").schema().getType());
  }

  @Test
  public void testConvertBinaryTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field("nullableBinary", FieldType.nullable(new ArrowType.Binary()), null),
            new Field("nonNullableBinary", FieldType.notNullable(new ArrowType.Binary()), null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(2, schema.getFields().size());

    assertEquals(Schema.Type.UNION, schema.getField("nullableBinary").schema().getType());
    assertEquals(2, schema.getField("nullableBinary").schema().getTypes().size());
    assertEquals(
        Schema.Type.BYTES, schema.getField("nullableBinary").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableBinary").schema().getTypes().get(1).getType());
    assertEquals(Schema.Type.BYTES, schema.getField("nonNullableBinary").schema().getType());
  }

  @Test
  public void testConvertFixedSizeBinaryTypes() {
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

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(2, schema.getFields().size());

    assertEquals(Schema.Type.UNION, schema.getField("nullableFixedSizeBinary").schema().getType());
    assertEquals(2, schema.getField("nullableFixedSizeBinary").schema().getTypes().size());
    assertEquals(
        Schema.Type.FIXED,
        schema.getField("nullableFixedSizeBinary").schema().getTypes().get(0).getType());
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableFixedSizeBinary").schema().getTypes().get(1).getType());
    assertEquals(
        Schema.Type.FIXED, schema.getField("nonNullableFixedSizeBinary").schema().getType());
  }

  // Schema conversion for logical types, nullable and non-nullable

  @Test
  public void testConvertDecimalTypes() {
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
                "nullableDecimal256", FieldType.nullable(new ArrowType.Decimal(20, 4, 256)), null),
            new Field(
                "nonNullableDecimal2561",
                FieldType.notNullable(new ArrowType.Decimal(20, 4, 256)),
                null),
            new Field(
                "nonNullableDecimal2562",
                FieldType.notNullable(new ArrowType.Decimal(25, 8, 256)),
                null),
            new Field(
                "nonNullableDecimal2563",
                FieldType.notNullable(new ArrowType.Decimal(30, 15, 256)),
                null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(8, schema.getFields().size());

    // Assertions for nullableDecimal128
    assertEquals(Schema.Type.UNION, schema.getField("nullableDecimal128").schema().getType());
    assertEquals(2, schema.getField("nullableDecimal128").schema().getTypes().size());
    Schema nullableDecimal128Schema =
        schema.getField("nullableDecimal128").schema().getTypes().get(0);
    assertEquals(Schema.Type.FIXED, nullableDecimal128Schema.getType());
    assertEquals("decimal", nullableDecimal128Schema.getProp("logicalType"));
    assertEquals(10, nullableDecimal128Schema.getObjectProp("precision"));
    assertEquals(2, nullableDecimal128Schema.getObjectProp("scale"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableDecimal128").schema().getTypes().get(1).getType());

    // Assertions for nonNullableDecimal1281
    Schema nonNullableDecimal1281Schema = schema.getField("nonNullableDecimal1281").schema();
    assertEquals(Schema.Type.FIXED, nonNullableDecimal1281Schema.getType());
    assertEquals("decimal", nonNullableDecimal1281Schema.getProp("logicalType"));
    assertEquals(10, nonNullableDecimal1281Schema.getObjectProp("precision"));
    assertEquals(2, nonNullableDecimal1281Schema.getObjectProp("scale"));

    // Assertions for nonNullableDecimal1282
    Schema nonNullableDecimal1282Schema = schema.getField("nonNullableDecimal1282").schema();
    assertEquals(Schema.Type.FIXED, nonNullableDecimal1282Schema.getType());
    assertEquals("decimal", nonNullableDecimal1282Schema.getProp("logicalType"));
    assertEquals(15, nonNullableDecimal1282Schema.getObjectProp("precision"));
    assertEquals(5, nonNullableDecimal1282Schema.getObjectProp("scale"));

    // Assertions for nonNullableDecimal1283
    Schema nonNullableDecimal1283Schema = schema.getField("nonNullableDecimal1283").schema();
    assertEquals(Schema.Type.FIXED, nonNullableDecimal1283Schema.getType());
    assertEquals("decimal", nonNullableDecimal1283Schema.getProp("logicalType"));
    assertEquals(20, nonNullableDecimal1283Schema.getObjectProp("precision"));
    assertEquals(10, nonNullableDecimal1283Schema.getObjectProp("scale"));

    // Assertions for nullableDecimal256
    assertEquals(Schema.Type.UNION, schema.getField("nullableDecimal256").schema().getType());
    assertEquals(2, schema.getField("nullableDecimal256").schema().getTypes().size());
    Schema nullableDecimal256Schema =
        schema.getField("nullableDecimal256").schema().getTypes().get(0);
    assertEquals(Schema.Type.FIXED, nullableDecimal256Schema.getType());
    assertEquals("decimal", nullableDecimal256Schema.getProp("logicalType"));
    assertEquals(20, nullableDecimal256Schema.getObjectProp("precision"));
    assertEquals(4, nullableDecimal256Schema.getObjectProp("scale"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableDecimal256").schema().getTypes().get(1).getType());

    // Assertions for nonNullableDecimal2561
    Schema nonNullableDecimal2561Schema = schema.getField("nonNullableDecimal2561").schema();
    assertEquals(Schema.Type.FIXED, nonNullableDecimal2561Schema.getType());
    assertEquals("decimal", nonNullableDecimal2561Schema.getProp("logicalType"));
    assertEquals(20, nonNullableDecimal2561Schema.getObjectProp("precision"));
    assertEquals(4, nonNullableDecimal2561Schema.getObjectProp("scale"));

    // Assertions for nonNullableDecimal2562
    Schema nonNullableDecimal2562Schema = schema.getField("nonNullableDecimal2562").schema();
    assertEquals(Schema.Type.FIXED, nonNullableDecimal2562Schema.getType());
    assertEquals("decimal", nonNullableDecimal2562Schema.getProp("logicalType"));
    assertEquals(25, nonNullableDecimal2562Schema.getObjectProp("precision"));
    assertEquals(8, nonNullableDecimal2562Schema.getObjectProp("scale"));

    // Assertions for nonNullableDecimal2563
    Schema nonNullableDecimal2563Schema = schema.getField("nonNullableDecimal2563").schema();
    assertEquals(Schema.Type.FIXED, nonNullableDecimal2563Schema.getType());
    assertEquals("decimal", nonNullableDecimal2563Schema.getProp("logicalType"));
    assertEquals(30, nonNullableDecimal2563Schema.getObjectProp("precision"));
    assertEquals(15, nonNullableDecimal2563Schema.getObjectProp("scale"));
  }

  @Test
  public void testConvertDateTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableDateDay", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
            new Field(
                "nonNullableDateDay",
                FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)),
                null),
            new Field(
                "nullableDateMilli",
                FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)),
                null),
            new Field(
                "nonNullableDateMilli",
                FieldType.notNullable(new ArrowType.Date(DateUnit.MILLISECOND)),
                null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(4, schema.getFields().size());

    // Assertions for nullableDateDay
    assertEquals(Schema.Type.UNION, schema.getField("nullableDateDay").schema().getType());
    assertEquals(2, schema.getField("nullableDateDay").schema().getTypes().size());
    Schema nullableDateDaySchema = schema.getField("nullableDateDay").schema().getTypes().get(0);
    assertEquals(Schema.Type.INT, nullableDateDaySchema.getType());
    assertEquals("date", nullableDateDaySchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableDateDay").schema().getTypes().get(1).getType());

    // Assertions for nonNullableDateDay
    Schema nonNullableDateDaySchema = schema.getField("nonNullableDateDay").schema();
    assertEquals(Schema.Type.INT, nonNullableDateDaySchema.getType());
    assertEquals("date", nonNullableDateDaySchema.getProp("logicalType"));

    // Assertions for nullableDateMilli
    assertEquals(Schema.Type.UNION, schema.getField("nullableDateMilli").schema().getType());
    assertEquals(2, schema.getField("nullableDateMilli").schema().getTypes().size());
    Schema nullableDateMilliSchema =
        schema.getField("nullableDateMilli").schema().getTypes().get(0);
    assertEquals(Schema.Type.INT, nullableDateMilliSchema.getType());
    assertEquals("date", nullableDateMilliSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableDateMilli").schema().getTypes().get(1).getType());

    // Assertions for nonNullableDateMilli
    Schema nonNullableDateMilliSchema = schema.getField("nonNullableDateMilli").schema();
    assertEquals(Schema.Type.INT, nonNullableDateMilliSchema.getType());
    assertEquals("date", nonNullableDateMilliSchema.getProp("logicalType"));
  }

  @Test
  public void testConvertTimeTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableTimeSec",
                FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)),
                null),
            new Field(
                "nonNullableTimeSec",
                FieldType.notNullable(new ArrowType.Time(TimeUnit.SECOND, 32)),
                null),
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
                null),
            new Field(
                "nullableTimeNanos",
                FieldType.nullable(new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
                null),
            new Field(
                "nonNullableTimeNanos",
                FieldType.notNullable(new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
                null));

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(8, schema.getFields().size());

    // Assertions for nullableTimeSec
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimeSec").schema().getType());
    assertEquals(2, schema.getField("nullableTimeSec").schema().getTypes().size());
    Schema nullableTimeSecSchema = schema.getField("nullableTimeSec").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimeSecSchema.getType());
    assertEquals("time-micros", nullableTimeSecSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL, schema.getField("nullableTimeSec").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimeSec
    Schema nonNullableTimeSecSchema = schema.getField("nonNullableTimeSec").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimeSecSchema.getType());
    assertEquals("time-micros", nonNullableTimeSecSchema.getProp("logicalType"));

    // Assertions for nullableTimeMillis
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimeMillis").schema().getType());
    assertEquals(2, schema.getField("nullableTimeMillis").schema().getTypes().size());
    Schema nullableTimeMillisSchema =
        schema.getField("nullableTimeMillis").schema().getTypes().get(0);
    assertEquals(Schema.Type.INT, nullableTimeMillisSchema.getType());
    assertEquals("time-millis", nullableTimeMillisSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimeMillis").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimeMillis
    Schema nonNullableTimeMillisSchema = schema.getField("nonNullableTimeMillis").schema();
    assertEquals(Schema.Type.INT, nonNullableTimeMillisSchema.getType());
    assertEquals("time-millis", nonNullableTimeMillisSchema.getProp("logicalType"));

    // Assertions for nullableTimeMicros
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimeMicros").schema().getType());
    assertEquals(2, schema.getField("nullableTimeMicros").schema().getTypes().size());
    Schema nullableTimeMicrosSchema =
        schema.getField("nullableTimeMicros").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimeMicrosSchema.getType());
    assertEquals("time-micros", nullableTimeMicrosSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimeMicros").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimeMicros
    Schema nonNullableTimeMicrosSchema = schema.getField("nonNullableTimeMicros").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimeMicrosSchema.getType());
    assertEquals("time-micros", nonNullableTimeMicrosSchema.getProp("logicalType"));

    // Assertions for nullableTimeNanos
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimeNanos").schema().getType());
    assertEquals(2, schema.getField("nullableTimeNanos").schema().getTypes().size());
    Schema nullableTimeNanosSchema =
        schema.getField("nullableTimeNanos").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimeNanosSchema.getType());
    assertEquals("time-micros", nullableTimeNanosSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimeNanos").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimeNanos
    Schema nonNullableTimeNanosSchema = schema.getField("nonNullableTimeNanos").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimeNanosSchema.getType());
    assertEquals("time-micros", nonNullableTimeNanosSchema.getProp("logicalType"));
  }

  @Test
  public void testConvertZoneAwareTimestampTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableTimestampSecTz",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")),
                null),
            new Field(
                "nonNullableTimestampSecTz",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC")),
                null),
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

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(8, schema.getFields().size());

    // Assertions for nullableTimestampSecTz
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimestampSecTz").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampSecTz").schema().getTypes().size());
    Schema nullableTimestampSecTzSchema =
        schema.getField("nullableTimestampSecTz").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampSecTzSchema.getType());
    assertEquals("timestamp-millis", nullableTimestampSecTzSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampSecTz").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampSecTz
    Schema nonNullableTimestampSecTzSchema = schema.getField("nonNullableTimestampSecTz").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampSecTzSchema.getType());
    assertEquals("timestamp-millis", nonNullableTimestampSecTzSchema.getProp("logicalType"));

    // Assertions for nullableTimestampMillisTz
    assertEquals(
        Schema.Type.UNION, schema.getField("nullableTimestampMillisTz").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampMillisTz").schema().getTypes().size());
    Schema nullableTimestampMillisTzSchema =
        schema.getField("nullableTimestampMillisTz").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampMillisTzSchema.getType());
    assertEquals("timestamp-millis", nullableTimestampMillisTzSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampMillisTz").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampMillisTz
    Schema nonNullableTimestampMillisTzSchema =
        schema.getField("nonNullableTimestampMillisTz").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampMillisTzSchema.getType());
    assertEquals("timestamp-millis", nonNullableTimestampMillisTzSchema.getProp("logicalType"));

    // Assertions for nullableTimestampMicrosTz
    assertEquals(
        Schema.Type.UNION, schema.getField("nullableTimestampMicrosTz").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampMicrosTz").schema().getTypes().size());
    Schema nullableTimestampMicrosTzSchema =
        schema.getField("nullableTimestampMicrosTz").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampMicrosTzSchema.getType());
    assertEquals("timestamp-micros", nullableTimestampMicrosTzSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampMicrosTz").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampMicrosTz
    Schema nonNullableTimestampMicrosTzSchema =
        schema.getField("nonNullableTimestampMicrosTz").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampMicrosTzSchema.getType());
    assertEquals("timestamp-micros", nonNullableTimestampMicrosTzSchema.getProp("logicalType"));

    // Assertions for nullableTimestampNanosTz
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimestampNanosTz").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampNanosTz").schema().getTypes().size());
    Schema nullableTimestampNanosTzSchema =
        schema.getField("nullableTimestampNanosTz").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampNanosTzSchema.getType());
    assertEquals("timestamp-nanos", nullableTimestampNanosTzSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampNanosTz").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampNanosTz
    Schema nonNullableTimestampNanosTzSchema =
        schema.getField("nonNullableTimestampNanosTz").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampNanosTzSchema.getType());
    assertEquals("timestamp-nanos", nonNullableTimestampNanosTzSchema.getProp("logicalType"));
  }

  @Test
  public void testConvertLocalTimestampTypes() {
    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableTimestampSec",
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)),
                null),
            new Field(
                "nonNullableTimestampSec",
                FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.SECOND, null)),
                null),
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

    Schema schema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals(8, schema.getFields().size());

    // Assertions for nullableTimestampSec
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimestampSec").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampSec").schema().getTypes().size());
    Schema nullableTimestampSecSchema =
        schema.getField("nullableTimestampSec").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampSecSchema.getType());
    assertEquals("local-timestamp-millis", nullableTimestampSecSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampSec").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampSec
    Schema nonNullableTimestampSecSchema = schema.getField("nonNullableTimestampSec").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampSecSchema.getType());
    assertEquals("local-timestamp-millis", nonNullableTimestampSecSchema.getProp("logicalType"));

    // Assertions for nullableTimestampMillis
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimestampMillis").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampMillis").schema().getTypes().size());
    Schema nullableTimestampMillisSchema =
        schema.getField("nullableTimestampMillis").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampMillisSchema.getType());
    assertEquals("local-timestamp-millis", nullableTimestampMillisSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampMillis").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampMillis
    Schema nonNullableTimestampMillisSchema =
        schema.getField("nonNullableTimestampMillis").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampMillisSchema.getType());
    assertEquals("local-timestamp-millis", nonNullableTimestampMillisSchema.getProp("logicalType"));

    // Assertions for nullableTimestampMicros
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimestampMicros").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampMicros").schema().getTypes().size());
    Schema nullableTimestampMicrosSchema =
        schema.getField("nullableTimestampMicros").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampMicrosSchema.getType());
    assertEquals("local-timestamp-micros", nullableTimestampMicrosSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampMicros").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampMicros
    Schema nonNullableTimestampMicrosSchema =
        schema.getField("nonNullableTimestampMicros").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampMicrosSchema.getType());
    assertEquals("local-timestamp-micros", nonNullableTimestampMicrosSchema.getProp("logicalType"));

    // Assertions for nullableTimestampNanos
    assertEquals(Schema.Type.UNION, schema.getField("nullableTimestampNanos").schema().getType());
    assertEquals(2, schema.getField("nullableTimestampNanos").schema().getTypes().size());
    Schema nullableTimestampNanosSchema =
        schema.getField("nullableTimestampNanos").schema().getTypes().get(0);
    assertEquals(Schema.Type.LONG, nullableTimestampNanosSchema.getType());
    assertEquals("local-timestamp-nanos", nullableTimestampNanosSchema.getProp("logicalType"));
    assertEquals(
        Schema.Type.NULL,
        schema.getField("nullableTimestampNanos").schema().getTypes().get(1).getType());

    // Assertions for nonNullableTimestampNanos
    Schema nonNullableTimestampNanosSchema = schema.getField("nonNullableTimestampNanos").schema();
    assertEquals(Schema.Type.LONG, nonNullableTimestampNanosSchema.getType());
    assertEquals("local-timestamp-nanos", nonNullableTimestampNanosSchema.getProp("logicalType"));
  }
}
