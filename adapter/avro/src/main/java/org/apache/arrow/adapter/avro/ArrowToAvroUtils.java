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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adapter.avro.producers.AvroArraysProducer;
import org.apache.arrow.adapter.avro.producers.AvroBooleanProducer;
import org.apache.arrow.adapter.avro.producers.AvroBytesProducer;
import org.apache.arrow.adapter.avro.producers.AvroDoubleProducer;
import org.apache.arrow.adapter.avro.producers.AvroFixedProducer;
import org.apache.arrow.adapter.avro.producers.AvroFloatProducer;
import org.apache.arrow.adapter.avro.producers.AvroIntProducer;
import org.apache.arrow.adapter.avro.producers.AvroLongProducer;
import org.apache.arrow.adapter.avro.producers.AvroMapProducer;
import org.apache.arrow.adapter.avro.producers.AvroNullProducer;
import org.apache.arrow.adapter.avro.producers.AvroNullableProducer;
import org.apache.arrow.adapter.avro.producers.AvroStringProducer;
import org.apache.arrow.adapter.avro.producers.AvroStructProducer;
import org.apache.arrow.adapter.avro.producers.AvroUnionsProducer;
import org.apache.arrow.adapter.avro.producers.BaseAvroProducer;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.adapter.avro.producers.Producer;
import org.apache.arrow.adapter.avro.producers.logical.AvroDateProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroDecimalProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimeMicroProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimeMillisProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampMicroProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampMillisProducer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class ArrowToAvroUtils {

  public static final String GENERIC_RECORD_TYPE_NAME = "GenericRecord";

  /**
   * Create an Avro record schema for a given list of Arrow fields.
   *
   * <p>This method currently performs following type mapping for Avro data types to corresponding
   * Arrow data types.
   *
   * <ul>
   *   <li>ArrowType.Null --> NULL
   *   <li>ArrowType.Bool --> BOOLEAN
   *   <li>ArrowType.Int(64 bit, unsigned 32 bit) --> LONG
   *   <li>ArrowType.Int(signed 32 bit, < 32 bit) --> INT
   *   <li>ArrowType.FloatingPoint(double) --> DOUBLE
   *   <li>ArrowType.FloatingPoint(single, half) --> FLOAT
   *   <li>ArrowType.Utf8 --> STRING
   *   <li>ArrowType.LargeUtf8 --> STRING
   *   <li>ArrowType.Binary --> BYTES
   *   <li>ArrowType.LargeBinary --> BYTES
   *   <li>ArrowType.FixedSizeBinary --> FIXED
   *   <li>ArrowType.Decimal --> decimal (FIXED)
   *   <li>ArrowType.Date --> date (INT)
   *   <li>ArrowType.Time (bit width <= 32) --> time-millis (INT)
   *   <li>ArrowType.Time (bit width > 32) --> time-micros (LONG)
   *   <li>ArrowType.Timestamp (NANOSECONDS, TZ != NULL) --> time-nanos (LONG)
   *   <li>ArrowType.Timestamp (MICROSECONDS, TZ != NULL) --> time-micros (LONG)
   *   <li>ArrowType.Timestamp (MILLISECONDS | SECONDS, TZ != NULL) --> time-millis (LONG)
   *   <li>ArrowType.Timestamp (NANOSECONDS, TZ == NULL) --> local-time-nanos (LONG)
   *   <li>ArrowType.Timestamp (MICROSECONDS, TZ == NULL) --> local-time-micros (LONG)
   *   <li>ArrowType.Timestamp (MILLISECONDS | SECONDS, TZ == NULL) --> local-time-millis (LONG)
   *   <li>ArrowType.Duration --> duration (FIXED)
   *   <li>ArrowType.Interval --> duration (FIXED)
   *   <li>ArrowType.Struct --> record
   *   <li>ArrowType.List --> array
   *   <li>ArrowType.LargeList --> array
   *   <li>ArrowType.FixedSizeList --> array
   *   <li>ArrowType.Map --> map
   *   <li>ArrowType.Union --> union
   * </ul>
   *
   * <p>Nullable fields are represented as a union of [null | base-type]. Special treatment is given
   * to nullability of unions - a union is considered nullable if the union field is nullable or any
   * of its child fields are nullable. The schema for a nullable union will always contain a null
   * type,none of the direct child types will be nullable.
   *
   * <p>List fields must contain precisely one child field, which may be nullable. Map fields must
   * contain precisely two child fields, the key field and the value field. The key field must
   * always be of type STRING (Utf8) and cannot be nullable. The value can be of any type and may be
   * nullable. Record types must contain at least one child field and cannot contain multiple fields
   * with the same name
   *
   * @param arrowFields The arrow fields used to generate the Avro schema
   * @param typeName Name of the top level Avro record type
   * @param namespace Namespace of the top level Avro record type
   * @return An Avro record schema for the given list of fields, with the specified name and
   *     namespace
   */
  public static Schema createAvroSchema(
      List<Field> arrowFields, String typeName, String namespace) {
    SchemaBuilder.RecordBuilder<Schema> assembler =
        SchemaBuilder.record(typeName).namespace(namespace);
    return buildRecordSchema(assembler, arrowFields, namespace);
  }

  /** Overload provided for convenience, sets namespace = null. */
  public static Schema createAvroSchema(List<Field> arrowFields, String typeName) {
    return createAvroSchema(arrowFields, typeName, null);
  }

  /** Overload provided for convenience, sets name = GENERIC_RECORD_TYPE_NAME. */
  public static Schema createAvroSchema(List<Field> arrowFields) {
    return createAvroSchema(arrowFields, GENERIC_RECORD_TYPE_NAME);
  }

  private static <T> T buildRecordSchema(
      SchemaBuilder.RecordBuilder<T> builder, List<Field> fields, String namespace) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("Record field must have at least one child field");
    }
    SchemaBuilder.FieldAssembler<T> assembler = builder.fields();
    for (Field field : fields) {
      assembler = buildFieldSchema(assembler, field, namespace);
    }
    return assembler.endRecord();
  }

  private static <T> SchemaBuilder.FieldAssembler<T> buildFieldSchema(
      SchemaBuilder.FieldAssembler<T> assembler, Field field, String namespace) {

    SchemaBuilder.FieldTypeBuilder<T> builder = assembler.name(field.getName()).type();

    // Nullable unions need special handling, since union types cannot be directly nested
    if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Union) {
      boolean unionNullable = field.getChildren().stream().anyMatch(Field::isNullable);
      if (field.isNullable() && !unionNullable) {
        SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<T>> union =
            builder.unionOf().nullType();
        return addTypesToUnion(union, field.getChildren(), namespace).nullDefault();
      } else {
        Field headType = field.getChildren().get(0);
        List<Field> tailTypes = field.getChildren().subList(1, field.getChildren().size());
        SchemaBuilder.UnionAccumulator<SchemaBuilder.FieldDefault<T, ?>> union =
            buildUnionFieldSchema(builder.unionOf(), headType, namespace);
        return addTypesToUnion(union, tailTypes, namespace).noDefault();
      }
    } else if (field.isNullable()) {
      return buildBaseFieldSchema(builder.nullable(), field, namespace);
    } else {
      return buildBaseFieldSchema(builder, field, namespace);
    }
  }

  private static <T> T buildArraySchema(
      SchemaBuilder.ArrayBuilder<T> builder, Field listField, String namespace) {
    if (listField.getChildren().size() != 1) {
      throw new IllegalArgumentException("List field must have exactly one child field");
    }
    Field itemField = listField.getChildren().get(0);
    return buildTypeSchema(builder.items(), itemField, namespace);
  }

  private static <T> T buildMapSchema(
      SchemaBuilder.MapBuilder<T> builder, Field mapField, String namespace) {
    if (mapField.getChildren().size() != 2) {
      throw new IllegalArgumentException("Map field must have exactly two child fields");
    }
    Field keyField = mapField.getChildren().get(0);
    Field valueField = mapField.getChildren().get(1);
    if (keyField.getType().getTypeID() != ArrowType.ArrowTypeID.Utf8 || keyField.isNullable()) {
      throw new IllegalArgumentException(
          "Map keys must be of type string and cannot be nullable for conversion to Avro");
    }
    return buildTypeSchema(builder.values(), valueField, namespace);
  }

  private static <T> T buildTypeSchema(
      SchemaBuilder.TypeBuilder<T> builder, Field field, String namespace) {

    // Nullable unions need special handling, since union types cannot be directly nested
    if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Union) {
      boolean unionNullable = field.getChildren().stream().anyMatch(Field::isNullable);
      if (field.isNullable() && !unionNullable) {
        SchemaBuilder.UnionAccumulator<T> union = builder.unionOf().nullType();
        return addTypesToUnion(union, field.getChildren(), namespace);
      } else {
        Field headType = field.getChildren().get(0);
        List<Field> tailTypes = field.getChildren().subList(1, field.getChildren().size());
        SchemaBuilder.UnionAccumulator<T> union =
            buildBaseTypeSchema(builder.unionOf(), headType, namespace);
        return addTypesToUnion(union, tailTypes, namespace);
      }
    } else if (field.isNullable()) {
      return buildBaseTypeSchema(builder.nullable(), field, namespace);
    } else {
      return buildBaseTypeSchema(builder, field, namespace);
    }
  }

  private static <T> T buildBaseTypeSchema(
      SchemaBuilder.BaseTypeBuilder<T> builder, Field field, String namespace) {

    ArrowType.ArrowTypeID typeID = field.getType().getTypeID();

    switch (typeID) {
      case Null:
        return builder.nullType();

      case Bool:
        return builder.booleanType();

      case Int:
        ArrowType.Int intType = (ArrowType.Int) field.getType();
        if (intType.getBitWidth() > 32 || (intType.getBitWidth() == 32 && !intType.getIsSigned())) {
          return builder.longType();
        } else {
          return builder.intType();
        }

      case FloatingPoint:
        ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) field.getType();
        if (floatType.getPrecision() == FloatingPointPrecision.DOUBLE) {
          return builder.doubleType();
        } else {
          return builder.floatType();
        }

      case Utf8:
      case LargeUtf8:
        return builder.stringType();

      case Binary:
      case LargeBinary:
        return builder.bytesType();

      case FixedSizeBinary:
        ArrowType.FixedSizeBinary fixedType = (ArrowType.FixedSizeBinary) field.getType();
        String fixedTypeName = field.getName();
        int fixedTypeWidth = fixedType.getByteWidth();
        return builder.fixed(fixedTypeName).size(fixedTypeWidth);

      case Decimal:
        ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
        return builder
            .fixed(field.getName())
            .prop("logicalType", "decimal")
            .prop("precision", decimalType.getPrecision())
            .prop("scale", decimalType.getScale())
            .size(decimalType.getBitWidth() / 8);

      case Date:
        return builder.intBuilder().prop("logicalType", "date").endInt();

      case Time:
        ArrowType.Time timeType = (ArrowType.Time) field.getType();
        if (timeType.getBitWidth() <= 32) {
          return builder.intBuilder().prop("logicalType", "time-millis").endInt();
        } else {
          return builder.longBuilder().prop("logicalType", "time-micros").endLong();
        }

      case Timestamp:
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) field.getType();
        String timestampLogicalType = timestampLogicalType(timestampType);
        return builder.longBuilder().prop("logicalType", timestampLogicalType).endLong();

      case Duration:
      case Interval:
        return builder.fixed(field.getName()).prop("logicalType", "duration").size(12);

      case Struct:
        String childNamespace =
            namespace == null ? field.getName() : namespace + "." + field.getName();
        return buildRecordSchema(
            builder.record(field.getName()), field.getChildren(), childNamespace);

      case List:
      case LargeList:
      case FixedSizeList:
        return buildArraySchema(builder.array(), field, namespace);

      case Map:
        return buildMapSchema(builder.map(), field, namespace);

      default:
        throw new IllegalArgumentException(
            "Element type not supported for Avro conversion: " + typeID.name());
    }
  }

  private static <T> SchemaBuilder.FieldAssembler<T> buildBaseFieldSchema(
      SchemaBuilder.BaseFieldTypeBuilder<T> builder, Field field, String namespace) {

    ArrowType.ArrowTypeID typeID = field.getType().getTypeID();

    switch (typeID) {
      case Null:
        return builder.nullType().noDefault();

      case Bool:
        return builder.booleanType().noDefault();

      case Int:
        ArrowType.Int intType = (ArrowType.Int) field.getType();
        if (intType.getBitWidth() > 32 || (intType.getBitWidth() == 32 && !intType.getIsSigned())) {
          return builder.longType().noDefault();
        } else {
          return builder.intType().noDefault();
        }

      case FloatingPoint:
        ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) field.getType();
        if (floatType.getPrecision() == FloatingPointPrecision.DOUBLE) {
          return builder.doubleType().noDefault();
        } else {
          return builder.floatType().noDefault();
        }

      case Utf8:
      case LargeUtf8:
        return builder.stringType().noDefault();

      case Binary:
      case LargeBinary:
        return builder.bytesType().noDefault();

      case FixedSizeBinary:
        ArrowType.FixedSizeBinary fixedType = (ArrowType.FixedSizeBinary) field.getType();
        return builder.fixed(field.getName()).size(fixedType.getByteWidth()).noDefault();

      case Decimal:
        ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
        return builder
            .fixed(field.getName())
            .prop("logicalType", "decimal")
            .prop("precision", decimalType.getPrecision())
            .prop("scale", decimalType.getScale())
            .size(decimalType.getBitWidth() / 8)
            .noDefault();

      case Date:
        return builder.intBuilder().prop("logicalType", "date").endInt().noDefault();

      case Time:
        ArrowType.Time timeType = (ArrowType.Time) field.getType();
        if (timeType.getBitWidth() <= 32) {
          return builder.intBuilder().prop("logicalType", "time-millis").endInt().noDefault();
        } else {
          return builder.longBuilder().prop("logicalType", "time-micros").endLong().noDefault();
        }

      case Timestamp:
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) field.getType();
        String timestampLogicalType = timestampLogicalType(timestampType);
        return builder
            .longBuilder()
            .prop("logicalType", timestampLogicalType)
            .endLong()
            .noDefault();

      case Duration:
      case Interval:
        return builder.fixed(field.getName()).prop("logicalType", "duration").size(12).noDefault();

      case Struct:
        String childNamespace =
            namespace == null ? field.getName() : namespace + "." + field.getName();
        return buildRecordSchema(
                builder.record(field.getName()), field.getChildren(), childNamespace)
            .noDefault();

      case List:
      case LargeList:
      case FixedSizeList:
        return buildArraySchema(builder.array(), field, namespace).noDefault();

      case Map:
        return buildMapSchema(builder.map(), field, namespace).noDefault();

      default:
        throw new IllegalArgumentException(
            "Field type not supported for Avro conversion: " + typeID.name());
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <T>
      SchemaBuilder.UnionAccumulator<SchemaBuilder.FieldDefault<T, ?>> buildUnionFieldSchema(
          SchemaBuilder.UnionFieldTypeBuilder<T> builder, Field field, String namespace) {

    ArrowType.ArrowTypeID typeID = field.getType().getTypeID();

    switch (typeID) {
      case Null:
        return (SchemaBuilder.UnionAccumulator) builder.nullType();

      case Bool:
        return (SchemaBuilder.UnionAccumulator) builder.booleanType();

      case Int:
        ArrowType.Int intType = (ArrowType.Int) field.getType();
        if (intType.getBitWidth() > 32 || (intType.getBitWidth() == 32 && !intType.getIsSigned())) {
          return (SchemaBuilder.UnionAccumulator) builder.longType();
        } else {
          return (SchemaBuilder.UnionAccumulator) builder.intType();
        }

      case FloatingPoint:
        ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) field.getType();
        if (floatType.getPrecision() == FloatingPointPrecision.DOUBLE) {
          return (SchemaBuilder.UnionAccumulator) builder.doubleType();
        } else {
          return (SchemaBuilder.UnionAccumulator) builder.floatType();
        }

      case Utf8:
      case LargeUtf8:
        return (SchemaBuilder.UnionAccumulator) builder.stringType();

      case Binary:
      case LargeBinary:
        return (SchemaBuilder.UnionAccumulator) builder.bytesType();

      case FixedSizeBinary:
        ArrowType.FixedSizeBinary fixedType = (ArrowType.FixedSizeBinary) field.getType();
        String fixedTypeName = field.getName();
        int fixedTypeWidth = fixedType.getByteWidth();
        return (SchemaBuilder.UnionAccumulator) builder.fixed(fixedTypeName).size(fixedTypeWidth);

      case Decimal:
        ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
        return (SchemaBuilder.UnionAccumulator)
            builder
                .fixed(field.getName())
                .prop("logicalType", "decimal")
                .prop("precision", decimalType.getPrecision())
                .prop("scale", decimalType.getScale())
                .size(decimalType.getBitWidth() / 8);

      case Date:
        return (SchemaBuilder.UnionAccumulator)
            builder.intBuilder().prop("logicalType", "date").endInt();

      case Time:
        ArrowType.Time timeType = (ArrowType.Time) field.getType();
        if (timeType.getBitWidth() <= 32) {
          return (SchemaBuilder.UnionAccumulator)
              builder.intBuilder().prop("logicalType", "time-millis").endInt();
        } else {
          return (SchemaBuilder.UnionAccumulator)
              builder.longBuilder().prop("logicalType", "time-micros").endLong();
        }

      case Timestamp:
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) field.getType();
        String timestampLogicalType = timestampLogicalType(timestampType);
        return (SchemaBuilder.UnionAccumulator)
            builder.longBuilder().prop("logicalType", timestampLogicalType).endLong();

      case Duration:
      case Interval:
        return (SchemaBuilder.UnionAccumulator)
            builder.fixed(field.getName()).prop("logicalType", "duration").size(12);

      case Struct:
        String childNamespace =
            namespace == null ? field.getName() : namespace + "." + field.getName();
        return (SchemaBuilder.UnionAccumulator)
            buildRecordSchema(builder.record(field.getName()), field.getChildren(), childNamespace);

      case List:
      case LargeList:
      case FixedSizeList:
        return (SchemaBuilder.UnionAccumulator) buildArraySchema(builder.array(), field, namespace);

      case Map:
        return (SchemaBuilder.UnionAccumulator) buildMapSchema(builder.map(), field, namespace);

      default:
        throw new IllegalArgumentException(
            "Union member type not supported for Avro conversion: " + typeID.name());
    }
  }

  private static <T> T addTypesToUnion(
      SchemaBuilder.UnionAccumulator<T> accumulator, List<Field> unionFields, String namespace) {
    for (var field : unionFields) {
      accumulator = buildBaseTypeSchema(accumulator.and(), field, namespace);
    }
    return accumulator.endUnion();
  }

  private static String timestampLogicalType(ArrowType.Timestamp timestampType) {
    boolean zoneAware = timestampType.getTimezone() != null;
    if (timestampType.getUnit() == TimeUnit.NANOSECOND) {
      return zoneAware ? "timestamp-nanos" : "local-timestamp-nanos";
    } else if (timestampType.getUnit() == TimeUnit.MICROSECOND) {
      return zoneAware ? "timestamp-micros" : "local-timestamp-micros";
    } else {
      // Timestamp in seconds will be cast to milliseconds, Avro does not support seconds
      return zoneAware ? "timestamp-millis" : "local-timestamp-millis";
    }
  }

  /**
   * Create a composite Avro producer for a set of field vectors (typically the root set of a VSR).
   *
   * @param vectors The vectors that will be used to produce Avro data
   * @return The resulting composite Avro producer
   */
  public static CompositeAvroProducer createCompositeProducer(List<FieldVector> vectors) {

    List<Producer<? extends FieldVector>> producers = new ArrayList<>(vectors.size());

    for (FieldVector vector : vectors) {
      BaseAvroProducer<? extends FieldVector> producer = createProducer(vector);
      producers.add(producer);
    }

    return new CompositeAvroProducer(producers);
  }

  private static BaseAvroProducer<?> createProducer(FieldVector vector) {
    boolean nullable = vector.getField().isNullable();
    return createProducer(vector, nullable);
  }

  private static BaseAvroProducer<?> createProducer(FieldVector vector, boolean nullable) {

    Preconditions.checkNotNull(vector, "Arrow vector object can't be null");

    final Types.MinorType minorType = vector.getMinorType();

    // Avro understands nullable types as a union of type | null
    // Most nullable fields in a VSR will not be unions, so provide a special wrapper
    if (nullable && minorType != Types.MinorType.UNION) {
      final BaseAvroProducer<?> innerProducer = createProducer(vector, false);
      return new AvroNullableProducer<>(innerProducer);
    }

    switch (minorType) {
      case NULL:
        return new AvroNullProducer((NullVector) vector);
      case BIT:
        return new AvroBooleanProducer((BitVector) vector);
      case INT:
        return new AvroIntProducer((IntVector) vector);
      case BIGINT:
        return new AvroLongProducer((BigIntVector) vector);
      case FLOAT4:
        return new AvroFloatProducer((Float4Vector) vector);
      case FLOAT8:
        return new AvroDoubleProducer((Float8Vector) vector);
      case VARBINARY:
        return new AvroBytesProducer((VarBinaryVector) vector);
      case FIXEDSIZEBINARY:
        return new AvroFixedProducer((FixedSizeBinaryVector) vector);
      case VARCHAR:
        return new AvroStringProducer((VarCharVector) vector);

        // Logical types

      case DECIMAL:
        return new AvroDecimalProducer((DecimalVector) vector);
      case DATEDAY:
        return new AvroDateProducer((DateDayVector) vector);
      case TIMEMILLI:
        return new AvroTimeMillisProducer((TimeMilliVector) vector);
      case TIMEMICRO:
        return new AvroTimeMicroProducer((TimeMicroVector) vector);
      case TIMESTAMPMILLI:
        return new AvroTimestampMillisProducer((TimeStampMilliVector) vector);
      case TIMESTAMPMICRO:
        return new AvroTimestampMicroProducer((TimeStampMicroVector) vector);

        // Complex types

      case STRUCT:
        StructVector structVector = (StructVector) vector;
        List<FieldVector> childVectors = structVector.getChildrenFromFields();
        Producer<?>[] childProducers = new Producer<?>[childVectors.size()];
        for (int i = 0; i < childVectors.size(); i++) {
          FieldVector childVector = childVectors.get(i);
          childProducers[i] = createProducer(childVector, childVector.getField().isNullable());
        }
        return new AvroStructProducer(structVector, childProducers);

      case LIST:
        ListVector listVector = (ListVector) vector;
        FieldVector itemVector = listVector.getDataVector();
        Producer<?> itemProducer = createProducer(itemVector, itemVector.getField().isNullable());
        return new AvroArraysProducer(listVector, itemProducer);

      case MAP:
        MapVector mapVector = (MapVector) vector;
        StructVector entryVector = (StructVector) mapVector.getDataVector();
        VarCharVector keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
        FieldVector valueVector = entryVector.getChildrenFromFields().get(1);
        Producer<?> keyProducer = new AvroStringProducer(keyVector);
        Producer<?> valueProducer =
            createProducer(valueVector, valueVector.getField().isNullable());
        Producer<?> entryProducer =
            new AvroStructProducer(entryVector, new Producer<?>[] {keyProducer, valueProducer});
        return new AvroMapProducer(mapVector, entryProducer);

      case UNION:
        UnionVector unionVector = (UnionVector) vector;
        List<FieldVector> unionChildVectors = unionVector.getChildrenFromFields();
        Producer<?>[] unionChildProducers = new Producer<?>[unionChildVectors.size()];
        for (int i = 0; i < unionChildVectors.size(); i++) {
          FieldVector unionChildVector = unionChildVectors.get(i);
          unionChildProducers[i] =
              createProducer(unionChildVector, /* nullable = */ false); // Do not nest union types
        }
        return new AvroUnionsProducer(unionVector, unionChildProducers);

      default:
        // Not all Arrow types are supported for encoding (yet)!
        String error =
            String.format(
                "Encoding Arrow type %s to Avro is not currently supported", minorType.name());
        throw new UnsupportedOperationException(error);
    }
  }
}
