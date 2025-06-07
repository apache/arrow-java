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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.arrow.adapter.avro.producers.AvroBigIntProducer;
import org.apache.arrow.adapter.avro.producers.AvroBooleanProducer;
import org.apache.arrow.adapter.avro.producers.AvroBytesProducer;
import org.apache.arrow.adapter.avro.producers.AvroEnumProducer;
import org.apache.arrow.adapter.avro.producers.AvroFixedSizeBinaryProducer;
import org.apache.arrow.adapter.avro.producers.AvroFixedSizeListProducer;
import org.apache.arrow.adapter.avro.producers.AvroFloat2Producer;
import org.apache.arrow.adapter.avro.producers.AvroFloat4Producer;
import org.apache.arrow.adapter.avro.producers.AvroFloat8Producer;
import org.apache.arrow.adapter.avro.producers.AvroIntProducer;
import org.apache.arrow.adapter.avro.producers.AvroListProducer;
import org.apache.arrow.adapter.avro.producers.AvroMapProducer;
import org.apache.arrow.adapter.avro.producers.AvroNullProducer;
import org.apache.arrow.adapter.avro.producers.AvroNullableProducer;
import org.apache.arrow.adapter.avro.producers.AvroSmallIntProducer;
import org.apache.arrow.adapter.avro.producers.AvroStringProducer;
import org.apache.arrow.adapter.avro.producers.AvroStructProducer;
import org.apache.arrow.adapter.avro.producers.AvroTinyIntProducer;
import org.apache.arrow.adapter.avro.producers.AvroUint1Producer;
import org.apache.arrow.adapter.avro.producers.AvroUint2Producer;
import org.apache.arrow.adapter.avro.producers.AvroUint4Producer;
import org.apache.arrow.adapter.avro.producers.AvroUint8Producer;
import org.apache.arrow.adapter.avro.producers.BaseAvroProducer;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.adapter.avro.producers.DictionaryDecodingProducer;
import org.apache.arrow.adapter.avro.producers.Producer;
import org.apache.arrow.adapter.avro.producers.logical.AvroDateDayProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroDateMilliProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroDecimal256Producer;
import org.apache.arrow.adapter.avro.producers.logical.AvroDecimalProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimeMicroProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimeMilliProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimeNanoProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimeSecProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampMicroProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampMicroTzProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampMilliProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampMilliTzProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampNanoProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampNanoTzProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampSecProducer;
import org.apache.arrow.adapter.avro.producers.logical.AvroTimestampSecTzProducer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
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
   * <table>
   *   <thead><tr><th>Arrow type</th><th>Avro encoding</th></tr></thead>
   *   <tbody>
   *     <tr><td>ArrowType.Null</td><td>NULL</td></tr>
   *     <tr><td>ArrowType.Bool</td><td>BOOLEAN</td></tr>
   *     <tr><td>ArrowType.Int(64 bit, unsigned 32 bit)</td><td>LONG</td></tr>
   *     <tr><td>ArrowType.Int(signed 32 bit, &lt; 32 bit)</td><td>INT</td></tr>
   *     <tr><td>ArrowType.FloatingPoint(double)</td><td>DOUBLE</td></tr>
   *     <tr><td>ArrowType.FloatingPoint(single, half)</td><td>FLOAT</td></tr>
   *     <tr><td>ArrowType.Utf8</td><td>STRING</td></tr>
   *     <tr><td>ArrowType.LargeUtf8</td><td>STRING</td></tr>
   *     <tr><td>ArrowType.Binary</td><td>BYTES</td></tr>
   *     <tr><td>ArrowType.LargeBinary</td><td>BYTES</td></tr>
   *     <tr><td>ArrowType.FixedSizeBinary</td><td>FIXED</td></tr>
   *     <tr><td>ArrowType.Decimal</td><td>decimal (FIXED)</td></tr>
   *     <tr><td>ArrowType.Date</td><td>date (INT)</td></tr>
   *     <tr><td>ArrowType.Time (SEC | MILLI)</td><td>time-millis (INT)</td></tr>
   *     <tr><td>ArrowType.Time (MICRO | NANO)</td><td>time-micros (LONG)</td></tr>
   *     <tr><td>ArrowType.Timestamp (NANOSECONDS, TZ != NULL)</td><td>time-nanos (LONG)</td></tr>
   *     <tr><td>ArrowType.Timestamp (MICROSECONDS, TZ != NULL)</td><td>time-micros (LONG)</td></tr>
   *     <tr><td>ArrowType.Timestamp (MILLISECONDS | SECONDS, TZ != NULL)</td><td>time-millis (LONG)</td></tr>
   *     <tr><td>ArrowType.Timestamp (NANOSECONDS, TZ == NULL)</td><td>local-time-nanos (LONG)</td></tr>
   *     <tr><td>ArrowType.Timestamp (MICROSECONDS, TZ == NULL)</td><td>local-time-micros (LONG)</td></tr>
   *     <tr><td>ArrowType.Timestamp (MILLISECONDS | SECONDS, TZ == NULL)</td><td>local-time-millis (LONG)</td></tr>
   *     <tr><td>ArrowType.Duration</td><td>duration (FIXED)</td></tr>
   *     <tr><td>ArrowType.Interval</td><td>duration (FIXED)</td></tr>
   *     <tr><td>ArrowType.Struct</td><td>record</td></tr>
   *     <tr><td>ArrowType.List</td><td>array</td></tr>
   *     <tr><td>ArrowType.LargeList</td><td>array</td></tr>
   *     <tr><td>ArrowType.FixedSizeList</td><td>array</td></tr>
   *     <tr><td>ArrowType.Map</td><td>map</td></tr>
   *     <tr><td>ArrowType.Union</td><td>union</td></tr>
   *   </tbody>
   * </table>
   *
   * <p>Nullable fields are represented as a union of [base-type | null]. Special treatment is given
   * to nullability of unions - a union is considered nullable if any of its child fields are
   * nullable. The schema for a nullable union will always contain a null type as its first member,
   * with none of the child types being nullable.
   *
   * <p>List fields must contain precisely one child field, which may be nullable. Map fields are
   * represented as a list of structs, where the struct fields are "key" and "value". The key field
   * must always be of type STRING (Utf8) and cannot be nullable. The value can be of any type and
   * may be nullable. Record types must contain at least one child field and cannot contain multiple
   * fields with the same name
   *
   * <p>String fields that are dictionary-encoded will be represented as an Avro enum, so long as
   * all the values meet the restrictions on Avro enums (non-null, valid identifiers). Other data
   * types that are dictionary encoded, or string fields that do not meet the avro requirements,
   * will be output as their decoded type.
   *
   * @param arrowFields The arrow fields used to generate the Avro schema
   * @param typeName Name of the top level Avro record type
   * @param namespace Namespace of the top level Avro record type
   * @param dictionaries A dictionary provider is required if any fields use dictionary encoding
   * @return An Avro record schema for the given list of fields, with the specified name and
   *     namespace
   */
  public static Schema createAvroSchema(
      List<Field> arrowFields, String typeName, String namespace, DictionaryProvider dictionaries) {
    SchemaBuilder.RecordBuilder<Schema> assembler =
        SchemaBuilder.record(typeName).namespace(namespace);
    return buildRecordSchema(assembler, arrowFields, namespace, dictionaries);
  }

  /** Overload provided for convenience, sets dictionaries = null. */
  public static Schema createAvroSchema(
      List<Field> arrowFields, String typeName, String namespace) {
    return createAvroSchema(arrowFields, typeName, namespace, null);
  }

  /** Overload provided for convenience, sets namespace = null. */
  public static Schema createAvroSchema(List<Field> arrowFields, String typeName) {
    return createAvroSchema(arrowFields, typeName, null);
  }

  /** Overload provided for convenience, sets name = GENERIC_RECORD_TYPE_NAME. */
  public static Schema createAvroSchema(List<Field> arrowFields) {
    return createAvroSchema(arrowFields, GENERIC_RECORD_TYPE_NAME);
  }

  /**
   * Overload provided for convenience, sets name = GENERIC_RECORD_TYPE_NAME and namespace = null.
   */
  public static Schema createAvroSchema(List<Field> arrowFields, DictionaryProvider dictionaries) {
    return createAvroSchema(arrowFields, GENERIC_RECORD_TYPE_NAME, null, dictionaries);
  }

  private static <T> T buildRecordSchema(
      SchemaBuilder.RecordBuilder<T> builder,
      List<Field> fields,
      String namespace,
      DictionaryProvider dictionaries) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("Record field must have at least one child field");
    }
    SchemaBuilder.FieldAssembler<T> assembler = builder.namespace(namespace).fields();
    for (Field field : fields) {
      assembler = buildFieldSchema(assembler, field, namespace, dictionaries);
    }
    return assembler.endRecord();
  }

  private static <T> SchemaBuilder.FieldAssembler<T> buildFieldSchema(
      SchemaBuilder.FieldAssembler<T> assembler,
      Field field,
      String namespace,
      DictionaryProvider dictionaries) {

    return assembler
        .name(field.getName())
        .type(buildTypeSchema(SchemaBuilder.builder(), field, namespace, dictionaries))
        .noDefault();
  }

  private static <T> T buildTypeSchema(
      SchemaBuilder.TypeBuilder<T> builder,
      Field field,
      String namespace,
      DictionaryProvider dictionaries) {

    // Nullable unions need special handling, since union types cannot be directly nested
    if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Union) {
      boolean unionNullable = field.getChildren().stream().anyMatch(Field::isNullable);
      if (unionNullable) {
        SchemaBuilder.UnionAccumulator<T> union = builder.unionOf().nullType();
        return addTypesToUnion(union, field.getChildren(), namespace, dictionaries);
      } else {
        Field headType = field.getChildren().get(0);
        List<Field> tailTypes = field.getChildren().subList(1, field.getChildren().size());
        SchemaBuilder.UnionAccumulator<T> union =
            buildBaseTypeSchema(builder.unionOf(), headType, namespace, dictionaries);
        return addTypesToUnion(union, tailTypes, namespace, dictionaries);
      }
    } else if (field.isNullable()) {
      return buildBaseTypeSchema(builder.nullable(), field, namespace, dictionaries);
    } else {
      return buildBaseTypeSchema(builder, field, namespace, dictionaries);
    }
  }

  private static <T> T buildArraySchema(
      SchemaBuilder.ArrayBuilder<T> builder,
      Field listField,
      String namespace,
      DictionaryProvider dictionaries) {
    if (listField.getChildren().size() != 1) {
      throw new IllegalArgumentException("List field must have exactly one child field");
    }
    Field itemField = listField.getChildren().get(0);
    return buildTypeSchema(builder.items(), itemField, namespace, dictionaries);
  }

  private static <T> T buildMapSchema(
      SchemaBuilder.MapBuilder<T> builder,
      Field mapField,
      String namespace,
      DictionaryProvider dictionaries) {
    if (mapField.getChildren().size() != 1) {
      throw new IllegalArgumentException("Map field must have exactly one child field");
    }
    Field entriesField = mapField.getChildren().get(0);
    if (mapField.getChildren().size() != 1) {
      throw new IllegalArgumentException("Map entries must have exactly two child fields");
    }
    Field keyField = entriesField.getChildren().get(0);
    Field valueField = entriesField.getChildren().get(1);
    if (keyField.getType().getTypeID() != ArrowType.ArrowTypeID.Utf8 || keyField.isNullable()) {
      throw new IllegalArgumentException(
          "Map keys must be of type string and cannot be nullable for conversion to Avro");
    }
    return buildTypeSchema(builder.values(), valueField, namespace, dictionaries);
  }

  private static <T> T buildBaseTypeSchema(
      SchemaBuilder.BaseTypeBuilder<T> builder,
      Field field,
      String namespace,
      DictionaryProvider dictionaries) {

    ArrowType.ArrowTypeID typeID = field.getType().getTypeID();

    switch (typeID) {
      case Null:
        return builder.nullType();

      case Bool:
        return builder.booleanType();

      case Int:
        if (field.getDictionary() != null) {
          if (dictionaries == null) {
            throw new IllegalArgumentException(
                "Field references a dictionary but no dictionaries were provided: "
                    + field.getName());
          }
          Dictionary dictionary = dictionaries.lookup(field.getDictionary().getId());
          if (dictionary == null) {
            throw new IllegalArgumentException(
                "Field references a dictionary that does not exist: "
                    + field.getName()
                    + ", dictionary ID = "
                    + field.getDictionary().getId());
          }
          if (dictionaryIsValidEnum(dictionary)) {
            String[] symbols = dictionarySymbols(dictionary);
            return builder.enumeration(field.getName()).symbols(symbols);
          } else {
            Field decodedField =
                new Field(
                    field.getName(),
                    dictionary.getVector().getField().getFieldType(),
                    dictionary.getVector().getField().getChildren());
            return buildBaseTypeSchema(builder, decodedField, namespace, dictionaries);
          }
        }

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
        return builder.stringType();

      case Binary:
        return builder.bytesType();

      case FixedSizeBinary:
        ArrowType.FixedSizeBinary fixedType = (ArrowType.FixedSizeBinary) field.getType();
        String fixedTypeName = field.getName();
        int fixedTypeWidth = fixedType.getByteWidth();
        return builder.fixed(fixedTypeName).size(fixedTypeWidth);

      case Decimal:
        ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
        return builder.type(
            LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                .addToSchema(
                    Schema.createFixed(
                        field.getName(), namespace, "", decimalType.getBitWidth() / 8)));

      case Date:
        return builder.type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));

      case Time:
        ArrowType.Time timeType = (ArrowType.Time) field.getType();
        if ((timeType.getUnit() == TimeUnit.SECOND || timeType.getUnit() == TimeUnit.MILLISECOND)) {
          // Second and millisecond time types are encoded as time-millis (INT)
          return builder.type(
              LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
        } else {
          // All other time types (micro, nano) are encoded as time-micros (LONG)
          return builder.type(
              LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)));
        }

      case Timestamp:
        ArrowType.Timestamp timestampType = (ArrowType.Timestamp) field.getType();
        LogicalType timestampLogicalType = timestampLogicalType(timestampType);
        return builder.type(timestampLogicalType.addToSchema(Schema.create(Schema.Type.LONG)));

      case Struct:
        String childNamespace =
            namespace == null ? field.getName() : namespace + "." + field.getName();
        return buildRecordSchema(
            builder.record(field.getName()), field.getChildren(), childNamespace, dictionaries);

      case List:
      case FixedSizeList:
        // Arrow uses "$data$" as the field name for list items, that is not a valid Avro name
        Field itemField = field.getChildren().get(0);
        if (ListVector.DATA_VECTOR_NAME.equals(itemField.getName())) {
          Field safeItemField =
              new Field("item", itemField.getFieldType(), itemField.getChildren());
          Field safeListField =
              new Field(field.getName(), field.getFieldType(), List.of(safeItemField));
          return buildArraySchema(builder.array(), safeListField, namespace, dictionaries);
        } else {
          return buildArraySchema(builder.array(), field, namespace, dictionaries);
        }

      case Map:
        return buildMapSchema(builder.map(), field, namespace, dictionaries);

      default:
        throw new IllegalArgumentException(
            "Element type not supported for Avro conversion: " + typeID.name());
    }
  }

  private static <T> T addTypesToUnion(
      SchemaBuilder.UnionAccumulator<T> accumulator,
      List<Field> unionFields,
      String namespace,
      DictionaryProvider dictionaries) {
    for (var field : unionFields) {
      accumulator = buildBaseTypeSchema(accumulator.and(), field, namespace, dictionaries);
    }
    return accumulator.endUnion();
  }

  private static LogicalType timestampLogicalType(ArrowType.Timestamp timestampType) {
    boolean zoneAware = timestampType.getTimezone() != null;
    if (timestampType.getUnit() == TimeUnit.NANOSECOND) {
      return zoneAware ? LogicalTypes.timestampNanos() : LogicalTypes.localTimestampNanos();
    } else if (timestampType.getUnit() == TimeUnit.MICROSECOND) {
      return zoneAware ? LogicalTypes.timestampMicros() : LogicalTypes.localTimestampMicros();
    } else {
      // Timestamp in seconds will be cast to milliseconds, Avro does not support seconds
      return zoneAware ? LogicalTypes.timestampMillis() : LogicalTypes.localTimestampMillis();
    }
  }

  private static boolean dictionaryIsValidEnum(Dictionary dictionary) {

    if (dictionary.getVectorType().getTypeID() != ArrowType.ArrowTypeID.Utf8) {
      return false;
    }

    VarCharVector vector = (VarCharVector) dictionary.getVector();
    Set<String> symbols = new HashSet<>();

    for (int i = 0; i < vector.getValueCount(); i++) {
      if (vector.isNull(i)) {
        return false;
      }
      Text text = vector.getObject(i);
      if (text == null) {
        return false;
      }
      String symbol = text.toString();
      if (!ENUM_REGEX.matcher(symbol).matches()) {
        return false;
      }
      if (symbols.contains(symbol)) {
        return false;
      }
      symbols.add(symbol);
    }

    return true;
  }

  private static String[] dictionarySymbols(Dictionary dictionary) {

    VarCharVector vector = (VarCharVector) dictionary.getVector();
    String[] symbols = new String[vector.getValueCount()];

    for (int i = 0; i < vector.getValueCount(); i++) {
      Text text = vector.getObject(i);
      // This should never happen if dictionaryIsValidEnum() succeeded
      if (text == null) {
        throw new IllegalArgumentException("Illegal null value in enum");
      }
      symbols[i] = text.toString();
    }

    return symbols;
  }

  private static final Pattern ENUM_REGEX = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  /**
   * Create a composite Avro producer for a set of field vectors (typically the root set of a VSR).
   *
   * @param vectors The vectors that will be used to produce Avro data
   * @return The resulting composite Avro producer
   */
  public static CompositeAvroProducer createCompositeProducer(
      List<FieldVector> vectors, DictionaryProvider dictionaries) {

    List<Producer<? extends FieldVector>> producers = new ArrayList<>(vectors.size());

    for (FieldVector vector : vectors) {
      BaseAvroProducer<? extends FieldVector> producer = createProducer(vector, dictionaries);
      producers.add(producer);
    }

    return new CompositeAvroProducer(producers);
  }

  /** Overload provided for convenience, sets dictionaries = null. */
  public static CompositeAvroProducer createCompositeProducer(List<FieldVector> vectors) {

    return createCompositeProducer(vectors, null);
  }

  private static BaseAvroProducer<?> createProducer(
      FieldVector vector, DictionaryProvider dictionaries) {
    boolean nullable = vector.getField().isNullable();
    return createProducer(vector, nullable, dictionaries);
  }

  private static BaseAvroProducer<?> createProducer(
      FieldVector vector, boolean nullable, DictionaryProvider dictionaries) {

    Preconditions.checkNotNull(vector, "Arrow vector object can't be null");

    final Types.MinorType minorType = vector.getMinorType();

    // Avro understands nullable types as a union of type | null
    // Most nullable fields in a VSR will not be unions, so provide a special wrapper
    if (nullable && minorType != Types.MinorType.UNION) {
      final BaseAvroProducer<?> innerProducer = createProducer(vector, false, dictionaries);
      return new AvroNullableProducer<>(innerProducer);
    }

    if (vector.getField().getDictionary() != null) {
      if (dictionaries == null) {
        throw new IllegalArgumentException(
            "Field references a dictionary but no dictionaries were provided: "
                + vector.getField().getName());
      }
      Dictionary dictionary = dictionaries.lookup(vector.getField().getDictionary().getId());
      if (dictionary == null) {
        throw new IllegalArgumentException(
            "Field references a dictionary that does not exist: "
                + vector.getField().getName()
                + ", dictionary ID = "
                + vector.getField().getDictionary().getId());
      }
      // If a field is dictionary-encoded but cannot be represented as an Avro enum,
      // then decode it before writing
      if (dictionaryIsValidEnum(dictionary)) {
        return new AvroEnumProducer((BaseIntVector) vector);
      } else {
        BaseAvroProducer<?> dictProducer = createProducer(dictionary.getVector(), false, null);
        return new DictionaryDecodingProducer<>((BaseIntVector) vector, dictProducer);
      }
    }

    switch (minorType) {
      case NULL:
        return new AvroNullProducer((NullVector) vector);
      case BIT:
        return new AvroBooleanProducer((BitVector) vector);
      case TINYINT:
        return new AvroTinyIntProducer((TinyIntVector) vector);
      case SMALLINT:
        return new AvroSmallIntProducer((SmallIntVector) vector);
      case INT:
        return new AvroIntProducer((IntVector) vector);
      case BIGINT:
        return new AvroBigIntProducer((BigIntVector) vector);
      case UINT1:
        return new AvroUint1Producer((UInt1Vector) vector);
      case UINT2:
        return new AvroUint2Producer((UInt2Vector) vector);
      case UINT4:
        return new AvroUint4Producer((UInt4Vector) vector);
      case UINT8:
        return new AvroUint8Producer((UInt8Vector) vector);
      case FLOAT2:
        return new AvroFloat2Producer((Float2Vector) vector);
      case FLOAT4:
        return new AvroFloat4Producer((Float4Vector) vector);
      case FLOAT8:
        return new AvroFloat8Producer((Float8Vector) vector);
      case VARBINARY:
        return new AvroBytesProducer((VarBinaryVector) vector);
      case FIXEDSIZEBINARY:
        return new AvroFixedSizeBinaryProducer((FixedSizeBinaryVector) vector);
      case VARCHAR:
        return new AvroStringProducer((VarCharVector) vector);

        // Logical types

      case DECIMAL:
        return new AvroDecimalProducer((DecimalVector) vector);
      case DECIMAL256:
        return new AvroDecimal256Producer((Decimal256Vector) vector);
      case DATEDAY:
        return new AvroDateDayProducer((DateDayVector) vector);
      case DATEMILLI:
        return new AvroDateMilliProducer((DateMilliVector) vector);
      case TIMESEC:
        return new AvroTimeSecProducer((TimeSecVector) vector);
      case TIMEMILLI:
        return new AvroTimeMilliProducer((TimeMilliVector) vector);
      case TIMEMICRO:
        return new AvroTimeMicroProducer((TimeMicroVector) vector);
      case TIMENANO:
        return new AvroTimeNanoProducer((TimeNanoVector) vector);
      case TIMESTAMPSEC:
        return new AvroTimestampSecProducer((TimeStampSecVector) vector);
      case TIMESTAMPMILLI:
        return new AvroTimestampMilliProducer((TimeStampMilliVector) vector);
      case TIMESTAMPMICRO:
        return new AvroTimestampMicroProducer((TimeStampMicroVector) vector);
      case TIMESTAMPNANO:
        return new AvroTimestampNanoProducer((TimeStampNanoVector) vector);
      case TIMESTAMPSECTZ:
        return new AvroTimestampSecTzProducer((TimeStampSecTZVector) vector);
      case TIMESTAMPMILLITZ:
        return new AvroTimestampMilliTzProducer((TimeStampMilliTZVector) vector);
      case TIMESTAMPMICROTZ:
        return new AvroTimestampMicroTzProducer((TimeStampMicroTZVector) vector);
      case TIMESTAMPNANOTZ:
        return new AvroTimestampNanoTzProducer((TimeStampNanoTZVector) vector);

        // Complex types

      case STRUCT:
        StructVector structVector = (StructVector) vector;
        List<FieldVector> childVectors = structVector.getChildrenFromFields();
        Producer<?>[] childProducers = new Producer<?>[childVectors.size()];
        for (int i = 0; i < childVectors.size(); i++) {
          FieldVector childVector = childVectors.get(i);
          childProducers[i] =
              createProducer(childVector, childVector.getField().isNullable(), dictionaries);
        }
        return new AvroStructProducer(structVector, childProducers);

      case LIST:
        ListVector listVector = (ListVector) vector;
        FieldVector itemVector = listVector.getDataVector();
        Producer<?> itemProducer =
            createProducer(itemVector, itemVector.getField().isNullable(), dictionaries);
        return new AvroListProducer(listVector, itemProducer);

      case FIXED_SIZE_LIST:
        FixedSizeListVector fixedListVector = (FixedSizeListVector) vector;
        FieldVector fixedItemVector = fixedListVector.getDataVector();
        Producer<?> fixedItemProducer =
            createProducer(fixedItemVector, fixedItemVector.getField().isNullable(), dictionaries);
        return new AvroFixedSizeListProducer(fixedListVector, fixedItemProducer);

      case MAP:
        MapVector mapVector = (MapVector) vector;
        StructVector entryVector = (StructVector) mapVector.getDataVector();
        Types.MinorType keyType = entryVector.getChildrenFromFields().get(0).getMinorType();
        if (keyType != Types.MinorType.VARCHAR) {
          throw new IllegalArgumentException("MAP key type must be VARCHAR for Avro encoding");
        }
        VarCharVector keyVector = (VarCharVector) entryVector.getChildrenFromFields().get(0);
        FieldVector valueVector = entryVector.getChildrenFromFields().get(1);
        Producer<?> keyProducer = new AvroStringProducer(keyVector);
        Producer<?> valueProducer =
            createProducer(valueVector, valueVector.getField().isNullable(), dictionaries);
        Producer<?> entryProducer =
            new AvroStructProducer(entryVector, new Producer<?>[] {keyProducer, valueProducer});
        return new AvroMapProducer(mapVector, entryProducer);

        // Support for UNION and DENSEUNION is not currently available
        // This is pending fixes in the implementation of the union vectors themselves
        // https://github.com/apache/arrow-java/issues/108

      default:
        // Not all Arrow types are supported for encoding (yet)!
        String error =
            String.format(
                "Encoding Arrow type %s to Avro is not currently supported", minorType.name());
        throw new UnsupportedOperationException(error);
    }
  }
}
