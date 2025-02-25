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

import org.apache.arrow.adapter.avro.producers.AvroBooleanProducer;
import org.apache.arrow.adapter.avro.producers.AvroBytesProducer;
import org.apache.arrow.adapter.avro.producers.AvroDoubleProducer;
import org.apache.arrow.adapter.avro.producers.AvroFixedProducer;
import org.apache.arrow.adapter.avro.producers.AvroFloatProducer;
import org.apache.arrow.adapter.avro.producers.AvroIntProducer;
import org.apache.arrow.adapter.avro.producers.AvroLongProducer;
import org.apache.arrow.adapter.avro.producers.AvroNullProducer;
import org.apache.arrow.adapter.avro.producers.AvroNullableProducer;
import org.apache.arrow.adapter.avro.producers.AvroStringProducer;
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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BigIntVector;
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
import org.apache.arrow.vector.types.Types;

import java.util.ArrayList;
import java.util.List;

public class ArrowToAvroUtils {

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

    if (nullable) {
      final BaseAvroProducer<?> innerProducer = createProducer(vector, false);
      return new AvroNullableProducer<>(innerProducer);
    }

    final Types.MinorType minorType = vector.getMinorType();

    switch (minorType) {

      // Primitive types with direct mapping to Avro

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
        return new AvroDecimalProducer.FixedDecimalProducer((DecimalVector) vector, DecimalVector.TYPE_WIDTH);
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

      // Not all Arrow types are supported for encoding (yet)!

      default:
        String error = String.format("Encoding Arrow type %s to Avro is not currently supported", minorType.name());
        throw new UnsupportedOperationException(error);
    }
  }
}
