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
package org.apache.arrow.vector.types.pojo;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;

/** A POJO representation of Arrow Dictionary metadata. */
public class DictionaryEncoding {

  private final long id;
  private final boolean ordered;
  private final Int indexType;

  /**
   * Constructs a new instance.
   *
   * @param id The ID of the dictionary to use for encoding.
   * @param ordered Whether the keys in values in the dictionary are ordered.
   * @param indexType (nullable). The integer type to use for indexing in the dictionary. Defaults
   *     to a signed 32 bit integer.
   */
  public DictionaryEncoding(long id, boolean ordered, Int indexType) {
    this.id = id;
    this.ordered = ordered;
    this.indexType = indexType == null ? new Int(32, true) : indexType;
  }

  public long getId() {
    return id;
  }

  public boolean isOrdered() {
    return ordered;
  }

  public Int getIndexType() {
    return indexType;
  }

  /** Serializes this encoding to JSON. */
  void serialize(JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField("id", id);
    generator.writeBooleanField("isOrdered", ordered);
    generator.writeFieldName("indexType");
    indexType.writeJson(generator);
    generator.writeEndObject();
  }

  /** Builds a DictionaryEncoding from a parsed JSON object. */
  @SuppressWarnings("unchecked")
  static DictionaryEncoding fromJson(Map<String, Object> object) {
    Object indexTypeObj = object.get("indexType");
    Int indexType = null;
    if (indexTypeObj != null) {
      Map<String, Object> indexTypeMap = (Map<String, Object>) indexTypeObj;
      indexType =
          (Int) ArrowType.getArrowType(JsonValues.asString(indexTypeMap.get("name")), indexTypeMap);
    }
    return new DictionaryEncoding(
        JsonValues.asLong(object.get("id")),
        JsonValues.asBoolean(object.get("isOrdered")),
        indexType);
  }

  @Override
  public String toString() {
    return "DictionaryEncoding[id=" + id + ",ordered=" + ordered + ",indexType=" + indexType + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof DictionaryEncoding)) {
      return false;
    }
    DictionaryEncoding that = (DictionaryEncoding) o;
    return id == that.id && ordered == that.ordered && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, ordered, indexType);
  }
}
