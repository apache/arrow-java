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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helpers for reading a JSON document into a generic tree of {@link Map}, {@link List}, {@link
 * String}, {@link Long}, {@link Double}, {@link Boolean} and {@code null} values, using only
 * Jackson's streaming API (jackson-core). Used by the POJO schema (de)serialization to avoid a
 * dependency on jackson-databind.
 */
final class JsonValues {

  private JsonValues() {}

  /**
   * Reads the value at the parser's current token into a generic Java object. After this call the
   * parser is positioned on the last token of the value (e.g. END_OBJECT / END_ARRAY / the scalar
   * token), matching the behavior of {@code ObjectMapper.readValue}.
   */
  static Object readValue(JsonParser parser) throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      token = parser.nextToken();
    }
    switch (token) {
      case START_OBJECT:
        return readObject(parser);
      case START_ARRAY:
        return readArray(parser);
      case VALUE_STRING:
        return parser.getValueAsString();
      case VALUE_NUMBER_INT:
        return parser.getLongValue();
      case VALUE_NUMBER_FLOAT:
        return parser.getDoubleValue();
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NULL:
        return null;
      default:
        throw new IOException("Unexpected JSON token: " + token);
    }
  }

  private static Map<String, Object> readObject(JsonParser parser) throws IOException {
    Map<String, Object> result = new LinkedHashMap<>();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String name = parser.currentName();
      parser.nextToken();
      result.put(name, readValue(parser));
    }
    return result;
  }

  private static List<Object> readArray(JsonParser parser) throws IOException {
    List<Object> result = new ArrayList<>();
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      result.add(readValue(parser));
    }
    return result;
  }

  static boolean asBoolean(Object value) {
    return Boolean.TRUE.equals(value);
  }

  static int asInt(Object value) {
    return ((Number) value).intValue();
  }

  static long asLong(Object value) {
    return ((Number) value).longValue();
  }

  static String asString(Object value) {
    return value == null ? null : value.toString();
  }

  static int[] asIntArray(Object value) {
    if (value == null) {
      return null;
    }
    List<?> list = (List<?>) value;
    int[] result = new int[list.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = ((Number) list.get(i)).intValue();
    }
    return result;
  }

  /**
   * Resolves an enum value by name, case-insensitively (matching the previous Jackson behavior).
   */
  static <E extends Enum<E>> E parseEnum(Class<E> clazz, Object value) {
    String name = asString(value);
    for (E constant : clazz.getEnumConstants()) {
      if (constant.name().equalsIgnoreCase(name)) {
        return constant;
      }
    }
    throw new IllegalArgumentException("No enum constant " + clazz.getName() + "." + name);
  }
}
