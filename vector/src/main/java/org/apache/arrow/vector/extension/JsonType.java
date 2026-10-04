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
package org.apache.arrow.vector.extension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.Objects;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Canonical extension type for UTF-8 encoded RFC 8259 JSON values.
 *
 * <p>The storage type is {@link ArrowType.Utf8}, {@link ArrowType.LargeUtf8}, or {@link
 * ArrowType.Utf8View}. Values use the corresponding string vector; this type does not parse or
 * validate individual JSON values.
 *
 * <p>Register the type before reading schemas containing {@code arrow.json}:
 *
 * <pre>{@code
 * JsonType.ensureRegistered();
 * Field field = Field.nullable("json", new JsonType(ArrowType.Utf8.INSTANCE));
 * try (JsonVector vector = (JsonVector) field.createVector(allocator)) {
 *   VarCharVector storage = (VarCharVector) vector.getUnderlyingVector();
 *   storage.setSafe(0, "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
 *   vector.setValueCount(1);
 *   Text value = vector.getObject(0);
 * }
 * }</pre>
 */
public class JsonType extends ArrowType.ExtensionType {
  public static final String EXTENSION_NAME = "arrow.json";
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final ArrowType storageType;

  /** Register a prototype that can deserialize all supported JSON storage types. */
  public static void ensureRegistered() {
    ExtensionTypeRegistry.register(new JsonType(ArrowType.Utf8.INSTANCE));
  }

  /**
   * Create a JSON type backed by the specified string type.
   *
   * @param storageType Utf8, LargeUtf8, or Utf8View
   * @throws IllegalArgumentException if the storage type is not a supported string type
   */
  public JsonType(ArrowType storageType) {
    Objects.requireNonNull(storageType, "storageType");
    if (!(storageType instanceof ArrowType.Utf8)
        && !(storageType instanceof ArrowType.LargeUtf8)
        && !(storageType instanceof ArrowType.Utf8View)) {
      throw new IllegalArgumentException(
          "arrow.json requires Utf8, LargeUtf8, or Utf8View storage, got " + storageType);
    }
    this.storageType = storageType;
  }

  @Override
  public ArrowType storageType() {
    return storageType;
  }

  @Override
  public String extensionName() {
    return EXTENSION_NAME;
  }

  @Override
  public boolean extensionEquals(ExtensionType other) {
    return other instanceof JsonType && storageType.equals(other.storageType());
  }

  @Override
  public String serialize() {
    return "";
  }

  @Override
  public ArrowType deserialize(ArrowType storageType, String serializedData) {
    JsonType type = new JsonType(storageType);
    if (serializedData == null) {
      throw new InvalidExtensionMetadataException("arrow.json metadata must not be null");
    }
    if (!serializedData.isEmpty()) {
      try {
        JsonNode metadata = MAPPER.readTree(serializedData);
        if (metadata == null || !metadata.isObject()) {
          throw new InvalidExtensionMetadataException("arrow.json metadata must be a JSON object");
        }
      } catch (JsonProcessingException e) {
        throw new InvalidExtensionMetadataException("arrow.json metadata is invalid", e);
      }
    }
    return type;
  }

  @Override
  public boolean isComplex() {
    return false;
  }

  @Override
  public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator) {
    Field field = new Field(name, fieldType, Collections.emptyList());
    FieldType storageFieldType =
        new FieldType(fieldType.isNullable(), storageType, fieldType.getDictionary(), null);
    FieldVector storage = storageFieldType.createNewSingleVector(name, allocator, null);
    return new JsonVector(field, allocator, storage);
  }
}
