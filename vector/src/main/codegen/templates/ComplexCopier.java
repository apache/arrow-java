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

import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/impl/ComplexCopier.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

<#function is_timestamp_tz type>
  <#return type?starts_with("TimeStamp") && type?ends_with("TZ")>
</#function>

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ComplexCopier {

  /**
   * Do a deep copy of the value in input into output
   * @param input field to read from
   * @param output field to write to
   */
  public static void copy(FieldReader reader, FieldWriter writer) {

    final MinorType mt = reader.getMinorType();

      switch (mt) {

      case LIST:
      case LISTVIEW:
      case LARGELIST:
      case LARGELISTVIEW:
      case FIXED_SIZE_LIST:
        if (reader.isSet()) {
          writer.startList();
          while (reader.next()) {
            FieldReader childReader = reader.reader();
            FieldWriter childWriter = getListWriterForReader(childReader, writer);
            if (childReader.isSet()) {
              copy(childReader, childWriter);
            } else {
              childWriter.writeNull();
            }
          }
          writer.endList();
        } else {
          writer.writeNull();
        }
        break;
      case MAP:
        if (reader.isSet()) {
          UnionMapReader mapReader = (UnionMapReader) reader;
          writer.startMap();
          while (mapReader.next()) {
            FieldReader structReader = reader.reader();
            if (structReader.isSet()) {
              writer.startEntry();
              copy(
                  mapReader.key(),
                  getMapWriterForReader(mapReader.key(), writer.key(), MapVector.KEY_NAME));
              copy(
                  mapReader.value(),
                  getMapWriterForReader(mapReader.value(), writer.value(), MapVector.VALUE_NAME));
              writer.endEntry();
            } else {
              writer.writeNull();
            }
          }
          writer.endMap();
        } else {
          writer.writeNull();
        }
        break;
      case STRUCT:
        if (reader.isSet()) {
          writer.start();
          for(String name : reader){
            FieldReader childReader = reader.reader(name);
            if (childReader.getMinorType() != Types.MinorType.NULL) {
              FieldWriter childWriter = getStructWriterForReader(childReader, writer, name);
              if (childReader.isSet()) {
                copy(childReader, childWriter);
              } else {
                childWriter.writeNull();
              }
            }
          }
          writer.end();
        } else {
          writer.writeNull();
        }
        break;
      case EXTENSIONTYPE:
        if (reader.isSet()) {
          Object value = reader.readObject();
          if (value != null) {
            // Use getExtensionType() rather than getField().getType(): holder-backed
            // readers carry their type on the ExtensionHolder and have no Field.
            writer.writeExtension(value, reader.getExtensionType());
          }
        } else {
          writer.writeNull();
        }
        break;
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) >

      case ${name?upper_case}:
        if (reader.isSet()) {
          Nullable${name}Holder ${uncappedName}Holder = new Nullable${name}Holder();
          reader.read(${uncappedName}Holder);
          if (${uncappedName}Holder.isSet == 1) {
            <#if is_timestamp_tz(minor.class)>
            ${name}Holder holder = new ${name}Holder();
            holder.value = ${uncappedName}Holder.value;
            holder.timezone = ${uncappedName}Holder.timezone;
            writer.write(holder);
            <#else>
            writer.write${name}(<#list fields as field>${uncappedName}Holder.${field.name}<#if field_has_next>, </#if></#list><#if minor.class?starts_with("Decimal")>, new ArrowType.Decimal(${uncappedName}Holder.precision, ${uncappedName}Holder.scale, ${name}Holder.WIDTH * 8)</#if>);
            </#if>
          }
        } else {
          writer.writeNull();
        }
        break;

  </#if>
  </#list></#list>
      }
 }

  private static FieldWriter getStructWriterForReader(FieldReader reader, StructWriter writer, String name) {
    switch (reader.getMinorType()) {
    <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
    <#assign fields = minor.fields!type.fields />
    <#assign uncappedName = name?uncap_first/>
    <#if !minor.typeParams??>
    case ${name?upper_case}:
      return (FieldWriter) writer.<#if name == "Int">integer<#else>${uncappedName}</#if>(name);
    </#if>
    <#if minor.class?starts_with("Decimal")>
    case ${name?upper_case}:
      if (reader.getField().getType() instanceof ArrowType.Decimal) {
        ArrowType.Decimal type = (ArrowType.Decimal) reader.getField().getType();
        return (FieldWriter) writer.${uncappedName}(name, type.getScale(), type.getPrecision());
      } else {
        return (FieldWriter) writer.${uncappedName}(name);
      }
    </#if>
    <#if is_timestamp_tz(minor.class)>
    case ${name?upper_case}:
      if (reader.getField().getType() instanceof ArrowType.Timestamp) {
        ArrowType.Timestamp type = (ArrowType.Timestamp) reader.getField().getType();
        return (FieldWriter) writer.${uncappedName}(name, type.getTimezone());
      } else {
        throw new UnsupportedOperationException(
            "Cannot copy " + reader.getMinorType() + " from a field of type "
                + reader.getField().getType());
      }
    </#if>

    </#list></#list>
    case STRUCT:
      return (FieldWriter) writer.struct(name);
    case FIXED_SIZE_LIST:
    case LIST:
      return (FieldWriter) writer.list(name);
    case MAP:
      return (FieldWriter) writer.map(name);
    case LISTVIEW:
      return (FieldWriter) writer.listView(name);
    case EXTENSIONTYPE:
      ExtensionWriter extensionWriter = writer.extension(name, reader.getExtensionType());
      return (FieldWriter) extensionWriter;
    default:
      throw new UnsupportedOperationException(reader.getMinorType().toString());
    }
  }

  private static FieldWriter getListWriterForReader(FieldReader reader, ListWriter writer) {
    switch (reader.getMinorType()) {
    <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
    <#assign fields = minor.fields!type.fields />
    <#assign uncappedName = name?uncap_first/>
    <#if !minor.typeParams?? || minor.class?starts_with("Decimal") || is_timestamp_tz(minor.class) >
    case ${name?upper_case}:
    return (FieldWriter) writer.<#if name == "Int">integer<#else>${uncappedName}</#if>();
    </#if>
    </#list></#list>
    case STRUCT:
      return (FieldWriter) writer.struct();
    case FIXED_SIZE_LIST:
    case LIST:
    case MAP:
    case NULL:
      return (FieldWriter) writer.list();
    case LISTVIEW:
      return (FieldWriter) writer.listView();
    case EXTENSIONTYPE:
      ExtensionWriter extensionWriter = writer.extension(reader.getExtensionType());
      return (FieldWriter) extensionWriter;
    default:
      throw new UnsupportedOperationException(reader.getMinorType().toString());
    }
  }

  /**
   * Return the writer for a map key or value.
   *
   * @param reader the key or value reader
   * @param writer the map writer, already in key or value mode
   * @param name the entry field to write: {@link MapVector#KEY_NAME} or
   *     {@link MapVector#VALUE_NAME}
   */
  private static FieldWriter getMapWriterForReader(
      FieldReader reader, MapWriter writer, String name) {
    switch (reader.getMinorType()) {
    <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
    <#assign fields = minor.fields!type.fields />
    <#assign uncappedName = name?uncap_first/>
    <#if !minor.typeParams?? || minor.class?starts_with("Decimal") >
      case ${name?upper_case}:
      return (FieldWriter) writer.<#if name == "Int">integer<#else>${uncappedName}</#if>();
    </#if>
    <#if is_timestamp_tz(minor.class)>
      case ${name?upper_case}:
        if (reader.getField().getType() instanceof ArrowType.Timestamp) {
          ArrowType.Timestamp type = (ArrowType.Timestamp) reader.getField().getType();
          // The map writer writes into the entry struct, so create the key or value field with
          // the source timezone, as the struct lookup does.
          return (FieldWriter) ((FieldWriter) writer).${uncappedName}(name, type.getTimezone());
        } else {
          throw new UnsupportedOperationException(
              "Cannot copy " + reader.getMinorType() + " from a field of type "
                  + reader.getField().getType());
        }
    </#if>
    </#list></#list>
      case STRUCT:
        return (FieldWriter) writer.struct();
      case FIXED_SIZE_LIST:
      case LIST:
      case NULL:
        return (FieldWriter) writer.list();
      case LISTVIEW:
        return (FieldWriter) writer.listView();
      case MAP:
        return (FieldWriter) writer.map(false);
      case EXTENSIONTYPE:
        ExtensionWriter extensionWriter = writer.extension(reader.getExtensionType());
        return (FieldWriter) extensionWriter;
      default:
        throw new UnsupportedOperationException(reader.getMinorType().toString());
    }
  }
}
