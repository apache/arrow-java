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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/arrow/vector/complex/writer/BaseWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.arrow.vector.complex.writer;

<#include "/@includes/vv_imports.ftl" />

/*
 * File generated from ${.template_name} using FreeMarker.
 */
@SuppressWarnings("unused")
public interface BaseWriter extends AutoCloseable, Positionable {
  int getValueCapacity();
  void writeNull();

  public interface StructWriter extends BaseWriter {

    Field getField();

    /**
     * Whether this writer is a struct writer and is empty (has no children).
     *
     * <p>
     *   Intended only for use in determining whether to add dummy vector to
     *   avoid empty (zero-column) schema, as in JsonReader.
     * </p>
     * @return whether the struct is empty
     */
    boolean isEmptyStruct();

    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    <#if minor.typeParams?? >
    ${capName}Writer ${lowerName}(String name<#list minor.typeParams as typeParam>, ${typeParam.type} ${typeParam.name}</#list>);
    </#if>
    ${capName}Writer ${lowerName}(String name);
    </#list></#list>

    void copyReaderToField(String name, FieldReader reader);
    StructWriter struct(String name);
    ExtensionWriter extension(String name, ArrowType arrowType);
    ListWriter list(String name);
    ListWriter listView(String name);
    MapWriter map(String name);
    MapWriter map(String name, boolean keysSorted);
    void start();
    void end();
  }

  public interface ListWriter extends BaseWriter {
    void startList();
    void endList();
    void startListView();
    void endListView();
    StructWriter struct();
    ListWriter list();
    ListWriter listView();
    MapWriter map();
    MapWriter map(boolean keysSorted);
    ExtensionWriter extension(ArrowType arrowType);
    void copyReader(FieldReader reader);

    <#list vv.types as type><#list type.minor as minor>
    <#assign lowerName = minor.class?uncap_first />
    <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
    <#assign upperName = minor.class?upper_case />
    <#assign capName = minor.class?cap_first />
    ${capName}Writer ${lowerName}();
    </#list></#list>
  }

  public interface MapWriter extends ListWriter {
    void startMap();
    void endMap();

    void startEntry();
    void endEntry();

    MapWriter key();
    MapWriter value();
  }

  public interface ExtensionWriter extends BaseWriter {

    /**
     * Writes a null value.
     */
    void writeNull();

    /**
     * Writes value from the given extension holder.
     *
     * @param holder the extension holder to write
     */
    void write(ExtensionHolder holder);

    /**
     * Writes the given extension type value.
     *
     * @param value the extension type value to write
     */
    void writeExtension(Object value);

    /**
     * Adds the given extension type factory. This factory allows configuring writer implementations for specific ExtensionTypeVector.
     *
     * @param factory the extension type factory to add
     */
    void addExtensionTypeWriterFactory(ExtensionTypeWriterFactory factory);
  }

  public interface ScalarWriter extends
  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first /> ${name}Writer, </#list></#list> BaseWriter {}

  public interface ComplexWriter {
    void allocate();
    void clear();
    void copyReader(FieldReader reader);
    StructWriter rootAsStruct();
    ListWriter rootAsList();
    ListWriter rootAsListView();
    MapWriter rootAsMap(boolean keysSorted);

    void setPosition(int index);
    void setValueCount(int count);
    void reset();
  }

  public interface StructOrListWriter {
    void start();
    void end();
    StructOrListWriter struct(String name);
    /**
     * @deprecated use {@link #listOfStruct()} instead.
     */
    @Deprecated
    StructOrListWriter listoftstruct(String name);
    StructOrListWriter listOfStruct(String name);
    StructOrListWriter list(String name);
    boolean isStructWriter();
    boolean isListWriter();
    VarCharWriter varChar(String name);
    IntWriter integer(String name);
    BigIntWriter bigInt(String name);
    Float4Writer float4(String name);
    Float8Writer float8(String name);
    BitWriter bit(String name);
    VarBinaryWriter binary(String name);
  }
}
