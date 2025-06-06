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
package org.apache.arrow.flatbuf;

/**
 * ----------------------------------------------------------------------
 * The root Message type
 * This union enables us to easily send different message types without
 * redundant storage, and in the future we can easily add new message types.
 *
 * Arrow implementations do not need to implement all of the message types,
 * which may include experimental metadata types. For maximum compatibility,
 * it is best to send data using RecordBatch
 */
@SuppressWarnings("unused")
public final class MessageHeader {
  private MessageHeader() { }
  public static final byte NONE = 0;
  public static final byte Schema = 1;
  public static final byte DictionaryBatch = 2;
  public static final byte RecordBatch = 3;
  public static final byte Tensor = 4;
  public static final byte SparseTensor = 5;

  public static final String[] names = { "NONE", "Schema", "DictionaryBatch", "RecordBatch", "Tensor", "SparseTensor", };

  public static String name(int e) { return names[e]; }
}
