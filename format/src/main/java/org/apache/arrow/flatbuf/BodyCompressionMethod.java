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
 * Provided for forward compatibility in case we need to support different
 * strategies for compressing the IPC message body (like whole-body
 * compression rather than buffer-level) in the future
 */
@SuppressWarnings("unused")
public final class BodyCompressionMethod {
  private BodyCompressionMethod() { }
  /**
   * Each constituent buffer is first compressed with the indicated
   * compressor, and then written with the uncompressed length in the first 8
   * bytes as a 64-bit little-endian signed integer followed by the compressed
   * buffer bytes (and then padding as required by the protocol). The
   * uncompressed length may be set to -1 to indicate that the data that
   * follows is not compressed, which can be useful for cases where
   * compression does not yield appreciable savings.
   */
  public static final byte BUFFER = 0;

  public static final String[] names = { "BUFFER", };

  public static String name(int e) { return names[e]; }
}
