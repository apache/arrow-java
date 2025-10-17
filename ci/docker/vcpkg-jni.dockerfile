# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARG base
FROM ${base}

# Install Java
# We need Java for JNI headers, but we don't invoke Maven in this build.
ARG java=11
RUN dnf install -y java-$java-openjdk-devel && dnf clean all

# For ci/scripts/{cpp,java}_*.sh
ENV ARROW_HOME=/tmp/local \
    ARROW_JAVA_CDATA=ON \
    ARROW_JAVA_JNI=ON \
    ARROW_USE_CCACHE=ON

LABEL org.opencontainers.image.source https://github.com/apache/arrow-java
