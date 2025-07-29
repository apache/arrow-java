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

# For --mount=type=secret: The GITHUB_TOKEN is the only real secret
# but we use --mount=type=secret for GITHUB_REPOSITORY_OWNER and
# VCPKG_BINARY_SOURCES too because we don't want to store them into
# the built image in order to easily reuse the built image cache.
#
# Install the libraries required by Gandiva to run. Use enable
# llvm[enable-rtti] in the vcpkg.json to avoid link problems in
# Gandiva.
RUN --mount=type=secret,id=github_repository_owner \
    --mount=type=secret,id=github_token \
    --mount=type=secret,id=vcpkg_binary_sources \
      export GITHUB_REPOSITORY_OWNER=$(cat /run/secrets/github_repository_owner); \
      export GITHUB_TOKEN=$(cat /run/secrets/github_token); \
      export VCPKG_BINARY_SOURCES=$(cat /run/secrets/vcpkg_binary_sources); \
      vcpkg install \
        --clean-after-build \
        --x-install-root=${VCPKG_ROOT}/installed \
        --x-manifest-root=/arrow/ci/vcpkg \
        --x-feature=dev \
        --x-feature=flight \
        --x-feature=gcs \
        --x-feature=json \
        --x-feature=parquet \
        --x-feature=gandiva \
        --x-feature=s3 && \
      rm -rf ~/.config/NuGet/

# Install Java
# We need Java for JNI headers, but we don't invoke Maven in this build.
ARG java=11
RUN yum install -y java-$java-openjdk-devel && yum clean all

# For ci/scripts/{cpp,java}_*.sh
ENV ARROW_HOME=/tmp/local \
    ARROW_JAVA_CDATA=ON \
    ARROW_JAVA_JNI=ON \
    ARROW_USE_CCACHE=ON

LABEL org.opencontainers.image.source https://github.com/apache/arrow-java
