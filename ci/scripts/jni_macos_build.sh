#!/usr/bin/env bash
#
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

# This script is like java_jni_build.sh, but is meant for release artifacts
# and hardcodes assumptions about the environment it is being run in.

set -euo pipefail

# shellcheck source=ci/scripts/util_log.sh
. "$(dirname "${0}")/util_log.sh"

github_actions_group_begin "Prepare arguments"
source_dir="$(cd "${1}" && pwd)"
arrow_dir="$(cd "${2}" && pwd)"
build_dir="${3}"
normalized_arch="$(arch)"
case "${normalized_arch}" in
arm64)
  normalized_arch=aarch_64
  ;;
i386)
  normalized_arch=x86_64
  ;;
esac
# The directory where the final binaries will be stored when scripts finish
dist_dir="${4}"
github_actions_group_end

github_actions_group_begin "Clear output directories and leftovers"
rm -rf "${build_dir}"
rm -rf "${dist_dir}"

mkdir -p "${build_dir}"
build_dir="$(cd "${build_dir}" && pwd)"
github_actions_group_end

: "${ARROW_USE_CCACHE:=ON}"
if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
  github_actions_group_begin "ccache statistics before build"
  ccache -sv 2>/dev/null || ccache -s
  github_actions_group_end
fi

github_actions_group_begin "Building Arrow C++ libraries"
install_dir="${build_dir}/cpp-install"
: "${CMAKE_UNITY_BUILD:=ON}"

export ARROW_TEST_DATA="${arrow_dir}/testing/data"
export PARQUET_TEST_DATA="${arrow_dir}/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

cmake \
  -S "${arrow_dir}/cpp" \
  -B "${build_dir}/cpp" \
  --preset=ninja-release-jni-macos \
  -DCMAKE_INSTALL_PREFIX="${install_dir}"
cmake --build "${build_dir}/cpp" --target install
github_actions_group_end

export JAVA_JNI_CMAKE_ARGS="-DProtobuf_ROOT=${build_dir}/cpp/protobuf_ep-install"
"${source_dir}/ci/scripts/jni_build.sh" \
  "${source_dir}" \
  "${install_dir}" \
  "${build_dir}" \
  "${dist_dir}"

if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
  github_actions_group_begin "ccache statistics after build"
  ccache -sv 2>/dev/null || ccache -s
  github_actions_group_end
fi

github_actions_group_begin "Checking shared dependencies for libraries"
pushd "${dist_dir}"
archery linking check-dependencies \
  --allow CoreFoundation \
  --allow Network \
  --allow Security \
  --allow libSystem \
  --allow libarrow_cdata_jni \
  --allow libarrow_dataset_jni \
  --allow libarrow_orc_jni \
  --allow libc++ \
  --allow libcurl \
  --allow libgandiva_jni \
  --allow libncurses \
  --allow libobjc \
  --allow libz \
  --allow libz3 \
  "arrow_cdata_jni/${normalized_arch}/libarrow_cdata_jni.dylib" \
  "arrow_dataset_jni/${normalized_arch}/libarrow_dataset_jni.dylib" \
  "arrow_orc_jni/${normalized_arch}/libarrow_orc_jni.dylib" \
  "gandiva_jni/${normalized_arch}/libgandiva_jni.dylib"
popd
github_actions_group_end
