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

set -euo pipefail

# shellcheck source=ci/scripts/util_log.sh
. "$(dirname "${0}")/util_log.sh"

github_actions_group_begin "Prepare arguments"
source_dir="$(cd "${1}" && pwd)"
arrow_dir="$(cd "${2}" && pwd)"
build_dir="${3}"
# The directory where the final binaries will be stored when scripts finish
dist_dir="${4}"
github_actions_group_end

github_actions_group_begin "Clear output directories and leftovers"
rm -rf "${build_dir}"

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
: "${ARROW_ACERO:=ON}"
export ARROW_ACERO
: "${ARROW_BUILD_TESTS:=OFF}" # TODO: ON
export ARROW_BUILD_TESTS
: "${ARROW_DATASET:=ON}"
export ARROW_DATASET
: "${ARROW_ORC:=ON}"
export ARROW_ORC
: "${ARROW_PARQUET:=ON}"
: "${ARROW_S3:=ON}"
: "${CMAKE_BUILD_TYPE:=release}"
: "${CMAKE_UNITY_BUILD:=ON}"

export ARROW_TEST_DATA="${arrow_dir}/testing/data"
export PARQUET_TEST_DATA="${arrow_dir}/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

cmake \
  -S "${arrow_dir}/cpp" \
  -B "${build_dir}/cpp" \
  -DARROW_ACERO="${ARROW_ACERO}" \
  -DARROW_BUILD_SHARED=OFF \
  -DARROW_BUILD_TESTS="${ARROW_BUILD_TESTS}" \
  -DARROW_CSV="${ARROW_DATASET}" \
  -DARROW_DATASET="${ARROW_DATASET}" \
  -DARROW_SUBSTRAIT="${ARROW_DATASET}" \
  -DARROW_DEPENDENCY_USE_SHARED=OFF \
  -DARROW_ORC="${ARROW_ORC}" \
  -DARROW_PARQUET="${ARROW_PARQUET}" \
  -DARROW_S3="${ARROW_S3}" \
  -DARROW_USE_CCACHE="${ARROW_USE_CCACHE}" \
  -DARROW_WITH_BROTLI=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZSTD=ON \
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
  -DCMAKE_INSTALL_PREFIX="${install_dir}" \
  -DCMAKE_UNITY_BUILD="${CMAKE_UNITY_BUILD}" \
  -GNinja
cmake --build "${build_dir}/cpp"
cmake --install "${build_dir}/cpp"
echo "--- ${install_dir}/lib/cmake/Arrow/ArrowConfig.cmake ---"
cat "${install_dir}/lib/cmake/Arrow/ArrowConfig.cmake"
echo "--- ${install_dir}/lib/cmake/Arrow/ArrowTargets.cmake ---"
cat "${install_dir}/lib/cmake/Arrow/ArrowTargets.cmake"
echo "--- ${install_dir}/lib/cmake/Arrow/ArrowTargets-release.cmake ---"
cat "${install_dir}/lib/cmake/Arrow/ArrowTargets-release.cmake"
github_actions_group_end

if [ "${ARROW_RUN_TESTS:-OFF}" = "ON" ]; then
  github_actions_group_begin "Running Arrow C++ libraries tests"
  # MinIO is required
  exclude_tests="arrow-s3fs-test"
  # unstable
  exclude_tests="${exclude_tests}|arrow-compute-hash-join-node-test"
  exclude_tests="${exclude_tests}|arrow-dataset-scanner-test"
  # strptime
  exclude_tests="${exclude_tests}|arrow-utility-test"
  ctest \
    --exclude-regex "${exclude_tests}" \
    --label-regex unittest \
    --output-on-failure \
    --parallel "$(nproc)" \
    --timeout 300
  github_actions_group_end
fi

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
normalized_arch="$(arch)"
case "${normalized_arch}" in
aarch64)
  normalized_arch=aarch_64
  ;;
esac
pushd "${dist_dir}"
# TODO
# archery linking check-dependencies \
#   --allow libm \
#   --allow librt \
#   --allow libz \
#   arrow_cdata_jni/"${normalized_arch}"/libarrow_cdata_jni.dll \
#   arrow_dataset_jni/"${normalized_arch}"/libarrow_dataset_jni.dll \
#   arrow_orc_jni/"${normalized_arch}"/libarrow_orc_jni.dll
popd
github_actions_group_end
