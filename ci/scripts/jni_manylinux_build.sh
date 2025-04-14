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
# The directory where the final binaries will be stored when scripts finish
dist_dir="${4}"
github_actions_group_end

github_actions_group_begin "Install Archery"
pip install -e "${arrow_dir}/dev/archery[all]"
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
devtoolset_version="$(rpm -qa "devtoolset-*-gcc" --queryformat '%{VERSION}' | grep -o "^[0-9]*")"
devtoolset_include_cpp="/opt/rh/devtoolset-${devtoolset_version}/root/usr/include/c++/${devtoolset_version}"
: "${ARROW_ACERO:=ON}"
export ARROW_ACERO
: "${ARROW_BUILD_TESTS:=OFF}"
export ARROW_BUILD_TESTS
: "${ARROW_DATASET:=ON}"
export ARROW_DATASET
: "${ARROW_GANDIVA:=ON}"
export ARROW_GANDIVA
: "${ARROW_GCS:=ON}"
: "${ARROW_JEMALLOC:=OFF}"
: "${ARROW_MIMALLOC:=ON}"
: "${ARROW_RPATH_ORIGIN:=ON}"
: "${ARROW_ORC:=ON}"
export ARROW_ORC
: "${ARROW_PARQUET:=ON}"
: "${ARROW_S3:=ON}"
: "${CMAKE_BUILD_TYPE:=release}"
: "${CMAKE_UNITY_BUILD:=ON}"
: "${VCPKG_ROOT:=/opt/vcpkg}"
: "${VCPKG_FEATURE_FLAGS:=-manifests}"
: "${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-linux-static-${CMAKE_BUILD_TYPE}}}"
: "${GANDIVA_CXX_FLAGS:=-isystem;${devtoolset_include_cpp};-isystem;${devtoolset_include_cpp}/x86_64-redhat-linux;-lpthread}"

export ARROW_TEST_DATA="${arrow_dir}/testing/data"
export PARQUET_TEST_DATA="${arrow_dir}/cpp/submodules/parquet-testing/data"
export AWS_EC2_METADATA_DISABLED=TRUE

install_dir="${build_dir}/cpp-install"

cmake \
  -S "${arrow_dir}/cpp" \
  -B "${build_dir}/cpp" \
  -DARROW_ACERO="${ARROW_ACERO}" \
  -DARROW_BUILD_SHARED=OFF \
  -DARROW_BUILD_TESTS="${ARROW_BUILD_TESTS}" \
  -DARROW_CSV="${ARROW_DATASET}" \
  -DARROW_DATASET="${ARROW_DATASET}" \
  -DARROW_SUBSTRAIT="${ARROW_DATASET}" \
  -DARROW_DEPENDENCY_SOURCE="VCPKG" \
  -DARROW_DEPENDENCY_USE_SHARED=OFF \
  -DARROW_GANDIVA_PC_CXX_FLAGS="${GANDIVA_CXX_FLAGS}" \
  -DARROW_GANDIVA="${ARROW_GANDIVA}" \
  -DARROW_GCS="${ARROW_GCS}" \
  -DARROW_JEMALLOC="${ARROW_JEMALLOC}" \
  -DARROW_JSON="${ARROW_DATASET}" \
  -DARROW_MIMALLOC="${ARROW_MIMALLOC}" \
  -DARROW_ORC="${ARROW_ORC}" \
  -DARROW_PARQUET="${ARROW_PARQUET}" \
  -DARROW_RPATH_ORIGIN="${ARROW_RPATH_ORIGIN}" \
  -DARROW_S3="${ARROW_S3}" \
  -DARROW_USE_CCACHE="${ARROW_USE_CCACHE}" \
  -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
  -DCMAKE_INSTALL_PREFIX="${install_dir}" \
  -DCMAKE_UNITY_BUILD="${CMAKE_UNITY_BUILD}" \
  -DGTest_SOURCE=BUNDLED \
  -DORC_SOURCE=BUNDLED \
  -DORC_PROTOBUF_EXECUTABLE="${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/tools/protobuf/protoc" \
  -DPARQUET_BUILD_EXAMPLES=OFF \
  -DPARQUET_BUILD_EXECUTABLES=OFF \
  -DPARQUET_REQUIRE_ENCRYPTION=OFF \
  -DVCPKG_MANIFEST_MODE=OFF \
  -DVCPKG_TARGET_TRIPLET="${VCPKG_TARGET_TRIPLET}" \
  -GNinja
cmake --build "${build_dir}/cpp"
cmake --install "${build_dir}/cpp"
github_actions_group_end

if [ "${ARROW_RUN_TESTS:-OFF}" = "ON" ]; then
  github_actions_group_begin "Running Arrow C++ libraries tests"
  # MinIO is required
  exclude_tests="arrow-s3fs-test"
  case $(arch) in
  aarch64)
    # GCS testbench is crashed on aarch64:
    # ImportError: ../grpc/_cython/cygrpc.cpython-38-aarch64-linux-gnu.so:
    # undefined symbol: vtable for std::__cxx11::basic_ostringstream<
    #   char, std::char_traits<char>, std::allocator<char> >
    exclude_tests="${exclude_tests}|arrow-gcsfs-test"
    ;;
  esac
  # unstable
  exclude_tests="${exclude_tests}|arrow-acero-asof-join-node-test"
  exclude_tests="${exclude_tests}|arrow-acero-hash-join-node-test"
  # external dependency
  exclude_tests="${exclude_tests}|arrow-gcsfs-test"
  # strptime
  exclude_tests="${exclude_tests}|arrow-utility-test"
  ctest \
    --exclude-regex "${exclude_tests}" \
    --label-regex unittest \
    --output-on-failure \
    --parallel "$(nproc)" \
    --test-dir "${build_dir}/cpp" \
    --timeout 300
  github_actions_group_end
fi

JAVA_JNI_CMAKE_ARGS="-DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
JAVA_JNI_CMAKE_ARGS="${JAVA_JNI_CMAKE_ARGS} -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}"
export JAVA_JNI_CMAKE_ARGS
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
archery linking check-dependencies \
  --allow ld-linux-aarch64 \
  --allow ld-linux-x86-64 \
  --allow libc \
  --allow libdl \
  --allow libgcc_s \
  --allow libm \
  --allow libpthread \
  --allow librt \
  --allow libstdc++ \
  --allow libz \
  --allow linux-vdso \
  arrow_cdata_jni/"${normalized_arch}"/libarrow_cdata_jni.so \
  arrow_dataset_jni/"${normalized_arch}"/libarrow_dataset_jni.so \
  arrow_orc_jni/"${normalized_arch}"/libarrow_orc_jni.so \
  gandiva_jni/"${normalized_arch}"/libgandiva_jni.so
popd
github_actions_group_end
