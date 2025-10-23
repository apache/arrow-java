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

: "${VCPKG_ROOT:=/opt/vcpkg}"
: "${VCPKG_FEATURE_FLAGS:=-manifests}"
: "${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-linux-static-release}}"
export VCPKG_TARGET_TRIPLET

export ARROW_BUILD_TESTS=OFF

export ARROW_DATASET=ON
export ARROW_GANDIVA=ON
export ARROW_ORC=ON
export ARROW_PARQUET=ON

export AWS_EC2_METADATA_DISABLED=TRUE

install_dir="${build_dir}/cpp-install"

pushd "${arrow_dir}"
cat <<EOF | patch -p1
diff --git a/cpp/src/arrow/CMakeLists.txt b/cpp/src/arrow/CMakeLists.txt
index fcdfaa21c7..9f30cc13e3 100644
--- a/cpp/src/arrow/CMakeLists.txt
+++ b/cpp/src/arrow/CMakeLists.txt
@@ -80,7 +80,7 @@ if(ARROW_WITH_ZSTD)
 endif()
 
 if(ARROW_ORC)
-  if(ORC_SOURCE STREQUAL "SYSTEM")
+  if(orc_SOURCE STREQUAL "SYSTEM")
     list(APPEND ARROW_STATIC_INSTALL_INTERFACE_LIBS orc::orc)
   endif()
 endif()
EOF
popd
cmake \
  -S "${arrow_dir}/cpp" \
  -B "${build_dir}/cpp" \
  --preset=ninja-release-jni-linux \
  -DCMAKE_INSTALL_PREFIX="${install_dir}"
cmake --build "${build_dir}/cpp"
cmake --install "${build_dir}/cpp"
github_actions_group_end

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
