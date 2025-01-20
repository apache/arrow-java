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
jni_build_dir="$(cd "${2}" && pwd)"
dist_dir="${3}"
rm -rf "${dist_dir}"
mkdir -p "${dist_dir}"
dist_dir="$(cd "${dist_dir}" && pwd)"
github_actions_group_end

github_actions_group_begin "Clear old artifacts"
# Ensure that there is no old artifacts inside the maven repository
maven_repo=~/.m2/repository/org/apache/arrow
if [ -d "$maven_repo" ]; then
  find "$maven_repo" \
    "(" -name "*.jar" -o -name "*.zip" -o -name "*.pom" ")" \
    -exec echo {} ";" \
    -exec rm -rf {} ";"
fi
github_actions_group_end

github_actions_group_begin "Generate dummy GPG key"
# Generate dummy GPG key for -Papache-release.
# -Papache-release generates signs (*.asc) of artifacts.
# We don't use these signs in our release process.
(
  echo "Key-Type: RSA"
  echo "Key-Length: 4096"
  echo "Name-Real: Build"
  echo "Name-Email: build@example.com"
  echo "%no-protection"
) |
  gpg --full-generate-key --batch
github_actions_group_end

pushd "${source_dir}"
github_actions_group_begin "Build .jar"
# build the entire project
mvn \
  --no-transfer-progress \
  -Darrow.c.jni.dist.dir="${jni_build_dir}" \
  -Darrow.cpp.build.dir="${jni_build_dir}" \
  -Papache-release \
  -Parrow-c-data \
  -Parrow-jni \
  clean \
  install
github_actions_group_end
github_actions_group_begin "Build docs"
# build docs
mvn \
  --no-transfer-progress \
  -Darrow.c.jni.dist.dir="${jni_build_dir}" \
  -Darrow.cpp.build.dir="${jni_build_dir}" \
  -Dcheckstyle.skip=true \
  -Dhttp.keepAlive=false \
  -Dmaven.wagon.http.pool=false \
  -Parrow-c-data \
  -Parrow-jni \
  site
github_actions_group_end
popd

github_actions_group_begin "Prepare artifacts"
# copy all jar, zip and pom files to the distribution folder
find ~/.m2/repository/org/apache/arrow \
  "(" \
  -name "*.jar" -o \
  -name "*.json" -o \
  -name "*.pom" -o \
  -name "*.xml" -o \
  -name "*.zip" \
  ")" \
  -exec echo "{}" ";" \
  -exec cp "{}" "${dist_dir}" ";"

for artifact in "${dist_dir}"/*; do
  sha256sum "${artifact}" >"${artifact}.sha256"
  sha512sum "${artifact}" >"${artifact}.sha512"
done
github_actions_group_end
