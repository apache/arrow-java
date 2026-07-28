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

set -ex

if [[ "${ARROW_JAVA_TEST:-ON}" != "ON" ]]; then
  exit
fi

source_dir="$(cd "${1}" && pwd)"
build_dir=${2}
java_jni_dist_dir=${3}

if [ -d "${java_jni_dist_dir}" ]; then
  java_jni_dist_dir="$(cd "${java_jni_dist_dir}" && pwd)"
fi

mvn=(
  mvn
  -B
  -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
  -T
  2C
  -Denforcer.skip=true
)

run_tests() {
  local log_name=$1
  shift

  if [[ "${ARROW_JAVA_TEST_PREBUILT:-OFF}" = "ON" ]]; then
    run_prebuilt_tests "${log_name}" "${@}" -DfailIfNoTests=false surefire:test
  else
    "${@}" test
  fi
}

run_prebuilt_tests() {
  local log_name=$1
  shift

  set -o pipefail
  "${@}" | tee "${source_dir}/${log_name}"

  if grep -E "Compiling [0-9]+ source files?" "${source_dir}/${log_name}"; then
    echo "Unexpected compilation occurred while running prebuilt tests."
    exit 1
  fi

  if ! grep -q "Tests run:" "${source_dir}/${log_name}"; then
    echo "No surefire test summary found; tests may have been skipped."
    exit 1
  fi
}

pushd "${build_dir}"

if [[ "${ARROW_JAVA_TEST_BASE:-ON}" = "ON" ]]; then
  run_tests \
    surefire.log \
    "${mvn[@]}" \
    -Darrow.test.dataRoot="${source_dir}/testing/data"

  if [[ "${ARROW_JAVA_TEST_PREBUILT:-OFF}" = "ON" ]]; then
    run_prebuilt_tests \
      opens-surefire.log \
      "${mvn[@]}" \
      -DfailIfNoTests=false \
      -pl memory/memory-core \
      org.apache.maven.plugins:maven-surefire-plugin:test@opens-tests
  fi
fi

projects=()
if [ "${ARROW_JAVA_JNI}" = "ON" ]; then
  projects+=(adapter/orc)
  projects+=(dataset)
  projects+=(gandiva)
fi
if [ "${#projects[@]}" -gt 0 ]; then
  run_tests \
    jni-surefire.log \
    "${mvn[@]}" \
    -Parrow-jni \
    -pl "$(
      IFS=,
      echo \""${projects[*]}"\"
    )" \
    -Darrow.cpp.build.dir="${java_jni_dist_dir}"
fi

if [ "${ARROW_JAVA_CDATA}" = "ON" ]; then
  run_tests \
    cdata-surefire.log \
    "${mvn[@]}" \
    -Parrow-c-data \
    -pl c \
    -Darrow.c.jni.dist.dir="${java_jni_dist_dir}"
fi

popd
