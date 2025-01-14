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

set -eo pipefail

if [[ "${ARROW_JAVA_BUILD:-ON}" != "ON" ]]; then
  exit
fi

source_dir=${1}
build_dir=${2}
java_jni_dist_dir=${3}

: "${BUILD_DOCS_JAVA:=OFF}"

mvn="mvn -B -DskipTests -Drat.skip=true -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

if [ "$ARROW_JAVA_SKIP_GIT_PLUGIN" ]; then
  mvn="${mvn} -Dmaven.gitcommitid.skip=true"
fi

# We want to do an out-of-source build since (when using docker) we'll pollute
# the source directory with files owned by root, but Maven does not really
# support this.  Instead, copy directories to the build directory.

rm -rf "${build_dir}"
mkdir -p "${build_dir}/arrow-format"
cp -r "${source_dir}/arrow-format" "${build_dir}"
cp -r "${source_dir}/dev" "${build_dir}"

# Instead of hardcoding the list of directories to copy, find pom.xml and then
# crawl back up to the top.  GNU realpath has --relative-to but this does not
# work on macOS

poms=$(find "${source_dir}" -not \( -path "${source_dir}"/build -prune \) -type f -name pom.xml)
if [[ "$OSTYPE" == "darwin"* ]]; then
  poms=$(echo "$poms" | xargs -n1 python -c "import sys; import os.path; print(os.path.relpath(sys.argv[1], '${source_dir}'))")
else
  poms=$(echo "$poms" | xargs -n1 realpath -s --relative-to="${source_dir}")
fi

for source_root in $(echo "${poms}" | awk -F/ '{print $1}' | sort -u); do
  cp -r "${source_dir}/${source_root}" "${build_dir}"
done

pushd "${build_dir}"

if [ "${ARROW_JAVA_SHADE_FLATBUFFERS}" == "ON" ]; then
  mvn="${mvn} -Pshade-flatbuffers"
fi

if [ "${ARROW_JAVA_CDATA}" = "ON" ]; then
  mvn="${mvn} -Darrow.c.jni.dist.dir=${java_jni_dist_dir} -Parrow-c-data"
fi

if [ "${ARROW_JAVA_JNI}" = "ON" ]; then
  mvn="${mvn} -Darrow.cpp.build.dir=${java_jni_dist_dir} -Parrow-jni"
fi

# Use `2 * ncores` threads
${mvn} -T 2C clean install

if [ "${BUILD_DOCS_JAVA}" == "ON" ]; then
  # HTTP pooling is turned of to avoid download issues https://issues.apache.org/jira/browse/ARROW-11633
  # GH-43378: Maven site plugins not compatible with multithreading
  mkdir -p "${build_dir}"/docs/java/reference
  ${mvn} -Dcheckstyle.skip=true -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false clean install site
  rsync -a target/site/apidocs/ "${build_dir}"/docs/java/reference
fi

popd
