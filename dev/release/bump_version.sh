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

set -eu

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  echo " e.g.: $0 19.0.1"
  echo " e.g.: $0 20.0.0-SNAPSHOT"
  exit 1
fi

version=$1

if [ ! -f "${SOURCE_DIR}/.env" ]; then
  echo "You must create ${SOURCE_DIR}/.env"
  echo "You can use ${SOURCE_DIR}/.env.example as template"
  exit 1
fi
. "${SOURCE_DIR}/.env"

cd "${SOURCE_TOP_DIR}"

git_origin_url="$(git remote get-url origin)"
case "${git_origin_url}" in
*apache/arrow-java*)
  echo "You must use your fork: ${git_origin_url}"
  exit 1
  ;;
*)
  : # OK
  ;;
esac

branch="bump-version-${version}"
git switch -c "${branch}" main
mvn versions:set "-DnewVersion=${version}" -DprocessAllModules -DgenerateBackupPoms=false
case "${version}" in
*-SNAPSHOT)
  tag=main
  ;;
*)
  tag=v${version}
  ;;
esac
mvn versions:set-scm-tag "-DnewTag=${tag}" -DgenerateBackupPoms=false -pl :arrow-java-root
mvn versions:set-scm-tag "-DnewTag=${tag}" -DgenerateBackupPoms=false -pl :arrow-bom
git add "pom.xml"
git add "**/pom.xml"
git commit -m "MINOR: Bump version to ${version}"
git push --set-upstream origin "${branch}"
gh pr create --fill --repo apache/arrow-java
