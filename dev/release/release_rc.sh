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
  echo "Usage: $0 <rc>"
  echo " e.g.: $0 1"
  exit 1
fi

rc=$1

: "${RELEASE_DEFAULT:=1}"
: "${RELEASE_PULL:=${RELEASE_DEFAULT}}"
: "${RELEASE_PUSH_TAG:=${RELEASE_DEFAULT}}"
: "${RELEASE_SIGN:=${RELEASE_DEFAULT}}"
: "${RELEASE_UPLOAD:=${RELEASE_DEFAULT}}"

cd "${SOURCE_TOP_DIR}"

if [ "${RELEASE_PULL}" -gt 0 ] || [ "${RELEASE_PUSH_TAG}" -gt 0 ]; then
  git_origin_url="$(git remote get-url origin)"
  if [ "${git_origin_url}" != "git@github.com:apache/arrow-java.git" ]; then
    echo "This script must be ran with working copy of apache/arrow-java."
    echo "The origin's URL: ${git_origin_url}"
    exit 1
  fi
fi

if [ "${RELEASE_PULL}" -gt 0 ]; then
  echo "Ensure using the latest commit"
  git checkout main
  git pull --ff-only
fi

version=$(grep -o '^  <version>.*</version>' "pom.xml" |
  sed \
    -e 's,^  <version>,,' \
    -e 's,</version>$,,')

case "${version}" in
*-SNAPSHOT)
  echo "Version isn't bumped: ${version}"
  echo "Run dev/release/bump_version.sh before you run this script."
  exit 1
  ;;
esac

rc_tag="v${version}-rc${rc}"
if [ "${RELEASE_PUSH_TAG}" -gt 0 ]; then
  echo "Tagging for RC: ${rc_tag}"
  git tag -a -m "${version} RC${rc}" "${rc_tag}"
  git push origin "${rc_tag}"
fi

rc_hash="$(git rev-list --max-count=1 "${rc_tag}")"

id="apache-arrow-java-${version}"
tar_gz="${id}.tar.gz"

if [ "${RELEASE_SIGN}" -gt 0 ]; then
  git_origin_url="$(git remote get-url origin)"
  repository="${git_origin_url#*github.com?}"
  repository="${repository%.git}"

  echo "Looking for GitHub Actions workflow on ${repository}:${rc_tag}"
  run_id=""
  while [ -z "${run_id}" ]; do
    echo "Waiting for run to start..."
    run_id=$(gh run list \
      --branch "${rc_tag}" \
      --jq ".[].databaseId" \
      --json 'databaseId' \
      --limit 1 \
      --repo "${repository}" \
      --workflow rc.yml)
    sleep 1
  done

  echo "Found GitHub Actions workflow with ID: ${run_id}"
  gh run watch --repo "${repository}" --exit-status "${run_id}"

  echo "Downloading .tar.gz from GitHub Releases"
  gh release download "${rc_tag}" \
    --dir . \
    --pattern "${tar_gz}" \
    --repo "${repository}" \
    --skip-existing

  echo "Signing tar.gz and creating checksums"
  gpg --armor --output "${tar_gz}.asc" --detach-sig "${tar_gz}"
fi

if [ "${RELEASE_UPLOAD}" -gt 0 ]; then
  echo "Uploading signature"
  gh release upload "${rc_tag}" \
    --clobber \
    --repo "${repository}" \
    "${tar_gz}.asc"
fi

echo "Draft email for dev@arrow.apache.org mailing list"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE][Java] Release Apache Arrow Java ${version} RC${rc}

Hi,

I would like to propose the following release candidate (RC${rc}) of
Apache Arrow Java version ${version}.

This release candidate is based on commit:
${rc_hash} [1]

The source release rc${rc} is hosted at [2].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release. See [3] for how to validate a release candidate.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow Java ${version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow Java ${version} because...

[1]: https://github.com/apache/arrow-java/tree/${rc_hash}
[2]: https://github.com/apache/arrow-java/releases/${rc_tag}
[3]: https://github.com/apache/arrow-java/blob/main/dev/release/README.md#verify
MAIL
echo "---------------------------------------------------------"
