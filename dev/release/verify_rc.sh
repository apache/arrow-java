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
TOP_SOURCE_DIR="$(dirname "$(dirname "${SOURCE_DIR}")")"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 19.0.1 1"
  exit 1
fi

set -o pipefail
set -x

VERSION="$1"
RC="$2"

ARROW_DIST_BASE_URL="https://dist.apache.org/repos/dist/release/arrow"
DOWNLOAD_RC_BASE_URL="https://github.com/apache/arrow-java/releases/download/v${VERSION}-rc${RC}"
ARCHIVE_BASE_NAME="apache-arrow-java-${VERSION}"

: "${VERIFY_DEFAULT:=1}"
: "${VERIFY_DOWNLOAD:=${VERIFY_DEFAULT}}"
: "${VERIFY_SIGN:=${VERIFY_DEFAULT}}"
: "${VERIFY_SOURCE:=${VERIFY_DEFAULT}}"
: "${VERIFY_BINARY:=${VERIFY_DEFAULT}}"

VERIFY_SUCCESS=no

setup_tmpdir() {
  cleanup() {
    if [ "${VERIFY_SUCCESS}" = "yes" ]; then
      rm -rf "${VERIFY_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${VERIFY_TMPDIR} for details."
    fi
  }

  if [ -z "${VERIFY_TMPDIR:-}" ]; then
    VERIFY_TMPDIR="$(mktemp -d -t "$1.XXXXX")"
    trap cleanup EXIT
  else
    mkdir -p "${VERIFY_TMPDIR}"
  fi
}

download() {
  curl \
    --fail \
    --location \
    --remote-name \
    --show-error \
    --silent \
    "$1"
}

download_rc_file() {
  if [ "${VERIFY_DOWNLOAD}" -gt 0 ]; then
    download "${DOWNLOAD_RC_BASE_URL}/$1"
  else
    cp "${TOP_SOURCE_DIR}/$1" "$1"
  fi
}

import_gpg_keys() {
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download "${ARROW_DIST_BASE_URL}/KEYS"
    gpg --import KEYS
  fi
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz"
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.asc"
    gpg --verify "${ARCHIVE_BASE_NAME}.tar.gz.asc" "${ARCHIVE_BASE_NAME}.tar.gz"
  fi
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.sha256"
  ${sha256_verify} "${ARCHIVE_BASE_NAME}.tar.gz.sha256"
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.sha512"
  ${sha512_verify} "${ARCHIVE_BASE_NAME}.tar.gz.sha512"
}

ensure_source_directory() {
  tar xf "${ARCHIVE_BASE_NAME}".tar.gz

  if [ -d "${TOP_SOURCE_DIR}/testing/data" ]; then
    cp -a "${TOP_SOURCE_DIR}/testing" "${ARCHIVE_BASE_NAME}/"
  else
    git clone \
      https://github.com/apache/arrow-testing.git \
      "${ARCHIVE_BASE_NAME}/testing"
  fi

  ARROW_TEST_DATA="$(pwd)/${ARCHIVE_BASE_NAME}/testing/data"
  export ARROW_TEST_DATA
}

test_source_distribution() {
  if [ "${VERIFY_SOURCE}" -le 0 ]; then
    return 0
  fi

  "${TOP_SOURCE_DIR}/ci/scripts/build.sh" "$(pwd)" build jni_dist
  "${TOP_SOURCE_DIR}/ci/scripts/test.sh" "$(pwd)" build jni_dist
  # TODO: JNI test
  # TODO: Integration test
}

test_binary_distribution() {
  if [ "${VERIFY_BINARY}" -le 0 ]; then
    return 0
  fi

  # TODO: jar test
}

setup_tmpdir "arrow-java-${VERSION}-${RC}"
echo "Working in sandbox ${VERIFY_TMPDIR}"
cd "${VERIFY_TMPDIR}"

import_gpg_keys
fetch_archive
ensure_source_directory
pushd "${ARCHIVE_BASE_NAME}"
test_source_distribution
test_binary_distribution
popd

VERIFY_SUCCESS=yes
echo "RC looks good!"
