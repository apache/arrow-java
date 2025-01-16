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

set -euxo pipefail

if [ $# -eq 1 ]; then
  version="${1}"
else
  version="latest-release"
fi

url=""

if [ "${version}" = "latest-release" ]; then
  version=$(curl \
    https://raw.githubusercontent.com/apache/arrow-site/refs/heads/main/_data/versions.yml |
    grep '^  number:' |
    sed -E -e "s/^  number: '|'$//g")
elif [ "${version}" = "latest-rc" ]; then
  rc_archive_name=$(curl \
    https://dist.apache.org/repos/dist/dev/arrow/ |
    grep -E -o 'apache-arrow-[0-9]+\.[0-9]+\.[0-9]+\-rc[0-9]' |
    sort |
    uniq |
    tail -n1)
  rc_version="${rc_archive_name#apache-arrow-}"
  version="${rc_version%-rc*}"
  url="https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-${rc_version}/apache-arrow-${version}.tar.gz"
fi

if [ -z "${url}" ]; then
  url="https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-${version}/apache-arrow-${version}.tar.gz"
fi
curl --location --output "apache-arrow-${version}.tar.gz" "${url}"
tar xf "apache-arrow-${version}.tar.gz"
mv "apache-arrow-${version}" arrow
