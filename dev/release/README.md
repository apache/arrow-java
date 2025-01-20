<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Release

## Overview

  1. Test the revision to be released
  2. Bump version for new release (detailed later)
  3. Prepare RC and vote (detailed later)
  4. Publish (detailed later)
  5. Bump version for new development (detailed later)

### Bump version for new release

Run `dev/release/bump_version.sh` on a working copy of your fork not
`git@github.com:apache/arrow-java`:

```console
$ git clone git@github.com:${YOUR_GITHUB_ACCOUNT}/arrow-java.git arrow-java.${YOUR_GITHUB_ACCOUNT}
$ cd arrow-java.${YOUR_GITHUB_ACCOUNT}
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/bump_version.sh ${NEW_VERSION}
```

Here is an example to bump version to 19.0.0:

```
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/bump_version.sh 19.0.0
```

It creates a feature branch and adds a commit that bumps version. This
opens a pull request from the feature branch by `gh pr create`. So you
need `gh` command and GitHub personal access token.

See also:
https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens

We need to merge the pull request before we cut a RC. If we try cut a
RC without merging the pull request, the script to cut a RC is failed.

### Prepare RC and vote

You can use `dev/release/release_rc.sh`.

Requirements to run `release_rc.sh`:

  * You must be an Apache Arrow committer or PMC member
  * You must prepare your PGP key for signing
  * You must configure Maven

If you don't have a PGP key,
https://infra.apache.org/release-signing.html#generate may be helpful.

Your PGP key must be registered to the followings:

  * https://dist.apache.org/repos/dist/dev/arrow/KEYS
  * https://dist.apache.org/repos/dist/release/arrow/KEYS

See the header comment of them how to add a PGP key.

Apache arrow committers can update them by Subversion client with
their ASF account. e.g.:

```console
$ svn co https://dist.apache.org/repos/dist/dev/arrow
$ cd arrow
$ head KEYS
(This shows how to update KEYS)
$ svn ci KEYS
```

Configure Maven to publish artifacts to Apache repositories. You will
need to setup a master password at `~/.m2/settings-security.xml` and
`~/.m2/settings.xml` as specified on [the Apache
guide](https://infra.apache.org/publishing-maven-artifacts.html). It
can be tested with the following command:

```bash
# You might need to export GPG_TTY=$(tty) to properly prompt for a passphrase
mvn clean install -Papache-release
```

Run `dev/release/release_rc.sh` on a working copy of
`git@github.com:apache/arrow-java` not your fork:

```console
$ git clone git@github.com:apache/arrow-java.git
$ cd arrow-java
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh ${RC}
(Send a vote email to dev@arrow.apache.org.
 You can use a draft shown by release_rc.sh for the email.)
```

Here is an example to release RC1:

```console
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh 1
```

The argument of `release_rc.sh` is the RC number. If RC1 has a
problem, we'll increment the RC number such as RC2, RC3 and so on.

### Publish

We need to do the followings to publish a new release:

  * Publish the source archive to apache.org
  * Publish the binary artifacts to repository.apache.org

Run `dev/release/release.sh` on a working copy of
`git@github.com:apache/arrow-java` not your fork to publish the source
archive to apache.org:

```console
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release.sh ${VERSION} ${RC}
```

Here is an example to release 19.0.0 RC1:

```console
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release.sh 19.0.0 1
```

Add the release to ASF's report database via [Apache Committee Report
Helper](https://reporter.apache.org/addrelease.html?arrow).

You need to do the followings to publish the binary artifacts to
repository.apache.org:

* Logon to the Apache repository:
  https://repository.apache.org/#stagingRepositories
* Select the Arrow staging repository you created for RC:
  `orgapachearrow-XXXX`
* Click the `release` button

### Bump version for new development

We should bump version in the main branch for new development after we
release a new version.

Run `dev/release/bump_version.sh` on a working copy of your fork not
`git@github.com:apache/arrow-java`:

```console
$ git clone git@github.com:${YOUR_GITHUB_ACCOUNT}/arrow-java.git arrow-java.${YOUR_GITHUB_ACCOUNT}
$ cd arrow-java.${YOUR_GITHUB_ACCOUNT}
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/bump_version.sh ${NEW_VERSION}-SNAPSHOT
```

Here is an example to bump version to 19.0.1-SNAPSHOT:

```
$ GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/bump_version.sh 19.0.0-SNAPSHOT
```

It creates a feature branch and adds a commit that bumps version. This
opens a pull request from the feature branch by `gh pr create`.

## Verify

We have a script to verify a RC.

You must install the following commands to use the script:

  * `curl`
  * `gpg`
  * `shasum` or `sha256sum`/`sha512sum`
  * `tar`

To verify a RC, run the following command line:

```console
$ dev/release/verify_rc.sh ${VERSION} ${RC}
```

Here is an example to verify the release 19.0.0 RC1:

```console
$ dev/release/verify_rc.sh 19.0.0 1
```

If the verification is successful, the message `RC looks good!` is shown.
