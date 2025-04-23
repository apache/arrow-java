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

### Prepare release environment

This step is needed only when you act as a release manager first time.

We use the following variables in multiple steps:

* `GH_TOKEN`: GitHub personal access token to automate GitHub related
  operations
* `GPG_KEY_ID`: PGP key ID that is used for signing official artifacts
  by GnuPG

We use `dev/release/.env` to share these variables in multiple
steps. You can use `dev/release/.env.example` as a template:

```console
$ cp dev/release/.env{.example,}
$ chmod go-r dev/release/.env
$ editor dev/release/.env
```

See
https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
how to prepare GitHub personal access token for `GH_TOKEN`.

Note that you also need to install `gh` command because our scripts
use `gh` command to use GitHub API. See
https://github.com/cli/cli#installation how to install `gh`
command.

If you don't have a PGP key for `GPG_KEY_ID`, see
https://infra.apache.org/release-signing.html#genegrate how to
generate your PGP key.

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

### Bump version for new release

Run `dev/release/bump_version.sh` on a working copy of your fork not
`git@github.com:apache/arrow-java`:

```console
$ git clone git@github.com:${YOUR_GITHUB_ACCOUNT}/arrow-java.git arrow-java.${YOUR_GITHUB_ACCOUNT}
$ cd arrow-java.${YOUR_GITHUB_ACCOUNT}
$ dev/release/bump_version.sh ${NEW_VERSION}
```

Here is an example to bump version to 19.0.0:

```
$ dev/release/bump_version.sh 19.0.0
```

It creates a feature branch and adds a commit that bumps version. This
opens a pull request from the feature branch.

We need to merge the pull request before we cut a RC. If we try
cutting a RC without merging the pull request, the script to cut a RC
is failed.

### Prepare RC and vote

You can use `dev/release/release_rc.sh`.

Requirements to run `release_rc.sh`:

  * You must be an Apache Arrow committer or PMC member
  * You must prepare your PGP key for signing
  * You must configure Maven

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
$ dev/release/release_rc.sh ${RC}
(Send a vote email to dev@arrow.apache.org.
 You can use a draft shown by release_rc.sh for the email.)
```

Here is an example to release RC1:

```console
$ dev/release/release_rc.sh 1
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
$ dev/release/release.sh ${VERSION} ${RC}
```

Here is an example to release 19.0.0 RC1:

```console
$ dev/release/release.sh 19.0.0 1
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
$ dev/release/bump_version.sh ${NEW_VERSION}-SNAPSHOT
```

Here is an example to bump version to 19.0.1-SNAPSHOT:

```
$ dev/release/bump_version.sh 19.0.0-SNAPSHOT
```

It creates a feature branch and adds a commit that bumps version. This
opens a pull request from the feature branch by `gh pr create`.

### Close the GitHub milestone

Close the milestone here, then open a new milestone for the next release:

https://github.com/apache/arrow-java/milestones

The milestone should be named after the version (e.g. "18.4.0").

### Publish the release blog post

Open a pull request on
[apache/arrow-site](https://github.com/apache/arrow-site) announcing the new
release and summarizing the changes.  Do not include the full changelog, just
important entries (breaking changes, new features, major bug fixes) and a link
to the full changelog.  See an [example
PR](https://github.com/apache/arrow-site/pull/594).

### Announce the new release on the mailing list

Send an email to "announce@apache.org" from your Apache email, CC'ing
dev@arrow.apache.org/user@arrow.apache.org.  See an [example
post](https://lists.apache.org/thread/bxpt0r8kw0ltgywnylqdroskkt6966z4).

```
To: announce@apache.org
CC: dev@arrow.apache.org, user@arrow.apache.org
Subject: [ANNOUNCE] Apache Arrow Java 18.2.0 released

The Apache Arrow community is pleased to announce the Arrow Java 18.2.0 release.

The release is available now from our website:
     https://arrow.apache.org/install/
and
     https://www.apache.org/dyn/closer.cgi/arrow/apache-arrow-java-18.2.0/

Read about what's new in the release at:
     https://arrow.apache.org/blog/2025/02/19/arrow-java-18.2.0/

Read the full changelog:
     https://github.com/apache/arrow-java/commits/v18.2.0

What is Apache Arrow?
-------------------------------

Apache Arrow is a universal columnar format and multi-language toolbox
for fast data interchange and in-memory analytics. It houses a set of
canonical in-memory representations of flat and hierarchical data
along with multiple language-bindings for structure manipulation. It
also provides low-overhead streaming and batch messaging, zero-copy
interprocess communication (IPC), and vectorized in-memory analytics
libraries.

Please report any feedback to the mailing lists:
    https://lists.apache.org/list.html?dev@arrow.apache.org

Regards,
The Apache Arrow community.
```

### Announce the new release on social media

Make a post on our [BlueSky](https://bsky.app/profile/arrow.apache.org) and
[LinkedIn](https://www.linkedin.com/company/apache-arrow/) accounts.  (Ask
your fellow PMC members for access if need be, or ask a PMC member to make the
post on your behalf.)  The post should link to the blog post.  See [example
BlueSky post](https://bsky.app/profile/arrow.apache.org/post/3lioi6ov5h22d)
and [example LinkedIn post](https://www.linkedin.com/posts/apache-arrow_apache-arrow-java-1820-release-activity-7298633716522758144-L71x).

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
