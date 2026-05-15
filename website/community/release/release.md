---
title: Create a release
sidebar_position: 3
---

This document mainly introduces how the release manager
releases a new version of Apache OpenDAL™ in accordance with the Apache requirements.

## Introduction

`Source Release` is the key point which Apache values, and is also necessary for an ASF release.

Please remember that publishing software has legal consequences.

This guide complements the foundation-wide policies and guides:

- [Release Policy](https://www.apache.org/legal/release-policy.html)
- [Release Distribution Policy](https://infra.apache.org/release-distribution)
- [Release Creation Process](https://infra.apache.org/release-publishing.html)

## Some Terminology of release

In the context of our release, we use several terms to describe different stages of the release process.

Here's an explanation of these terms:

- `opendal_version`: the version of OpenDAL to be released, like `0.46.0`.
- `release_version`: the version of release candidate, like `0.46.0-rc.1`.
- `rc_version`: the minor version for voting round, like `rc.1`.
- `maven_artifact_number`: the number for Maven staging artifacts, like `1010`. The number can be found by searching "opendal" on https://repository.apache.org/#stagingRepositories. And the Maven staging artifacts will be created automatically when we push a git tag to GitHub for now.

## Preparation

:::caution

This section is the requirements for individuals who are new to the role of release manager.

:::

Refer to [Setup GPG Key](reference/setup_gpg.md) to make sure the GPG key has been set up.

## Start discussion about the next release

Start a discussion at [OpenDAL Discussion General](https://github.com/apache/opendal/discussions/categories/general):

Title:

```
[DISCUSS] Release Apache OpenDAL ${release_version}
```

Content:

```
Hello, Apache OpenDAL Community,

This is a call for a discussion to release Apache OpenDAL version ${opendal_version}.

The change lists about this release:

https://github.com/apache/opendal/compare/v${opendal_last_version}...main

Please leave your comments here about this release plan. We will bump the version in the repo and start the release process after the discussion.

Thanks

${name}
```

## Start a tracking issue about the next release

Start a [tracking issue on GitHub](https://github.com/apache/opendal/issues/new?template=3-new-release.md) for the upcoming release to track all tasks that need to be completed.

## Release List

Update the version list in the `dev/src/release/package.rs` file.

This file is the source of truth for the split source release layout. Each entry in this list produces an independent source archive named `apache-opendal-{package}-{version}-src.tar.gz`.

For example:

- If there is any breaking change, please bump the `minor` version instead of the `patch` version.
- If this package is not ready for release, please skip it from the release list.
- Packages that have moved to separate repositories must not be included here.

## GitHub Side

### Bump version in project

Run `just update-version` to bump the version in the project.

### Update docs

- Update `CHANGELOG.md`, refer to [Generate Release Note](reference/generate_release_note.md) for more information.
- Update `core/core/src/docs/upgrade.md` if there are breaking changes in `core`
- Make sure every released bindings' `upgrade.md` has been updated.
    - java: `bindings/java/upgrade.md`
    - node.js: `bindings/nodejs/upgrade.md`
    - python: `bindings/python/upgrade.md`

### Generate dependencies list

Download and setup `cargo-deny`. You can refer to [cargo-deny](https://embarkstudios.github.io/cargo-deny/cli/index.html).

Running `python3 ./scripts/dependencies.py generate` to update the dependency list of every package.

### Push release candidate tag

After bump version PR gets merged, push the release candidate tag:

- Create a tag at `main` branch on the `Bump Version` / `Patch up version` commit: `git tag -s "v0.46.0-rc.1"`, please correctly check out the corresponding commit instead of directly tagging on the main branch.
- Push tags to GitHub: `git push --tags`.


:::note

Pushing an RC tag to GitHub will trigger the tag-based release workflows. In the current flow, this includes the Java staging artifacts on https://repository.apache.org and dry-run checks for other released packages.

:::

### Check the GitHub action status

After pushing the tag, check the GitHub action status to make sure the RC workflows finished successfully.

- Rust packages: [Release Rust Packages](https://github.com/apache/opendal/actions/workflows/release_rust.yml)
- Python: [Release Python Binding](https://github.com/apache/opendal/actions/workflows/release_python.yml)
- Java: [Bindings Java CI](https://github.com/apache/opendal/actions/workflows/ci_bindings_java.yml) and [Bindings Java Release](https://github.com/apache/opendal/actions/workflows/release_java.yml)
- Node.js: [Bindings Node.js CI](https://github.com/apache/opendal/actions/workflows/ci_bindings_nodejs.yml) and [Release NodeJS Binding](https://github.com/apache/opendal/actions/workflows/release_nodejs.yml)
- Docs: [Docs](https://github.com/apache/opendal/actions/workflows/docs.yml)

In the most cases, it would be great to rerun the failed workflow directly when you find some failures. But if a new code patch is needed to fix the failure, you should create a new release candidate tag, increase the rc number and push it to GitHub.

### Check Rust crates.io readiness

Before the official release tag is pushed, check the Rust package publish plan:

```shell
python3 .github/scripts/release_rust/plan.py
```

The plan must include every Rust package that is referenced by released crates.
For example, `opendal-testkit` is referenced by the `opendal` crate's `tests`
feature, so it must be publishable and appear before `opendal` in the plan.

The Rust release workflow uses `.github/scripts/release_rust/publish.py` instead
of calling `cargo publish` directly. The helper temporarily removes repo-local
`dev-dependencies` while packaging crates, because crates.io resolves
`dev-dependencies` even when `cargo publish --no-verify` is used. Without this
step, same-version dev dependencies and dev-only cycles can block the release
even though they are not needed by downstream users.

:::caution

crates.io trusted publishing tokens cannot create new crates. If this release
introduces a new crate name, make sure a crates.io token that is allowed to
create crates is available as `CARGO_REGISTRY_TOKEN`, or publish the new crate
manually before relying on trusted publishing.

:::

## ASF Side

If any step in the ASF Release process fails and requires code changes,
we will abandon that version and prepare for the next one.
Our release page will only display ASF releases instead of GitHub Releases.

Additionally, we should also drop the staging Maven artifacts on https://repository.apache.org.

### Create an ASF Release

After the RC tag has been pushed and the required workflows are green, create the ASF source release artifacts.

- Checkout to released tag. (e.g. `git checkout v0.46.0-rc.1`, tag is created in the previous step)
- Use the release script to create a new release: `just release`
  - This script will generate the release candidate artifacts under `dist` for every package listed in `dev/src/release/package.rs`, including:
    - `apache-opendal-{package}-{version}-src.tar.gz`
    - `apache-opendal-{package}-{version}-src.tar.gz.asc`
    - `apache-opendal-{package}-{version}-src.tar.gz.sha512`
  - Artifact names use each package's own version. The RC version is only used for the SVN directory name, such as `0.55.0-rc.1/`.
  - Each archive contains `LICENSE`, `NOTICE`, the package directory itself, and any repo-local dependencies needed to build that package from source.
  - This repository no longer produces a monolithic `apache-opendal-${opendal_version}-src.tar.gz` artifact or any `apache-opendal-bin-*` artifacts.

This script will create a new release under `dist`.

For example:

```shell
dist
├── apache-opendal-bindings-c-${c_version}-src.tar.gz
├── apache-opendal-bindings-c-${c_version}-src.tar.gz.asc
├── apache-opendal-bindings-c-${c_version}-src.tar.gz.sha512
├── apache-opendal-bindings-cpp-${cpp_version}-src.tar.gz
├── apache-opendal-bindings-cpp-${cpp_version}-src.tar.gz.asc
├── apache-opendal-bindings-cpp-${cpp_version}-src.tar.gz.sha512
├── apache-opendal-bindings-java-${java_version}-src.tar.gz
├── apache-opendal-bindings-java-${java_version}-src.tar.gz.asc
├── apache-opendal-bindings-java-${java_version}-src.tar.gz.sha512
├── apache-opendal-bindings-nodejs-${nodejs_version}-src.tar.gz
├── apache-opendal-bindings-nodejs-${nodejs_version}-src.tar.gz.asc
├── apache-opendal-bindings-nodejs-${nodejs_version}-src.tar.gz.sha512
├── apache-opendal-bindings-python-${python_version}-src.tar.gz
├── apache-opendal-bindings-python-${python_version}-src.tar.gz.asc
├── apache-opendal-bindings-python-${python_version}-src.tar.gz.sha512
├── apache-opendal-core-${core_version}-src.tar.gz
├── apache-opendal-core-${core_version}-src.tar.gz.asc
├── apache-opendal-core-${core_version}-src.tar.gz.sha512
├── apache-opendal-integrations-dav-server-${dav_server_version}-src.tar.gz
├── apache-opendal-integrations-dav-server-${dav_server_version}-src.tar.gz.asc
├── apache-opendal-integrations-dav-server-${dav_server_version}-src.tar.gz.sha512
├── apache-opendal-integrations-object_store-${object_store_version}-src.tar.gz
├── apache-opendal-integrations-object_store-${object_store_version}-src.tar.gz.asc
├── apache-opendal-integrations-object_store-${object_store_version}-src.tar.gz.sha512
├── apache-opendal-integrations-parquet-${parquet_version}-src.tar.gz
├── apache-opendal-integrations-parquet-${parquet_version}-src.tar.gz.asc
├── apache-opendal-integrations-parquet-${parquet_version}-src.tar.gz.sha512
├── apache-opendal-integrations-unftp-sbe-${unftp_sbe_version}-src.tar.gz
├── apache-opendal-integrations-unftp-sbe-${unftp_sbe_version}-src.tar.gz.asc
└── apache-opendal-integrations-unftp-sbe-${unftp_sbe_version}-src.tar.gz.sha512
```

### Upload artifacts to the SVN dist repo

:::info

SVN is required for this step.

:::

The svn repository of the dev branch is: <https://dist.apache.org/repos/dist/dev/opendal>

First, checkout OpenDAL to local directory:

```shell
# As this step will copy all the versions, it will take some time. If the network is broken, please use svn cleanup to delete the lock before re-execute it.
svn co https://dist.apache.org/repos/dist/dev/opendal opendal-dist-dev
```

Then, upload the artifacts:

> The `${release_version}` here should be like `0.46.0-rc.1`

```shell
cd opendal-dist-dev
# create a directory named by version
mkdir ${release_version}
# copy source code and signature package to the versioned directory
cp ${repo_dir}/dist/* ${release_version}/
# check svn status
svn status
# add to svn
svn add ${release_version}
# check svn status
svn status
# commit to SVN remote server
svn commit -m "Prepare for ${release_version}"
```

Visit <https://dist.apache.org/repos/dist/dev/opendal/> to make sure the artifacts are uploaded correctly.

### Close the Nexus staging repo

To verify the Maven staging artifacts in the next step, close the Nexus staging repo as below.

1. Open https://repository.apache.org/#stagingRepositories with your Apache ID login.
2. Find the artifact `orgapacheopendal-${maven_artifact_number}`, click the "Close" button.

The `close` operation means that the artifacts are ready for voting.

:::caution

If the vote failed, click "Drop" to drop the staging Maven artifacts.

:::

### Rescue

If you accidentally published wrong or unexpected artifacts, like wrong signature files, wrong sha256 files,
please cancel the release for the current `release_version`,
_increase th RC counting_ and re-initiate a release with the new `release_version`.
And remember to delete the wrong artifacts from the SVN dist repo.
Additionally, you should also drop the staging Maven artifacts on https://repository.apache.org.

## Voting

OpenDAL requires votes from both the OpenDAL Community.

Start a VOTE at [OpenDAL Discussion General](https://github.com/apache/opendal/discussions/categories/general):

Title:

```
[VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1
```

Content:

```
Hello, Apache OpenDAL Community,

This is a call for a vote to release Apache OpenDAL version ${opendal_version}.

The tag to be voted on is v${release_version}.

The release candidate source packages:

https://dist.apache.org/repos/dist/dev/opendal/${release_version}/

Keys to verify the release candidate:

https://downloads.apache.org/opendal/KEYS

Git tag for the release:

https://github.com/apache/opendal/releases/tag/v${release_version}

Maven staging repo:

https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/

Pypi testing repo:

https://test.pypi.org/project/opendal/

Website staged:

https://opendal-v${release_version | replace('.', '-')}.staged.apache.org/

Please download, verify, and test.

The VOTE will be open for at least 72 hours and until the necessary
number of votes are reached.

- [ ] +1 approve
- [ ] +0 no opinion
- [ ] -1 disapprove with the reason

To learn more about apache opendal, please see https://opendal.apache.org/

Checklist for reference:

- [ ] Download links are valid.
- [ ] Checksums and signatures.
- [ ] LICENSE/NOTICE files exist
- [ ] No unexpected binary files
- [ ] All source files have ASF headers
- [ ] Can compile from source

Use our verify.py to assist in the verify process:

    svn co https://dist.apache.org/repos/dist/dev/opendal/${release_version}/ opendal-dev
    cd opendal-dev
    curl -sSL https://github.com/apache/opendal/raw/v${release_version}/scripts/verify.py -o verify.py
    python verify.py

Thanks

${name}
```

Example: <https://github.com/apache/opendal/discussions/5211>

The vote should be open for **at least 72 hours** except the following cases:

1. Security issues
2. The wild user affected bug fixes
3. Any other emergency cases

The Release manager should claim the emergency cases in the vote email if he wants to vote it rapidly.

> Tips: The 72 hours is the minimum time for voting, so we can ensure that community members from various time zones can participate in the verification process.

After at least 3 `+1` binding vote ([from OpenDAL PMC member](https://people.apache.org/phonebook.html?project=opendal)) and more +1 bindings than -1 bindings, claim the vote result:

Title:

```
[RESULT][VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1
```

Content:

```
Hello, Apache OpenDAL Community,

The vote to release Apache OpenDAL ${release_version} has passed.

The vote PASSED with 3 +1 binding and 1 +1 non-binding votes, no +0 or -1 votes:

Binding votes:

- xxx
- yyy
- zzz

Non-Binding votes:

- aaa

Vote thread: ${vote_thread_url}

Thanks

${name}
```

It's better to use the real name or the public name which is displayed on the voters' profile page,
or Apache ID of the voter, to show who voted in the vote result email,
and avoid using nicknames, it will make the vote result hard for others to track the voter.
We should make sure the binding votes are from the people who have the right to vote the binding one.

Example: <https://lists.apache.org/thread/xk5myl10mztcfotn59oo59s4ckvojds6>

## Official Release

### Push the release git tag

```shell
# Checkout the tags that passed VOTE
git checkout ${release_version}
# Tag with the opendal version
git tag -s ${opendal_version}
# Push tags to GitHub to trigger releases
git push origin ${opendal_version}
```

### Publish artifacts to SVN RELEASE branch

```shell
svn mv https://dist.apache.org/repos/dist/dev/opendal/${release_version} https://dist.apache.org/repos/dist/release/opendal/${opendal_version} -m "Release ${opendal_version}"
```

### Release Maven artifacts

1. Open https://repository.apache.org/#stagingRepositories.
2. Find the artifact `orgapacheopendal-${maven_artifact_number}`, click the "Release" button.

It will take some time to sync the Maven artifacts to the Maven Central.

:::caution

If the vote failed, click "Drop" to drop the staging Maven artifacts.

:::

### Check the language binding artifacts

We need to check the language binding artifacts in the language package repo to make sure they are released successfully.

- Rust: <https://crates.io/crates/opendal>
- Python: <https://pypi.org/project/opendal/>
- Java: <https://repository.apache.org/#nexus-search;quick~opendal>
- Node.js: <https://www.npmjs.com/package/opendal>

For Rust crates, check both the top-level `opendal` crate and split crates that
were added or changed in the release, such as `opendal-service-*`,
`opendal-layer-*`, and integration crates.

For Java binding, if we cannot find the latest version of artifacts in the repo,
we need to check the `orgapacheopendal-${maven_artifact_number}` artifact status in staging repo.

For non-Java bindings, if we cannot find the latest version of artifacts in the repo,
we need to check the GitHub action status.

### Create a GitHub Release

- Click [here](https://github.com/apache/opendal/releases/new) to create a new release.
- Pick the git tag of this release version from the dropdown menu.
- Make sure the branch target is `main`.
- Generate the release note by clicking the `Generate release notes` button.
- Add the release note from every component's `upgrade.md` if there are breaking changes before the content generated by GitHub. Check them carefully.
- Publish the release.

### Send the announcement

Start an announcement to [OpenDAL Discussion Announcements](https://github.com/apache/opendal/discussions/categories/announcements) and send the same content to `announce@apache.org`.

> Tips: Please follow the [Committer Email](https://infra.apache.org/committer-email.html) guide to make sure you have already set up the email SMTP. Otherwise, your email cannot be sent to the announcement mailing list.

Instead of adding breaking changes, let's include the new features as "notable changes" in the announcement.

Title:

```
[ANNOUNCE] Release Apache OpenDAL ${opendal_version}
```

Content:

```
Hi all,

The Apache OpenDAL community is pleased to announce
that Apache OpenDAL ${opendal_version} has been released!

OpenDAL is a data access layer that allows users to easily and efficiently
retrieve data from various storage services in a unified way.

The notable changes since ${opendal_version} include:

1. xxxxx
2. yyyyyy
3. zzzzzz

Please refer to the change log for the complete list of changes:
https://github.com/apache/opendal/releases/tag/v${opendal_version}

Apache OpenDAL website: https://opendal.apache.org/

Download Links: https://opendal.apache.org/download

OpenDAL Resources:
- Issue: https://github.com/apache/opendal/issues
- Mailing list: dev@opendal.apache.org

Thanks
On behalf of Apache OpenDAL community
```

Example: <https://lists.apache.org/thread/oy77n55brvk72tnlb2bjzfs9nz3cfd0s>

## Post release

After the official release out, you may perform a few post-actions.

### Remove the old releases

Remove the old releases if any. You only need the latest release there, and older releases are available through the Apache archive.

To clean up old releases, run:

```shell
# 1. Get the list of releases
svn ls https://dist.apache.org/repos/dist/release/opendal
# 2. Delete each release (except for the last one)
svn del -m "Archiving OpenDAL release X.Y.Z" https://dist.apache.org/repos/dist/release/opendal/X.Y.Z
```
