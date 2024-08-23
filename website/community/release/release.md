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

Start a discussion about the next release via sending email to: <dev@opendal.apache.org>:

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

Start a tracking issue on GitHub for the upcoming release to track all tasks that need to be completed.

Title:

```
Tracking issues of OpenDAL ${opendal_version} Release
```

Content:

```markdown
This issue is used to track tasks of the opendal ${opendal_version} release.

## Tasks

### Blockers

<!-- Blockers are the tasks that must be completed before the release. -->

### Build Release

#### Release List

<!-- Generate release list by `./scripts/version.py`, please adapt with the actual needs. -->

#### GitHub Side

- [ ] Bump version in project
  - [ ] rust
  - [ ] cpp
  - [ ] haskell
  - [ ] java
  - [ ] nodejs
- [ ] Update docs
- [ ] Generate dependencies list
- [ ] Push release candidate tag to GitHub

#### ASF Side

- [ ] Create an ASF Release
- [ ] Upload artifacts to the SVN dist repo
- [ ] Close the Nexus staging repo

### Voting

- [ ] Start VOTE at opendal community

### Official Release

- [ ] Push the release git tag
- [ ] Publish artifacts to SVN RELEASE branch
- [ ] Release Maven artifacts
- [ ] Send the announcement

For details of each step, please refer to: https://opendal.apache.org/community/release/
```

## Release List

Use `./scripts/version.py` to generate a release version list for review.

This list bumps `patch` version by default, please adapt with the actual needs.

For example:

- If breaking change happened, we need to bump `minor` version instead of `patch`.
- If this package is not ready for release, we can skip it.

## GitHub Side

### Bump version in project

Bump all components' version in the project to the new opendal version.
Please note that this version is the exact version of the release, not the release candidate version.

- rust core: bump version in `Cargo.toml`
- cpp binding: bump version in `bindings/cpp/CMakeLists.txt`
- haskell binding: bump version and update the `tag` field of `source-repository this` in `bindings/haskell/opendal.cabal`
- java binding: bump version in `bindings/java/pom.xml`
- node.js binding: bump version in `bindings/nodejs/package.json` and `bindings/nodejs/npm/*/package.json`

### Update docs

- Update `CHANGELOG.md`, refer to [Generate Release Note](reference/generate_release_note.md) for more information.
- Update `core/src/docs/upgrade.md` if there are breaking changes in `core`
- Make sure every released bindings' `upgrade.md` has been updated.
    - java: `bindings/java/upgrade.md`
    - node.js: `bindings/nodejs/upgrade.md`
    - python: `bindings/python/upgrade.md`

### Generate dependencies list

Download and setup `cargo-deny`. You can refer to [cargo-deny](https://embarkstudios.github.io/cargo-deny/cli/index.html). 

Running `python3 ./scripts/dependencies.py generate` to update the dependency list of every package.

### Push release candidate tag

After bump version PR gets merged, we can create a GitHub release for the release candidate:

- Create a tag at `main` branch on the `Bump Version` / `Patch up version` commit: `git tag -s "v0.46.0-rc.1"`, please correctly check out the corresponding commit instead of directly tagging on the main branch.
- Push tags to GitHub: `git push --tags`.


:::note

Pushing a Git tag to GitHub repo will trigger a GitHub Actions workflow that creates a staging Maven release on https://repository.apache.org which can be verified on voting.

:::

### Check the GitHub action status

After pushing the tag, we need to check the GitHub action status to make sure the release candidate is created successfully.

- Python: [Bindings Python CI](https://github.com/apache/opendal/actions/workflows/bindings_python.yml)
- Java: [Bindings Java CI](https://github.com/apache/opendal/actions/workflows/bindings_java.yml) and [Bindings Java Release](https://github.com/apache/opendal/actions/workflows/release_java.yml)
- Node.js: [Bindings Node.js CI](https://github.com/apache/opendal/actions/workflows/bindings_nodejs.yml)

In the most cases, it would be great to rerun the failed workflow directly when you find some failures. But if a new code patch is needed to fix the failure, you should create a new release candidate tag, increase the rc number and push it to GitHub.

## ASF Side

If any step in the ASF Release process fails and requires code changes,
we will abandon that version and prepare for the next one.
Our release page will only display ASF releases instead of GitHub Releases.

Additionally, we should also drop the staging Maven artifacts on https://repository.apache.org.

### Create an ASF Release

After GitHub Release has been created, we can start to create ASF Release.

- Checkout to released tag. (e.g. `git checkout v0.46.0-rc.1`, tag is created in the previous step)
- Use the release script to create a new release: `python ./scripts/release.py`
  - This script will generate the release candidate artifacts under `dist`, including:
    - `apache-opendal-{package}-{version}-src.tar.gz`
    - `apache-opendal-{package}-{version}-src.tar.gz.asc`
    - `apache-opendal-{package}-{version}-src.tar.gz.sha512`
- Push the newly created branch to GitHub

This script will create a new release under `dist`.

For example:

```shell
dist
├── apache-opendal-bindings-c-0.44.2-src.tar.gz
├── apache-opendal-bindings-c-0.44.2-src.tar.gz.asc
├── apache-opendal-bindings-c-0.44.2-src.tar.gz.sha512
...
├── apache-opendal-core-0.45.0-src.tar.gz
├── apache-opendal-core-0.45.0-src.tar.gz.asc
├── apache-opendal-core-0.45.0-src.tar.gz.sha512
├── apache-opendal-integrations-dav-server-0.0.0-src.tar.gz
├── apache-opendal-integrations-dav-server-0.0.0-src.tar.gz.asc
├── apache-opendal-integrations-dav-server-0.0.0-src.tar.gz.sha512
├── apache-opendal-integrations-object_store-0.42.0-src.tar.gz
├── apache-opendal-integrations-object_store-0.42.0-src.tar.gz.asc
└── apache-opendal-integrations-object_store-0.42.0-src.tar.gz.sha512

1 directory, 60 files
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

Vote should send email to: <dev@opendal.apache.org>:

Title:

```
[VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1
```

Content:

```
Hello, Apache OpenDAL Community,

This is a call for a vote to release Apache OpenDAL version ${opendal_version}.

The tag to be voted on is ${opendal_version}.

The release candidate:

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

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

To learn more about apache opendal, please see https://opendal.apache.org/

Checklist for reference:

[ ] Download links are valid.
[ ] Checksums and signatures.
[ ] LICENSE/NOTICE files exist
[ ] No unexpected binary files
[ ] All source files have ASF headers
[ ] Can compile from source

Use our verify.py to assist in the verify process:

svn co https://dist.apache.org/repos/dist/dev/opendal/${release_version}/ opendal-dev

cd opendal-dev

curl -sSL https://github.com/apache/opendal/raw/v${release_version}/scripts/verify.py -o verify.py

python verify.py

Thanks

${name}
```

Example: <https://lists.apache.org/thread/c211gqq2yl15jbxqk4rcnq1bdqltjm5l>

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

- Python: <https://pypi.org/project/opendal/>
- Java: <https://repository.apache.org/#nexus-search;quick~opendal>
- Node.js: <https://www.npmjs.com/package/opendal>

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

Send the release announcement to `dev@opendal.apache.org` and CC `announce@apache.org`.

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
