---
title: Create a release
sidebar_position: 3
---

This document mainly introduces
how the release manager releases a new version in accordance with the Apache requirements.

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

- `opendal_version`: the version of OpenDAL to be released, like `0.36.0`.
- `release_version`: the version of release candidate, like `0.36.0-rc.1`.
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
[DISCUSS] Release Apache OpenDAL(incubating) ${release_version}
```

Content:

```
Hello, Apache OpenDAL(incubating) Community,

This is a call for a discussion to release Apache OpenDAL(incubating) version ${opendal_version}.

The change lists about this release:

https://github.com/apache/incubator-opendal/compare/v${opendal_last_version}...main

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

> Blockers are the tasks that must be completed before the release.

### Build Release

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
- [ ] Start VOTE at incubator community

### Official Release

- [ ] Push the release git tag
- [ ] Publish artifacts to SVN RELEASE branch
- [ ] Change OpenDAL Website download link
- [ ] Release Maven artifacts
- [ ] Send the announcement

For details of each step, please refer to: https://opendal.apache.org/community/committers/release
```

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

Running `python3 ./scripts/dependencies.py generate` to update the dependencies list of every package.

### Push release candidate tag

After bump version PR gets merged, we can create a GitHub release for the release candidate:

- Create a tag at `main` branch on the `Bump Version` / `Patch up version` commit: `git tag -s "v0.36.0-rc.1"`, please correctly check out the corresponding commit instead of directly tagging on the main branch.
- Push tags to GitHub: `git push --tags`.


:::note

Pushing a Git tag to GitHub repo will trigger a GitHub Actions workflow that creates a staging Maven release on https://repository.apache.org which can be verified on voting.

:::

## ASF Side

If any step in the ASF Release process fails and requires code changes,
we will abandon that version and prepare for the next one.
Our release page will only display ASF releases instead of GitHub Releases.

Additionally, we should also drop the staging Maven artifacts on https://repository.apache.org.

### Create an ASF Release

After GitHub Release has been created, we can start to create ASF Release.

- Checkout to released tag. (e.g. `git checkout v0.36.0-rc.1`, tag is created in the previous step)
- Use the release script to create a new release: `OPENDAL_VERSION=<opendal_version> OPENDAL_VERSION_RC=<rc_version> ./scripts/release.sh`(e.g. `OPENDAL_VERSION=0.36.0 OPENDAL_VERSION_RC=rc.1 ./scripts/release.sh`)
    - This script will do the following things:
        - Create a new branch named by `release-${release_version}` from the tag
        - Generate the release candidate artifacts under `dist`, including:
            - `apache-opendal-incubating-${release_version}-src.tar.gz`
            - `apache-opendal-incubating-${release_version}-src.tar.gz.asc`
            - `apache-opendal-incubating-${release_version}-src.tar.gz.sha512`
        - Check the header of the source code. This step needs docker to run.
- Push the newly created branch to GitHub

This script will create a new release under `dist`.

For example:

```shell
> tree dist
dist
├── apache-opendal-incubating-0.36.0-src.tar.gz
├── apache-opendal-incubating-0.36.0-src.tar.gz.asc
└── apache-opendal-incubating-0.36.0-src.tar.gz.sha512
```

### Upload artifacts to the SVN dist repo

:::info

SVN is required for this step.

:::

The svn repository of the dev branch is: <https://dist.apache.org/repos/dist/dev/incubator/opendal>

First, checkout OpenDAL to local directory:

```shell
# As this step will copy all the versions, it will take some time. If the network is broken, please use svn cleanup to delete the lock before re-execute it.
svn co https://dist.apache.org/repos/dist/dev/incubator/opendal opendal-dist-dev
```

Then, upload the artifacts:

> The `${release_version}` here should be like `0.36.0-rc.1`

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

Visit <https://dist.apache.org/repos/dist/dev/incubator/opendal/> to make sure the artifacts are uploaded correctly.

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

As an incubating project, OpenDAL requires votes from both the OpenDAL Community and Incubator Community.

### OpenDAL Community Vote

OpenDAL Community Vote should send email to: <dev@opendal.apache.org>:

Title:

```
[VOTE] Release Apache OpenDAL(incubating) ${release_version} - OpenDAL Vote Round 1
```

Content:

```
Hello, Apache OpenDAL(incubating) Community,

This is a call for a vote to release Apache OpenDAL(incubating) version ${opendal_version}.

The tag to be voted on is ${opendal_version}.

The release candidate:

https://dist.apache.org/repos/dist/dev/incubator/opendal/${release_version}/

Keys to verify the release candidate:

https://downloads.apache.org/incubator/opendal/KEYS

Git tag for the release:

https://github.com/apache/incubator-opendal/releases/tag/${release_version}

Maven staging repo:

https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/

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

More detailed checklist please refer to:
https://github.com/apache/incubator-opendal/tree/main/scripts

To compile from source, please refer to:
https://github.com/apache/incubator-opendal/blob/main/CONTRIBUTING.md

Here is a Python script in release to help you verify the release candidate:

./scripts/verify.py

Thanks

${name}
```

Example: <https://lists.apache.org/thread/c211gqq2yl15jbxqk4rcnq1bdqltjm5l>

After at least 3 `+1` binding vote ([from OpenDAL Podling PMC member](https://opendal.apache.org/community/#committers)) and no veto, claim the vote result:

Title:

```
[RESULT][VOTE] Release Apache OpenDAL(incubating) ${release_version} - OpenDAL Vote Round 1
```

Content:

```
Hello, Apache OpenDAL(incubating) Community,

The vote to release Apache OpenDAL(Incubating) ${release_version} has passed.

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

Example: <https://lists.apache.org/thread/xk5myl10mztcfotn59oo59s4ckvojds6>

### Incubator Community Vote

Incubator Community Vote should send email to: <general@incubator.apache.org>:

Title:

```
[VOTE] Release Apache OpenDAL(incubating) ${release_version} - Incubator Vote Round 1
```

Content:

```
Hello Incubator PMC,

The Apache OpenDAL community has voted and approved the release of Apache
OpenDAL(incubating) ${release_version}. We now kindly request the IPMC members
review and vote for this release.

OpenDAL is a data access layer that allows users to easily and efficiently
retrieve data from various storage services in a unified way.

OpenDAL community vote thread:

${community_vote_thread_url}

Vote result thread:

${community_vote_result_thread_url}

The release candidate:

https://dist.apache.org/repos/dist/dev/incubator/opendal/${release_version}/

This release has been signed with a PGP available here:

https://downloads.apache.org/incubator/opendal/KEYS

Git tag for the release:

https://github.com/apache/incubator-opendal/releases/tag/${release_version}

Maven staging repo:

https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/

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

More detailed checklist please refer to:
https://github.com/apache/incubator-opendal/tree/main/scripts

To compile from source, please refer to:
https://github.com/apache/incubator-opendal/blob/main/CONTRIBUTING.md

Here is python script in release to help you verify the release candidate:

./scripts/verify.py

Thanks

${name}
```

Example: <https://lists.apache.org/thread/sjdzs89p2x4tlb813ow7lhdhdfcvhysx>

After at least 72 hours with at least 3 +1 binding vote (from Incubator PMC member) and no veto, claim the vote result:

Title:

```
[RESULT][VOTE] Release Apache OpenDAL(incubating) ${release_version} - Incubator Vote Round 1
```

Content:

```
Hi Incubator PMC,

The vote to release Apache OpenDAL(incubating) ${release_version} has passed with
4 +1 binding and 3 +1 non-binding votes, no +0 or -1 votes.

Binding votes：

- xxx
- yyy
- zzz

Non-Binding votes:

- aaa

Vote thread: ${incubator_vote_thread_url}

Thanks for reviewing and voting for our release candidate.

We will proceed with publishing the approved artifacts and sending out the announcement soon.
```

Example: <https://lists.apache.org/thread/h3x9pq1djpg76q3ojpqmdr3d0o03fld1>

## Official Release

### Push the release git tag

```shell
# Checkout the tags that passed VOTE
git checkout ${release_version}
# Tag with the opendal version
git tag -s ${opendal_version}
# Push tags to github to trigger releases
git push origin ${opendal_version}
```

### Publish artifacts to SVN RELEASE branch

```shell
svn mv https://dist.apache.org/repos/dist/dev/incubator/opendal/${release_version} https://dist.apache.org/repos/dist/release/incubator/opendal/${opendal_version} -m "Release ${opendal_version}"
```

### Change OpenDAL Website download link

Change the [download](https://github.com/apache/incubator-opendal/blob/main/website/src/pages/download.md) link in the website to the new release version.

Update the latest release link and add the new release link to the version list.

Take [Add 0.39.0 release link to download.md](https://github.com/apache/incubator-opendal/pull/2882) as an example.

### Release Maven artifacts

1. Open https://repository.apache.org/#stagingRepositories.
2. Find the artifact `orgapacheopendal-${maven_artifact_number}`, click the "Release" button.

It will take some time to sync the Maven artifacts to the Maven Central.

:::caution

If the vote failed, click "Drop" to drop the staging Maven artifacts.

:::

### Create a GitHub Release

- Click [here](https://github.com/apache/incubator-opendal/releases/new) to create a new release.
- Pick the git tag of this release version from the dropdown menu.
- Make sure the branch target is `main`.
- Generate the release note by clicking the `Generate release notes` button.
- Add the release note from every component's `upgrade.md` if there are breaking changes before the content generated by GitHub. Check them carefully.
- Publish the release.

### Send the announcement

Send the release announcement to `dev@opendal.apache.org` and CC `announce@apache.org`.

Instead of adding breaking changes, let's include the new features as "notable changes" in the announcement.

Title:

```
[ANNOUNCE] Release Apache OpenDAL(incubating) ${opendal_version}
```

Content:

```
Hi all,

The Apache OpenDAL(incubating) community is pleased to announce
that Apache OpenDAL(incubating) ${opendal_version} has been released!

OpenDAL is a data access layer that allows users to easily and efficiently
retrieve data from various storage services in a unified way.

The notable changes since ${opendal_version} include:
1. xxxxx
2. yyyyyy
3. zzzzzz

Please refer to the change log for the complete list of changes:
https://github.com/apache/incubator-opendal/releases/tag/v${opendal_version}

Apache OpenDAL website: https://opendal.apache.org/

Download Links: https://opendal.apache.org/download

OpenDAL Resources:
- Issue: https://github.com/apache/incubator-opendal/issues
- Mailing list: dev@opendal.apache.org

Thanks
On behalf of Apache OpenDAL community

---
Apache OpenDAL (incubating) is an effort undergoing incubation at the Apache
Software Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects.

While incubation status is not necessarily a reflection of the completeness
or stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.
```

Example: <https://lists.apache.org/thread/oy77n55brvk72tnlb2bjzfs9nz3cfd0s>

## Post release

After the official release out, you may perform a few post actions.

### Remove the old releases

Remove the old releases if any. You only need the latest release there, and older releases are available through the Apache archive.

To clean up old releases, run:

```shell
# 1. Get the list of releases
svn ls https://dist.apache.org/repos/dist/release/incubator/opendal
# 2. Delete each release (except for the last one)
svn del -m "Archiving OpenDAL release X.Y.Z" https://dist.apache.org/repos/dist/release/incubator/opendal/X.Y.Z
```
