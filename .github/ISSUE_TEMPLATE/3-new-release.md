---
name: New Release
about: Use this template for start making a new release
title: "Tracking issues of OpenDAL ${opendal_version} Release"
---

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
