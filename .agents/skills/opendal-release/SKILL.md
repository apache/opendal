---
name: opendal-release
description: Execute and verify Apache OpenDAL release-manager work, including RC tagging, required GitHub Actions checks, ASF SVN dist uploads, Nexus staging close/release, vote discussions, language package readiness, GitHub release, announcements, and release postmortems.
---

# OpenDAL Release

## Overview

Use this skill for Apache OpenDAL release-manager work. Treat releases as a state machine with externally visible legal and distribution effects, not as ordinary CI chores.

The primary repository runbook is `website/community/release/release.md`. The split source artifact source of truth is `dev/src/release/package.rs`. Read the relevant source in the current checkout when first needed. Reuse that context across release stages; refresh the affected sections when the checkout or source changes, or when an unresolved question requires it.

## Ground Rules

- Use `gh` for GitHub PRs, issues, discussions, checks, and Actions logs.
- Do not use web search for repository state. Query live GitHub, SVN, Nexus, crates.io, PyPI, npm, and Maven URLs directly.
- Do not claim any step succeeded until the external system confirms it.
- Treat an RC as bound to its signed tag, target commit, and generated artifacts. Do not create a new RC merely because `main` advances after tagging.
- Do not assume bindings or integrations share the top-level OpenDAL version. Each released binding or integration can have its own version.
- Do not start a public vote with broken links, open Maven staging, missing SVN artifacts, or incomplete required workflows.
- Do not conflate Nexus `Close` before voting with Nexus `Release` after the vote passes.
- Do not over-block on unrelated/noncritical CI if the release gate is explicitly narrowed.
- Do not put SVN, Nexus, or mail credentials in commands, files, issue text, PRs, or release notes. Read them from environment variables or an interactive prompt and avoid shell history when possible.
- Commit messages and public release text must not include agent attribution.

## Version Scope Rules

OpenDAL releases have multiple version scopes:

- `opendal_version`: the final core OpenDAL release version, for example `0.56.0`.
- `release_version`: the RC directory and RC tag version, for example `0.56.0-rc.4`.
- Package versions: the versions of individual core, binding, and integration packages listed in `dev/src/release/package.rs`.

Rules:

- Use `release_version` for RC tags, vote titles, staged website URLs, and ASF SVN `dist/dev/opendal/${release_version}/` directories.
- Use `opendal_version` for the final release tag and ASF SVN `dist/release/opendal/${opendal_version}/`.
- Use package-specific versions for generated source archive names, package repository checks, and language binding or integration readiness.
- When verifying artifacts, build an explicit package-to-version map from `dev/src/release/package.rs`; do not infer binding or integration versions from `opendal_version`.
- When checking upgrade docs, only check released bindings and integrations that appear in the current package list, and only add upgrade notes for components with breaking changes.

## Release State Model

Use these states explicitly when reporting status:

1. `planning`: release discussion/tracking issue/version bump not done.
2. `bump-pr`: version/changelog/upgrade/dependency updates are in PR.
3. `crates-bootstrapped`: the bootstrap workflow has authenticated every Rust crate in the scanned `main` publish plan, and every name exists with the exact Trusted Publisher and `trustpub_only` enabled.
4. `rc-tagged`: signed RC tag exists and was pushed.
5. `rc-ci`: tag-triggered release workflows are still running or failed.
6. `artifacts-built`: `just release` generated local ASF source artifacts.
7. `dist-dev-uploaded`: artifacts are committed to ASF SVN `dist/dev`.
8. `nexus-closed`: Java staging repo is closed and publicly accessible.
9. `vote-open`: GitHub Discussion vote is open.
10. `vote-passed`: at least 72 hours elapsed and binding vote requirements are met.
11. `official-release`: final tag, `dist/release`, package repositories, GitHub release, and announcement are complete.

If a release fails before `official-release`, abandon that RC, clean up wrong staged artifacts where needed, drop the Maven staging repo, and create the next RC.

## Choose the Current Phase

Load only the references needed for the requested phase or unresolved issue. Continue from the established release state; opening this skill does not require restarting release preparation. References retain phase-specific gates and command examples. Repository paths and shell commands are relative to the repository root; Markdown links are relative to their containing file.

- [Preparation](references/preparation.md): start a release or prepare version, changelog, and upgrade updates.
- [Rust crate bootstrap](references/rust-bootstrap.md): reserve crate names or inspect the protected bootstrap prerequisites.
- [Release candidate](references/rc.md): tag an RC, inspect its required CI, build source artifacts, upload to SVN, or close Nexus staging.
- [Go readiness](references/go.md): release or verify Go bindings and service modules when Go is in scope.
- [Rust readiness](references/rust.md): inspect the publish plan, packaging constraints, or release helper CI.
- [Vote](references/vote.md): check pre-vote readiness, prepare the vote, or determine and publish its result.
- [Official release](references/official-release.md): publish after the vote passes and complete release follow-up.
- [Troubleshooting](references/troubleshooting.md): diagnose a matching CI, registry, staging, or authentication failure.
