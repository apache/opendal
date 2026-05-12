---
name: opendal-release
description: Execute and verify Apache OpenDAL release-manager work, including RC tagging, required GitHub Actions checks, ASF SVN dist uploads, Nexus staging close/release, vote discussions, language package readiness, GitHub release, announcements, and release postmortems.
---

# OpenDAL Release

## Overview

Use this skill for Apache OpenDAL release-manager work. Treat releases as a state machine with externally visible legal and distribution effects, not as ordinary CI chores.

The primary repository runbook is `website/community/release/release.md`. The split source artifact source of truth is `dev/src/release/package.rs`. Always re-read both in the current checkout before tagging, packaging, voting, or declaring completion.

## Ground Rules

- Use `gh` for GitHub PRs, issues, discussions, checks, and Actions logs.
- Do not use web search for repository state. Query live GitHub, SVN, Nexus, crates.io, PyPI, npm, and Maven URLs directly.
- Do not claim any step succeeded until the external system confirms it.
- Do not reuse an RC tag after `main` advances. Preserve the old RC and increment the RC number.
- Do not assume bindings or integrations share the top-level OpenDAL version. Each released binding or integration can have its own version.
- Do not start a public vote with broken links, open Maven staging, missing SVN artifacts, or incomplete required workflows.
- Do not conflate Nexus `Close` before voting with Nexus `Release` after the vote passes.
- Do not over-block on unrelated/noncritical CI if the release gate is explicitly narrowed.
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
3. `rc-tagged`: signed RC tag exists and was pushed.
4. `rc-ci`: tag-triggered release workflows are still running or failed.
5. `artifacts-built`: `just release` generated local ASF source artifacts.
6. `dist-dev-uploaded`: artifacts are committed to ASF SVN `dist/dev`.
7. `nexus-closed`: Java staging repo is closed and publicly accessible.
8. `vote-open`: GitHub Discussion vote is open.
9. `vote-passed`: at least 72 hours elapsed and binding vote requirements are met.
10. `official-release`: final tag, `dist/release`, package repositories, GitHub release, and announcement are complete.

If a release fails before `official-release`, abandon that RC, clean up wrong staged artifacts where needed, drop the Maven staging repo, and create the next RC.

## Start A Release

1. Verify context:
   - Current repo is `apache/opendal`.
   - Current default branch and latest `main` SHA are known.
   - Existing release discussions, tracking issue, PRs, tags, and votes are identified.
   - Existing RC tags are listed with their target commits.

2. Read current release docs and source of truth:
   - `website/community/release/release.md`
   - `dev/src/release/package.rs`
   - `.github/workflows/release_*.yml`
   - `.github/scripts/release_rust/plan.py`
   - `.github/scripts/release_rust/publish.py`

3. Determine versions:
   - `opendal_version`: final release version, for example `0.56.0`.
   - `release_version`: RC version, for example `0.56.0-rc.4`.
   - Package-specific versions from `dev/src/release/package.rs`.
   - Existing RC numbers. If `main` advanced after the latest RC, use the next RC number.

## Bump And Release Notes

When preparing a bump PR:

- Run `just update-version` only after confirming the desired package versions.
- Update `CHANGELOG.md`.
- Update `core/core/src/docs/upgrade.md` only for core breaking changes.
- Update binding upgrade docs only for released bindings that have breaking changes:
  - `bindings/java/upgrade.md`
  - `bindings/nodejs/upgrade.md`
  - `bindings/python/upgrade.md`
- Do not add upgrade sections for bindings without breaking changes.
- Respect that every binding can have a different version.
- Regenerate dependency lists with `python3 ./scripts/dependencies.py generate` when the release docs require it.

Before opening the PR, check whether PR templates exist and use them. Keep the PR body self-contained and reviewer-facing.

## RC Tagging

Before creating an RC tag:

- Resolve the remote that points to `apache/opendal`; do not assume it is named `origin`.
- Confirm the bump PR or required fix PR is merged.
- Confirm the tag target commit exactly.
- Confirm no existing tag uses the intended RC version.

Tag and push:

```bash
apache_remote="$(git remote -v | awk '$2 ~ /github.com[:\/]apache\/opendal(\.git)?$/ && $3 == "(fetch)" { print $1; exit }')"
test -n "${apache_remote}" || {
  echo "cannot find a git remote for apache/opendal" >&2
  exit 1
}

git fetch "${apache_remote}" main --tags
git tag -s "v${release_version}" "${main_sha}" -m "v${release_version}"
git tag -v "v${release_version}"
git push "${apache_remote}" "v${release_version}"
```

If a new commit lands after an RC tag and before the release is final, do not move the tag. Create the next RC tag.

## Required CI Gate

After pushing the RC tag, inspect tag-triggered workflows with `gh`.

Default required gate:

- `Release Rust Packages`
- `Release Java Binding`
- `Bindings Java CI`
- `Release Python Binding`
- `Release NodeJS Binding`
- `Bindings NodeJS CI`
- `Docs`

If the release manager explicitly narrows or expands the gate, follow that instruction and state the gate in status updates.

Useful commands:

```bash
gh run list --repo apache/opendal --branch "v${release_version}" --event push --limit 50 \
  --json name,status,conclusion,databaseId,url

gh run view "${run_id}" --repo apache/opendal --json status,conclusion,jobs

gh run view "${run_id}" --repo apache/opendal --log-failed
```

Rules:

- Rerun transient workflow failures when logs indicate network, GitHub, package registry, or runner flakiness.
- If code or workflow changes are needed, land a PR on `main`, then create the next RC.
- Python wheel matrix can be slow. Poll job status before treating long runtime as a real failure.
- Dotnet/NuGet RC publish failures are nonblocking only if Dotnet is outside the agreed gate. RC tags must not publish prerelease packages to NuGet.

## Rust Release Readiness

Before official release, and preferably before vote, inspect the Rust publish plan:

```bash
python3 .github/scripts/release_rust/plan.py
```

Release safety requirements from the 0.56.0 cycle:

- `core/testkit` / `opendal-testkit` must be in the Rust publish plan when top-level `opendal` references it through the `tests` feature.
- Publish helpers must use `cargo publish --package <name>` rather than relying on workspace defaults.
- Repo-local `dev-dependencies` can break packaging even with `cargo publish --no-verify`; use `.github/scripts/release_rust/publish.py` and keep its tests green.
- Trusted publishing tokens cannot create new crates. If a new crate name is introduced, verify creation permissions or pre-create/publish manually.

If release helper CI is noisy, rebase onto latest `origin/main`, run targeted helper tests locally, then update the PR.

## Build ASF Source Artifacts

Only build artifacts after the required RC workflows are green.

```bash
git checkout "v${release_version}"
rm -rf dist
just release
find dist -maxdepth 1 -type f | sort
```

Verify:

- Artifacts exist for every package listed in `dev/src/release/package.rs`.
- Each package group has `.tar.gz`, `.tar.gz.asc`, and `.tar.gz.sha512`.
- Artifact filenames use package-specific versions, not necessarily `opendal_version`. For example, Java, Python, Node.js, C/C++, and integrations can all differ from each other and from core.
- There is no obsolete monolithic `apache-opendal-${opendal_version}-src.tar.gz` assumption.

## Upload To ASF SVN `dist/dev`

Use the user's normal SVN configuration. Do not use an isolated SVN config unless the release manager explicitly asks for it.

```bash
svn co https://dist.apache.org/repos/dist/dev/opendal /tmp/opendal-dist-dev-${release_version}
mkdir /tmp/opendal-dist-dev-${release_version}/${release_version}
cp dist/* /tmp/opendal-dist-dev-${release_version}/${release_version}/
svn add /tmp/opendal-dist-dev-${release_version}/${release_version}
svn status /tmp/opendal-dist-dev-${release_version}
svn commit /tmp/opendal-dist-dev-${release_version} -m "Prepare for ${release_version}"
svn ls https://dist.apache.org/repos/dist/dev/opendal/${release_version}/
```

Stop rules:

- If `svn commit` fails with `E215004 Authentication failed`, treat it as an auth blocker, not an artifact blocker.
- If `svn commit --force-interactive` hangs, do not keep retrying forced interactivity from Codex. It may not be able to access macOS Keychain prompts.
- Do not say "uploaded" until SVN returns a committed revision and `svn ls` confirms the remote RC directory.

## Close Java Nexus Staging

Find the staging repo id from the Java release workflow logs or Nexus UI. It has the form `orgapacheopendal-<number>`.

The Java workflow can deploy staging artifacts without closing them. A successful Java release workflow does not mean Maven is vote-ready.

Before vote, the staging repo must be closed and publicly exposed:

```bash
curl -sS -L -o /tmp/opendal-maven-index.html -w '%{http_code} %{url_effective}\n' \
  https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/
```

Stop rules:

- Any `404` from the staging URL means the Maven artifacts are not vote-ready. Inspect the response body to distinguish an open or not-exposed staging repo from a wrong repo id, dropped repo, or missing repo.
- `404` with text like `staging: open` or `not exposed` means the repo exists but is not closed. Close it before voting.
- `Close` is pre-vote. `Release` is post-vote. Do not click or automate `Release` before the vote passes.
- If the vote fails, drop the staging repo.

Issue `apache/opendal#7435` tracks automating the RC pre-vote close step.

## Pre-Vote Readiness Checklist

Run this checklist immediately before creating the vote discussion:

- RC tag exists and points to the intended commit.
- Required RC workflows are `completed/success`.
- `dist/dev/opendal/${release_version}/` exists and contains all generated source artifacts.
- The artifact filenames in `dist/dev` match the package-specific versions from `dev/src/release/package.rs`.
- `KEYS` URL is reachable: `https://downloads.apache.org/opendal/KEYS`.
- Maven staging URL returns success and is not an open/hidden staging repo.
- TestPyPI project URL is reachable: `https://test.pypi.org/project/opendal/`.
- Staged website URL is reachable: `https://opendal-v${release_version with dots replaced by hyphens}.staged.apache.org/`.
- `scripts/verify.py` is reachable from the RC tag.

Do not start a vote if any checklist item fails.

## Start Vote Discussion

Create the discussion in the `General` category of `apache/opendal`.

Use the repository runbook template, with:

- Title: `[VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1`
- Source packages: `https://dist.apache.org/repos/dist/dev/opendal/${release_version}/`
- Git tag: `https://github.com/apache/opendal/releases/tag/v${release_version}`
- Maven staging repo: `https://repository.apache.org/content/repositories/orgapacheopendal-${maven_artifact_number}/`
- Website: `https://opendal-v${release_version with dots replaced by hyphens}.staged.apache.org/`
- Verify command:

```bash
svn co https://dist.apache.org/repos/dist/dev/opendal/${release_version}/ opendal-dev
cd opendal-dev
curl -sSL https://github.com/apache/opendal/raw/v${release_version}/scripts/verify.py -o verify.py
python verify.py
```

Use `gh api graphql` by default. Creating the discussion requires the repository id and the `General` category id, not just the category name:

```bash
gh api graphql -F query='
query {
  repository(owner: "apache", name: "opendal") {
    id
    discussionCategories(first: 20) {
      nodes { id name slug }
    }
  }
}'
```

Then call `createDiscussion` with the resolved repository id, category id, title, and body. Avoid hand-editing multiline bodies through escaped `\n`; use a body file or stdin.

```bash
gh api graphql \
  -F repositoryId="${repository_id}" \
  -F categoryId="${category_id}" \
  -F title="${vote_title}" \
  -F body=@/tmp/opendal-vote.md \
  -F query='
mutation($repositoryId: ID!, $categoryId: ID!, $title: String!, $body: String!) {
  createDiscussion(input: {repositoryId: $repositoryId, categoryId: $categoryId, title: $title, body: $body}) {
    discussion { number url }
  }
}'
```

## Vote Result

The vote must stay open for at least 72 hours unless the release manager explicitly declares an emergency case.

Before claiming the result:

- Count only valid binding votes from OpenDAL PMC members as binding.
- Require at least 3 `+1` binding votes.
- Require more `+1` binding votes than `-1` binding votes.
- Use voters' real names, public profile names, or Apache IDs in the result.
- Check that the vote discussion is not closed and that a result discussion has not already been posted.

Create the result discussion with:

- Title: `[RESULT][VOTE] Release Apache OpenDAL ${release_version} - Vote Round 1`
- Body containing binding votes, non-binding votes, `+0`, `-1`, and the vote thread URL.

Do not declare an ASF release official just because the vote looks promising. Wait for the formal result.

## Official Release

After the vote passes:

1. Push final release tag:

```bash
apache_remote="$(git remote -v | awk '$2 ~ /github.com[:\/]apache\/opendal(\.git)?$/ && $3 == "(fetch)" { print $1; exit }')"
test -n "${apache_remote}" || {
  echo "cannot find a git remote for apache/opendal" >&2
  exit 1
}

git checkout "v${release_version}"
git tag -s "v${opendal_version}" -m "v${opendal_version}"
git push "${apache_remote}" "v${opendal_version}"
```

2. Move SVN artifacts from `dist/dev` to `dist/release`:

```bash
svn mv https://dist.apache.org/repos/dist/dev/opendal/${release_version} \
  https://dist.apache.org/repos/dist/release/opendal/${opendal_version} \
  -m "Release ${opendal_version}"
```

3. Release Maven artifacts in Nexus:
   - Open https://repository.apache.org/#stagingRepositories.
   - Find `orgapacheopendal-${maven_artifact_number}`.
   - Click `Release`.

4. Verify language package repositories:
   - Rust: `https://crates.io/crates/opendal`
   - Python: `https://pypi.org/project/opendal/`
   - Java: Maven Central or Nexus search for `opendal`
   - Node.js: `https://www.npmjs.com/package/opendal`

For Rust, verify both top-level `opendal` and any split crates added or changed in the release. For bindings and integrations, verify the package-specific version from `dev/src/release/package.rs`, not `opendal_version`.

5. Create GitHub Release for `v${opendal_version}`:
   - Target branch is `main`.
   - Generate release notes.
   - Prepend upgrade notes only for components with breaking changes.

6. Send announcement:
   - GitHub Discussions `Announcements`.
   - `announce@apache.org` from the committer email setup.
   - Use notable changes, not a raw breaking-change dump.

## Post Release

After official release:

- Verify `https://dist.apache.org/repos/dist/release/opendal/${opendal_version}/`.
- Verify old release cleanup requirements.
- Verify website download page references the ASF release.
- Verify package repository propagation after enough sync time.
- Close or update the release tracking issue.
- Record failures and permanent fixes as PRs/issues, not just notes.

Old release cleanup:

```bash
svn ls https://dist.apache.org/repos/dist/release/opendal
svn del -m "Archiving OpenDAL release X.Y.Z" \
  https://dist.apache.org/repos/dist/release/opendal/X.Y.Z
```

## Common Failure Patterns

### Required CI is green except unrelated workflows

Use the agreed gate. For 0.56.0, the blocking gate was Rust / Java / Python / NodeJS. Dotnet RC NuGet publishing was outside that gate and was fixed separately.

### Python release looks stuck

The wheel matrix can take a long time, especially macOS Intel and Windows. Poll actual job status and wait for conclusion before rerunning.

### Dotnet tries to publish on RC tags

RC tags should build/validate but not publish prerelease packages to NuGet. The durable fix is a prerelease guard in `.github/workflows/release_dotnet.yml`, as done in PR `#7433`.

### Rust publish fails on dev dependencies

Do not patch workflow YAML blindly. Use and test `.github/scripts/release_rust/publish.py`, which strips repo-local `dev-dependencies` during packaging and restores manifests afterward.

### `opendal-testkit` is missing from crates.io plan

Include `core/testkit` / `opendal-testkit` when `opendal` references it through the `tests` feature. Make it publishable and publish before top-level `opendal`.

### SVN authentication fails

Confirm whether the files are staged locally. If yes, the blocker is credentials. Do not claim upload until `svn commit` returns a revision. macOS Keychain credentials may be visible to `svn auth` but unavailable to the Codex process.

### Maven URL returns 404

Inspect the response body. If it says the staging repo exists but is `open` or `not exposed`, close the repo before voting. Do not treat Java workflow success as Maven vote-readiness.

### Vote has comments but may not have passed

Count binding votes explicitly and verify the 72-hour rule. Do not move artifacts to `dist/release` or release Maven artifacts until the result is formally posted.
