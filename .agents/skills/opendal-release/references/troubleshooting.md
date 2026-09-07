# Release Troubleshooting

Repository paths and shell commands are relative to the repository root.

## Common Failure Patterns

### Required CI is green except unrelated workflows

Use the agreed gate. For 0.56.0, the blocking gate was Rust / Java / Python / NodeJS. Dotnet RC NuGet publishing was outside that gate and was fixed separately.

### Python release looks stuck

The wheel matrix can take a long time, especially macOS Intel and Windows. Poll actual job status and wait for conclusion before rerunning.

### TestPyPI reports duplicate files

`uv publish` can fail with `400 File already exists` when an RC attempt already uploaded the same wheel to TestPyPI. Record the filename, verify whether the existing TestPyPI artifacts are usable, and get an explicit release-manager waiver before voting. Do not treat duplicate files as equivalent to a successful fresh upload.

### PyPI top-level JSON is stale

Do not rely only on `https://pypi.org/pypi/opendal/json` immediately after publishing. Check `https://pypi.org/pypi/opendal/${python_version}/json` and the simple index for the exact version.

### Java CI fails on crates.io index flakiness

Failures such as `no matching package named futures-channel found` from the crates.io sparse index are registry flakes. Rerun failed jobs before opening a code-fix PR.

### Docs deploy fails after docs build succeeds

If the build jobs pass but the tagged website deploy fails with SSH key or rsync errors such as `error in libcrypto` or `Permission denied (publickey)`, separate it from documentation correctness. It can be a release follow-up when the release manager explicitly agrees; otherwise fix the deploy secret and create a new RC if the agreed gate requires `Docs` success.

### Dotnet tries to publish on RC tags

RC tags should build/validate but not publish prerelease packages to NuGet. The durable fix is a prerelease guard in `.github/workflows/release_dotnet.yml`, as done in PR `#7433`.

### Rust publish fails on dev dependencies

Do not patch workflow YAML blindly. Use and test `.github/scripts/release_rust/publish.py`, which strips repo-local `dev-dependencies` during packaging and restores manifests afterward.

### `opendal-testkit` is missing from crates.io plan

Include `core/testkit` / `opendal-testkit` when `opendal` references it through the `tests` feature. Make it publishable and publish before top-level `opendal`.

### SVN authentication fails

Confirm whether the files are staged locally. If yes, the blocker is credentials. Do not claim upload until `svn commit` returns a revision. macOS Keychain credentials may be visible to `svn auth` but unavailable to the Codex process. If `--force-interactive` hangs, stop retrying it and switch to a shallow working copy plus explicit credential environment variables.

### Rust publish takes a long time

The final Rust publish can run for a long time and still be healthy because split crates publish in plan order. Inspect `.github/scripts/release_rust/plan.py`, watch the workflow logs, and verify crates progressively instead of assuming the job is stuck.

### Maven URL returns 404

Inspect the response body. If it says the staging repo exists but is `open` or `not exposed`, close the repo before voting. Do not treat Java workflow success as Maven vote-readiness.

### Vote has comments but may not have passed

Count binding votes explicitly and verify the 72-hour rule. Do not move artifacts to `dist/release` or release Maven artifacts until the result is formally posted.
