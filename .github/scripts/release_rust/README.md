# Release Rust Plan

This directory contains the planning logic for the Rust crates.io release workflow.

## Why this script exists

After the repository split, Rust crates are no longer represented by a short hand-maintained list.
The release workflow needs to:

- discover all publishable Rust crates under `core/` and `integrations/`
- exclude crates with `publish = false`
- publish them in dependency order so local path dependencies are already available on crates.io
- package crates without repo-local dev-dependencies so same-version dev-dependency cycles don't block publishing

Keeping this logic in a standalone script makes it testable and keeps the workflow YAML readable.

## Planned crate roots

The planner scans:

- `core/Cargo.toml`
- `core/core/Cargo.toml`
- `core/testkit/Cargo.toml`
- `core/layers/*/Cargo.toml`
- `core/services/*/Cargo.toml`
- `integrations/*/Cargo.toml`

It reads local `dependencies`, `build-dependencies`, and target-specific variants of those tables
to build a dependency graph, then emits a deterministic topological order.

## Usage

Print the publish order as JSON:

```bash
python3 .github/scripts/release_rust/plan.py
```

Write the same JSON to GitHub Actions output as `packages=<json>`:

```bash
python3 .github/scripts/release_rust/plan.py --github-output
```

Publish the planned crates:

```bash
PACKAGES="$(python3 .github/scripts/release_rust/plan.py)" \
  python3 .github/scripts/release_rust/publish.py
```

`publish.py` wraps `cargo publish` with the release-specific behavior we need:

- retries crates.io rate limits by using the server-provided retry time
- uses `cargo publish --package <name>` so workspace packages publish the intended crate
- temporarily removes repo-local `dev-dependencies` from the package manifest before publishing
- restores every touched manifest and lockfile after each package

## Tests

Run the unit tests with:

```bash
python3 .github/scripts/release_rust/test_plan.py
python3 .github/scripts/release_rust/test_publish.py
```
