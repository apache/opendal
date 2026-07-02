# AGENTS.md

Guidance for AI coding agents working in this repository.

## General Rules

- Use English in repository files: code, comments, commit messages, PR titles, PR bodies, and technical docs.
- Add comments only when they explain non-obvious intent or constraints.
- Add tests when they verify real behavior or guard a regression; do not add placeholder tests.
- Do not change public APIs or behavior tests unless the task explicitly requires it.

## Rust Workspace Commands

The Rust workspace for OpenDAL core lives under `core/`. There is no root `Cargo.toml`; run core cargo commands from `core/`.

```bash
cd core

# Check and build
cargo check
cargo build --locked
cargo build --all-features --locked

# Lint, matching Core CI
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Lint a focused service/layer feature set
cargo clippy --all-targets --features=services-s3 -- -D warnings

# Unit tests, matching Core CI
cargo nextest run --workspace --no-fail-fast --all-features

# Doc tests and docs
cargo test --workspace --doc --all-features
cargo doc --lib --no-deps --all-features

# Behavior tests
OPENDAL_TEST=s3 cargo test behavior --features tests,services-s3

# Format core workspace
cargo fmt --all
cargo fmt --all -- --check
```

Repository-wide format checks run from the repository root:

```bash
./scripts/workspace.py cargo fmt -- --check
taplo format --check
```

Code generation and release helpers also run from the repository root:

```bash
just generate python
just generate java
just update-version
just release
```

## Current Architecture

OpenDAL's Rust core has been split into a facade crate plus smaller core, service, and layer crates.

- `core/Cargo.toml`: Rust workspace root and `opendal` facade package.
- `core/src/lib.rs`: facade crate that re-exports `opendal-core`, wires optional service/layer crates, and registers enabled services for `Operator::from_uri` / `Operator::via_iter`.
- `core/core/`: `opendal-core`, containing public core types, raw traits, shared layers, HTTP utilities, docs, RFCs, and the always-available memory service.
- `core/services/<service>/`: standalone service crates named `opendal-service-*`.
- `core/layers/<layer>/`: standalone layer crates named `opendal-layer-*`.
- `core/testkit/`: behavior-test support.
- `core/tests/behavior/`: core behavior test entrypoint.
- `integrations/`: ecosystem integrations such as `object_store`, `parquet`, `dav-server`, `unftp-sbe`, and Spring.
- `bindings/`: language bindings; each binding has its own build/test conventions.
- `website/`: documentation website.
- `dev/`: repository maintenance, code generation, and release tooling used by `just`.

Important consequences of the split:

- Service and layer code no longer lives under `core/src/services` or `core/src/layers`.
- Public API changes usually touch `core/core/src/...` and the facade exports in `core/src/lib.rs`.
- Optional user-facing features are declared in `core/Cargo.toml` as `services-*` and `layers-*`, and usually map to optional `opendal-service-*` / `opendal-layer-*` dependencies.
- The memory service is in `core/core/src/services/memory`; `services-memory` is a deprecated compatibility feature because memory is always enabled.

## Service Implementation Pattern

Most services follow this shape under `core/services/<name>/`:

- `src/lib.rs`: crate docs, module declarations, public builder/config exports, and service registration function.
- `src/backend.rs`: builder and `opendal_core::raw::Access` implementation.
- `src/config.rs`: serializable config and builder conversion.
- `src/core.rs`: shared service client, request construction, and service-specific helpers.
- `src/error.rs`: service-specific error parsing.
- `src/reader.rs`, `src/writer.rs`, `src/lister.rs`, `src/deleter.rs`, `src/copier.rs`: operation implementations when the service needs them.
- `src/docs.md`: service docs included into rustdoc.

When adding or changing a service:

1. Put implementation in `core/services/<service>/`.
2. Implement `Builder` and `Access` using `opendal_core`.
3. Add or update the facade feature and optional dependency in `core/Cargo.toml`.
4. Register the service in `core/src/lib.rs` when it should support URI/iterator construction.
5. Add or update behavior-test setup under `.github/services/<service>/` when real backend testing is needed.
6. Run focused clippy/tests first, then broaden validation based on the blast radius.

## Layer Implementation Pattern

Reusable layers live under `core/layers/<layer>/` as `opendal-layer-*` crates. Core layers that are required by `opendal-core` itself live under `core/core/src/layers/`.

When adding or changing a public optional layer:

1. Put reusable optional code in `core/layers/<layer>/`.
2. Depend on `opendal-core` and implement `Layer` / `LayeredAccess` against `opendal_core::raw`.
3. Add or update the corresponding `layers-*` feature and optional dependency in `core/Cargo.toml`.
4. Re-export it from the facade when users should access it through `opendal::layers`.

## Testing Expectations

- Use `cargo fmt --all -- --check` for Rust formatting inside `core/`; use `./scripts/workspace.py cargo fmt -- --check` for the repository-wide format check.
- For core changes, `cargo clippy --workspace --all-targets --all-features -- -D warnings` is the CI-level lint gate.
- For behavior changes, run the narrow behavior test, for example `OPENDAL_TEST=s3 cargo test behavior --features tests,services-s3`.
- Behavior tests require backend credentials or fixture setup. Use `.env.example`, `fixtures/`, and `.github/services/<service>/` as the source of truth for service-specific setup.
- Integration crates under `integrations/` have their own CI workflows and should be validated in their own directories when touched.
- Binding changes should follow the binding's local README/build files and the matching `.github/workflows/ci_bindings_*.yml`.

## Pull Requests

- Always use `.github/pull_request_template.md` when creating a PR.
- Keep PR titles and descriptions factual and concise.
- Do not add AI-tool branding or co-author trailers.
- If public APIs or user-facing behavior change, update docs and call out the user-facing impact in the PR template.

## Important Notes

- Minimum Rust version is 1.91, configured in `core/Cargo.toml` and checked by CI.
- Use `opendal_core::raw::Access`, `Layer`, and `LayeredAccess` for internal implementations.
- Use `opendal_core::raw::oio::{ReadStream, Write, List, Delete}` for operation bodies.
- Use `Operator` and `blocking::Operator` as the public API entry points.
- Prefer existing helpers in `opendal-core` before adding service-local utilities.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` and the threat
model it links (`SECURITY-THREAT-MODEL.md`) for the project's in-scope /
out-of-scope declarations, adversary model, and known non-findings before
reporting issues.
