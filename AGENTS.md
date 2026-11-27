# AGENTS.md

Guidance for AI coding agents working in this repository.

## General Rules

- Use English in repository files: code, comments, commit messages, PR titles, PR bodies, and technical docs.
- Add comments only when they explain non-obvious intent or constraints.
- Add tests when they verify real behavior or guard a regression; do not add placeholder tests.
- Do not change public APIs or behavior tests unless the task explicitly requires it.

## AI-Assisted Contributions

Follow the [AI-Assisted Contributions](CONTRIBUTING.md#ai-assisted-contributions)
policy.

- Publish an Issue, Pull Request, Discussion, or review comment only within
  explicit authorization from an accountable human contributor or maintainer.
  Do not take unsolicited public actions or claim to speak on behalf of the
  OpenDAL PMC.
- Clearly identify AI-authored comments, triage notes, and reviews as
  AI-generated, including when posting under a contributor's or committer's
  personal GitHub account.
- Verify every claim, code reference, API behavior, and test result against the
  repository before posting. Never fabricate command output, claim unexecuted
  checks passed, or post unedited LLM dumps.
- Do not implement or submit a service-specific change based only on source-code
  analysis, a mock or emulator, a synthetic malformed response, or another
  service's implementation. Reproduce the problem against the actual service
  first.
- If actual-service reproduction is unavailable, stop before changing code or
  opening a Pull Request. Raise the hypothesis in a Discussion unless a
  maintainer has explicitly accepted it as a hardening or maintenance goal.
- A test against a fabricated response can verify implementation behavior, but
  it does not establish that the change solves a real-world problem.
- Do not weaken, skip, or delete existing assertions, behavior tests, or lint
  rules to make CI pass; fix the underlying bug or escalate when a contract
  must change.
- Keep changes scoped to the agreed goal. Do not bundle unrelated refactors,
  speculative abstractions, or capability claims unsupported by the
  implementation.
- In a Pull Request, disclose each materially contributing harness, model, and
  reasoning effort setting, along with AI's role and any assumptions or unknowns
  that affect review. Follow the AI Usage Statement requirements in
  `CONTRIBUTING.md`. Do not repeat routine validation output.

The accountable human contributor or maintainer remains responsible for every
submitted claim, review, and change.

## Maintenance and Review Agents

Agents assisting with OpenDAL maintenance and code review must protect community
attention and uphold project quality.

- **Governance boundaries:** Maintenance agents may triage issues, check for
  duplicates and security boundaries (`SECURITY-THREAT-MODEL.md`), screen pull
  requests for evidence and scope, review code, assist with reproduction, and
  prepare routine maintenance pull requests. Every pull request requires at
  least one human OpenDAL committer to review, approve, and merge it; do not
  approve or merge PRs, declare PMC consensus, issue official security
  decisions, or cast Apache release votes without explicit human committer or
  PMC review and action.
- **Fast-fail screening:** Screen incoming Issues and Pull Requests in order:
  duplicates, threat-model and project scope, linked issue or consensus, real
  reproduction and evidence, change scope and contract alignment, and CI
  status. Stop and report blocking prerequisite failures rather than spending
  review cycles polishing an unverified or out-of-scope submission.
- **High-signal comments:** Post review comments only for concrete defects,
  contract or capability mismatches, missing evidence, security risks, or
  maintainability regressions backed by specific code references. Omit filler,
  restatements of the diff, and style nitpicks already enforced by CI.
- **Bounded interaction and escalation:** Limit automated back-and-forth to at
  most one or two rounds on a thread. If a discussion does not converge, or
  touches public APIs (`core/core/src/types/`), portable specifications
  (`core/core/src/docs/specs/`), breaking changes, new services or
  dependencies, concurrency or consistency semantics, or security boundaries,
  stop automated replies and escalate to a human maintainer.
- **Contract and test rigor:** Treat passing CI as necessary, not sufficient.
  Verify that advertised `Capability` flags match implementation guarantees,
  fallbacks preserve concurrency and retry semantics, and tests protect real
  contracts. Flag or reject bloated mock tests that mirror implementation
  details without proving real behavior.

## Optimizing for Reading

OpenDAL optimizes code for reading top-to-bottom within a single file or module.
Local duplication is acceptable—and often preferred—when it keeps call chains
short and each component self-contained.

### Abstractions and Duplication

- Keep operation-specific validation, lowering, request construction, and
  response handling at the dispatch, request-builder, or response-processing
  boundary that owns the behavior. A reader should see how one operation maps
  its options and executes without following a chain of generic helpers or
  indirection layers.
- Extract a helper or shared abstraction only when it owns a stable, cohesive
  responsibility, such as a non-trivial invariant or algorithm, shared protocol
  encoding with identical semantics, a state-machine transition, or a trait or
  interface boundary.
- Do not introduce an intermediate type, wrapper, or helper merely to
  deduplicate a short condition, merge, forwarding step, request parameter,
  error mapping, or similar logic across distinct operations, layers, or crates.
  Prefer self-contained local code over shared abstractions that add cross-file
  jumps or mode flags.
- Before sharing code, verify that the call sites have the same contract,
  inputs, failure semantics, and reasons to change. Similar syntax alone does
  not justify an abstraction. Reuse an existing helper only when it already
  expresses the same contract; do not broaden it with operation-specific
  branches merely to obtain reuse.
- When cleaning up or refactoring, prioritize removing indirection and traps
  over deduplicating honest local code:
  - Remove dead branches, unused trait implementations, single-purpose
    conversion traits, pass-through shells, unused generics, and transitional
    trampolines.
  - Consolidate duplicate implementations only when duplication creates a real
    reading trap or divergence hazard—for example, an uncalled legacy copy left
    in a public module that misses bug fixes applied to the live copy, or
    parallel validation layers on the same request path that drift apart.

### State, Naming, and Comments

- Make state-machine invariants readable in one place. When a type coordinates
  multiple lifecycle phases and payload fields (especially around `close` /
  `abort` or async poll states), represent valid states as enum variants that
  own their payloads rather than loose booleans, `Option`s, and `debug_assert!`
  checks spread across methods.
- Leave small, single-screen state structs alone when their invariants are
  already obvious at a glance; do not add boilerplate enums for symmetry.
- Keep internal names, visibility, and safety comments aligned with the current
  code: name wrapped layer services `*Service` (not legacy `*Accessor`), rename
  types whose responsibility has changed, and keep `unsafe` invariants and doc
  comments accurate for the exact type they annotate.

## Documentation Style

- Prefer active voice. Name the type or component that performs an action, for example, “`RetryLayer` retries failed operations.”
- Use direct, present-tense sentences.
- Lead with what a public type or method does, then explain important constraints, defaults, and behavior.
- Describe API semantics precisely. Verify option types, capability requirements, error behavior, and overwrite or versioning semantics against the implementation.
- Keep terminology consistent with the codebase, especially `service`, `layer`, `operator`, `storage` and `operation`.
- Use parallel structure in lists and punctuate complete sentences consistently.

## Documentation Architecture

OpenDAL splits documentation by audience. The website carries only content
that holds for OpenDAL across languages; Rust-specific documentation lives in
rustdoc.

- `DESIGN.md` defines OpenDAL's living architectural model, ownership
  boundaries, public API invariants, and design principles across core,
  services, layers, and bindings. Consult it before introducing public APIs,
  new layers, fallbacks, or cross-boundary changes, and update it when
  architectural invariants change.
- `core/core/src/docs/specs/` contains the living portable contracts, the one
  set of documents published to both destinations. Rustdoc publishes them
  under `opendal::docs::specs`; the website reads the same Markdown at
  `/docs/specifications/`.
- `core/core/src/docs/concepts.rs`, `core/core/src/docs/internals/`, and
  `core/core/src/docs/performance/` contain the Rust core guides: Rust types,
  traits, implementation architecture, runtime resources, and transport
  tuning. They are rustdoc-only; do not publish them on the website.
- `website/docs/` contains cross-language concepts and non-normative guidance,
  plus the per-language guides. Content there must hold for OpenDAL as a
  whole. Do not present Rust implementation detail as cross-language behavior,
  and do not claim every binding exposes a feature (layers, builders,
  concurrency controls) unless the bindings demonstrate it.
- `core/core/src/docs/rfcs/` contains immutable accepted design history. An
  accepted RFC records the proposal that reached consensus; update a
  specification, not the accepted RFC, when the current contract changes.
- `core/core/src/docs/upgrade.md` contains upgrade procedures, and
  `CHANGELOG.md` contains the release changelog. Rustdoc exposes them through
  `opendal::docs`.
- `website/docusaurus.config.js` and `website/specifications.sidebars.js`
  define website publication and navigation for the specifications. Do not add
  a website section that republishes rustdoc content, and do not copy
  specifications into `website/docs/`.

Public API documentation under `core/core/src/types/` must explain the
observable behavior, capability requirements, and relevant errors at the API
site. Link to a specification only after the local API contract is
self-contained.

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

Service and layer composition uses these boundaries:

- `Builder::build(self)` returns a typed `Service`. `Operator::new(builder)` returns a ready-to-use `Operator` with the default layers applied.
- `raw::Service` implements storage operations with concrete associated body types. `raw::ServiceDyn` is its object-safe adapter, and `raw::Servicer` is `Arc<dyn ServiceDyn>`.
- `Service::info()` returns immutable `raw::ServiceInfo` identity (scheme, root, and name). `Service::capability()` reports the capabilities of the current service stack.
- Each service operation receives `&OperationContext`. Services obtain the composed HTTP transport and executor from this context; operation-specific inputs stay in `Op*` arguments.
- `Operator` stores a base service, a base context, and an ordered layer list. Adding a layer or replacing the base context replays all service hooks, then all context hooks, to produce consistent dispatch state.

## Service Implementation Pattern

Most services follow this shape under `core/services/<name>/`:

- `src/lib.rs`: crate docs, module declarations, public builder/config exports, and service registration function.
- `src/backend.rs`: builder and `opendal_core::raw::Service` implementation.
- `src/config.rs`: serializable config and builder conversion.
- `src/core.rs`: shared service client, request construction, and service-specific helpers.
- `src/error.rs`: service-specific error parsing.
- `src/reader.rs`, `src/writer.rs`, `src/lister.rs`, `src/deleter.rs`, `src/copier.rs`, `src/composer.rs`: operation implementations when the service needs them.
- `README.md`: service docs included into rustdoc by `src/lib.rs`.
- `src/docs.md`: builder docs included into rustdoc by `src/backend.rs`.

When adding or changing a service:

1. Put implementation in `core/services/<service>/`.
2. Implement `Builder` and `raw::Service` using `opendal_core`. Keep service identity in `ServiceInfo` and report supported behavior through `capability()`.
3. Add or update the facade feature and optional dependency in `core/Cargo.toml`.
4. Register the service in `core/src/lib.rs` when it should support URI/iterator construction.
5. Add or update behavior-test setup under `.github/services/<service>/` when real backend testing is needed.
6. Obtain runtime resources from the operation's `OperationContext` rather than caching them in the service constructed by the builder.
7. Run focused clippy/tests first, then broaden validation based on the blast radius.

### Capabilities and Upstream Bug Workarounds

- Expose standard capabilities a service declares directly through `capability()` without gating them behind `enable_*` configuration switches. Letting an individual server reject an unsupported request is valid.
- Withhold a capability only when exposing it risks incorrectness—such as wrong results, silent omissions, or skipped verification—and document why in the service or preset docs.
- A service-level opt-in can be appropriate for a non-standard protocol extension that most servers lack. Callers can still request a standard capability for an individual operation. For example, `WriteChecksum` requests verification for one write.
- Verify capability decisions and upstream quirks against a current release rather than an outdated pinned fixture; document version-specific caveats instead of withholding a capability or adding a workaround for already-fixed server behavior.
- Before working around an upstream service bug, weigh how many users it affects. Report the bug upstream and track it in an OpenDAL issue first; add a client-side workaround only when a mainstream service blocks a broad user base, and keep it small and easy to remove once upstream fixes it.

## Layer Implementation Pattern

Reusable layers live under `core/layers/<layer>/` as `opendal-layer-*` crates. Core layers that are required by `opendal-core` itself live under `core/core/src/layers/`.

When adding or changing a public optional layer:

1. Put reusable optional code in `core/layers/<layer>/`.
2. Depend on `opendal-core` and implement the object-safe `raw::Layer` trait. Use `apply_service(&self, inner: Servicer) -> Servicer` for operation wrappers and `apply_context(&self, service: Servicer, inner: OperationContext) -> OperationContext` for runtime resource wrappers; both hooks return their input unchanged by default.
3. Add or update the corresponding `layers-*` feature and optional dependency in `core/Cargo.toml`.
4. Re-export it from the facade when users should access it through `opendal::layers`.

Operation wrappers implement `raw::Service`, store the inner `Servicer`, forward unchanged operations with their context, and report the wrapped stack's capabilities. Construct the internal `*Service` wrapper directly inside `Layer::apply_service` and keep it private unless external callers need to name it; avoid leftover `layer()` trampolines and `#[doc(hidden)] pub` service structs. Context hooks receive the final service stack. Resource wrappers should forward through the previous transport or executor when earlier layers must remain effective.

## Testing Expectations

- Use `cargo fmt --all -- --check` for Rust formatting inside `core/`; use `./scripts/workspace.py cargo fmt -- --check` for the repository-wide format check.
- For core changes, `cargo clippy --workspace --all-targets --all-features -- -D warnings` is the CI-level lint gate.
- For behavior changes, run the narrow behavior test, for example `OPENDAL_TEST=s3 cargo test behavior --features tests,services-s3`.
- Behavior tests require backend credentials or fixture setup. Use `.env.example`, `fixtures/`, and `.github/services/<service>/` as the source of truth for service-specific setup.
- Integration crates under `integrations/` have their own CI workflows and should be validated in their own directories when touched.
- Binding changes should follow the binding's local README/build files and the matching `.github/workflows/ci_bindings_*.yml`.

## Pull Requests

- Before opening a PR, search for similar open PRs, issues, and in-progress branches that already address the same problem. Prefer contributing to or coordinating with existing work over opening a duplicate.
- Always use `.github/pull_request_template.md` when creating a PR.
- Keep PR titles and descriptions factual and concise.
- Do not add AI-tool marketing banners or AI co-author trailers to commits or PR descriptions; record AI usage in the AI Usage Statement section.
- If public APIs or user-facing behavior change, update docs and call out the user-facing impact in the PR template.

## Discussions and Mailing Lists

- GitHub Discussions sync to `dev@opendal.apache.org` (configured in `.asf.yaml` under `notifications.discussions`).
- Start or continue public development discussions—such as design proposals, RFC feedback requests, and general community threads—in GitHub Discussions rather than sending a separate email to `dev@opendal.apache.org`.
- GitHub commits, issues, and pull requests mirror to `commits@opendal.apache.org`, not `dev@opendal.apache.org`.

## Important Notes

- Minimum Rust version is 1.95, configured in `core/Cargo.toml` and checked by CI.
- Use `opendal_core::raw::{Service, ServiceInfo, ServiceDyn, Servicer, Layer}` for internal service and layer boundaries, and `opendal_core::OperationContext` for runtime resources.
- Use `opendal_core::raw::oio::{Read, Write, List, Delete, Copy, Compose}` for service operation bodies. `Read` opens range streams that implement `ReadStream`; type-erased handles use the corresponding `*Dyn` adapters.
- Use `Operator` and `blocking::Operator` as the public API entry points.

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` and the threat
model it links (`SECURITY-THREAT-MODEL.md`) for the project's in-scope /
out-of-scope declarations, adversary model, and known non-findings before
reporting issues.
