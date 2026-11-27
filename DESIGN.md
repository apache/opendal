# Architecture and Design

This document defines the architectural model, ownership boundaries, public API invariants, and design principles for Apache OpenDAL. Priority ordering lives in [Vision](website/docs/02-vision.md), portable feature contracts live in [Specifications](core/core/src/docs/specs/README.md), and concrete type inventories and runnable examples belong in [README.md](README.md) and API documentation.

## Scope and Ownership Boundaries

OpenDAL separates storage access into explicit layers of ownership so each concern evolves at its own boundary:

| Boundary | Owns | Excludes |
| :------- | :--- | :------- |
| `opendal-core` | Public API types, `raw` extension traits, default core layers, the built-in `memory` service, and portable storage semantics | Provider wire protocols, optional layers, concrete HTTP clients, and async runtimes |
| `opendal` facade | Re-exporting core, feature-gated wiring of services, layers, transports, and executors, and opt-in process defaults | Core storage semantics and backend protocol logic |
| Services (`core/services/*`) | Provider protocols, request construction, response and error parsing, native limits, and typed builders and configs | Shared path normalization, capability gating, credential chains, and request-signing algorithms |
| Layers (`core/layers/*`) | Independently composable cross-cutting policy over operations and runtime resources | Provider-specific protocol quirks |
| Transports & Executors | Concrete HTTP client and task-spawning adapters behind core transport and executor traits | Storage semantics |
| Bindings (`bindings/*`) | Target-language idiomatic APIs, native types, and resource lifecycles over the Rust core | Divergent storage contracts |

- Authentication, credential chains, profile and environment loading, and request signing belong to `reqsign`. Services only wire in what `reqsign` provides and never re-implement credential discovery, token exchange, or signing locally.
- `opendal::*` is the public API surface; `opendal::raw::*` is the internal extension seam for service and layer authors. Public APIs must never expose `raw` types directly.
- Object storage workloads guide core representations and optimizations, while leaving room for file systems, key-value stores, and databases to execute the same operations cleanly.
- Assign behavior to the narrowest boundary that owns it: shared storage semantics to core, provider protocols and limits to the service, reusable policy to a layer, and language conventions to the binding.

## Core Primitives and Composition Topology

OpenDAL models storage access through six orthogonal primitives:

| Primitive | Question Answered | Representation |
| :-------- | :---------------- | :------------- |
| `Builder` / `Configurator` | How is a storage backend configured and constructed? | Typed service builders and serializable configs |
| `Service` / `ServiceInfo` / `Servicer` | What backend executes protocol operations and what is its identity? | `raw::Service` with concrete `oio::*` bodies, erased once at `ServiceDyn` into `Servicer` |
| `OperationContext` | What runtime resources execute I/O and background tasks? | Composed transport and executor passed per operation |
| `Layer` | What cross-cutting policy intercepts operations or runtime resources? | Object-safe two-plane trait (`apply_service` and `apply_context`) |
| `Operator` | What handle do applications hold and share for one root? | Cheap `Clone`, `Send + Sync`, type-erased, immutable handle |
| Stateful handles | What multi-step operation retains protocol state or resources? | Owned public handles over `oio::*` bodies constructed by `raw::Service` factories |

- `raw::Service` keeps operation body types as concrete associated `oio::*` traits (using `()` when unsupported) and erases services and bodies once at `ServiceDyn` / `Servicer`, avoiding nested dynamic dispatch inside backends. One-shot operations on `raw::Service` are `async`; stateful operations are synchronous factories returning operation bodies.
- `ServiceInfo` holds only immutable backend identity (`scheme`, `root`, `name`). Runtime resources live in `OperationContext`, and per-call caller intent lives in `Op*` arguments. Services read transports and executors from `&OperationContext` at call time and never cache them at build time.
- `Operator` stores its base service, base `OperationContext`, and an immutable ordered list of layers. Adding a layer or replacing base resources returns a new `Operator` by replaying every `apply_service` hook in insertion order, followed by every `apply_context` hook in the same order against the final service stack. Existing clones and in-flight operations keep their previous stack, and replacing a transport after attaching observability layers stays consistent.
- `Operator::new` installs core infrastructure layers (error context, optional capability simulation, and completion/validation) in fixed order before user layers.
- `Layer` hooks take `&self` and must remain `Send + Sync` across cloned operators and concurrent calls. Context layers that wrap a transport or executor must forward through the previous resource unless explicit replacement is intended, so lower layers remain effective.
- Core primitives avoid hidden global singletons; process-wide registries and default runtime resources are opt-in facade conveniences that applications can override or disable.

## Public API Grammar and Surface Discipline

- Start every API change from a concrete storage task a caller performs. A new type, operation, or option must simplify downstream code, prevent a real failure mode, or avoid wasted backend work.
- A public handle earns its place only when callers can use its lifetime to retain a resource or protocol state across calls—for example, reusing an open file handle across concurrent range reads or tracking a multipart upload. Stateless per-path object wrappers do not exist; operations live directly on `Operator`.
- Operations with options on `Operator` follow one uniform three-form grammar: a direct method for common defaults (`op.read`), a fluent `*_with` builder (`op.read_with`), and a `*_options` entry point taking a plain `options::*Options` struct (`op.read_options`). Keep one canonical path per concern instead of parallel API dialects.
- Keep widely shared types (`Operator`, `Error`, `Metadata`, `Capability`) minimal. Adding a field or method requires a concrete caller outside the producing component that must branch or act on it, plus proof that an existing error kind, capability, or layer cannot carry the signal.
- Core and services maintain a single async implementation path. `blocking::Operator` is a thin runtime bridge over async `Operator` rather than a parallel native blocking implementation across services, avoiding behavioral divergence between execution modes.
- Every path accepted by `Operator` is normalized relative to the operator's root: `/` addresses the root, a trailing `/` denotes a directory, and any other normalized path denotes a file. Normalization rejects root-escaping traversals before reaching any service.
- Language bindings adapt surface naming, types, configuration entry points, and resource lifecycles to target-language idioms while preserving core storage semantics, error classification, capability guarantees, and [operator configuration](core/core/src/docs/specs/operator-configuration.md) rules.

## Capabilities, Portability, and Simulation

- Define every portable operation by what the application must observe: which object is checked, when conditions or verifications take effect, and what remains visible on failure.
- `OperatorInfo::capability()` reports the effective `Capability` of the composed stack. Advertise an operation or option capability only where the composed stack satisfies its full observable contract, including commit-time atomicity and verification guarantees.
- `Operator` validates correctness-critical options against the outermost composed `Capability` before dispatch and fails fast with `ErrorKind::Unsupported` instead of silently ignoring options. Layers that dispatch to a separate target service whose capability is not reflected by the outer stack must revalidate arguments against the target's capability before dispatch.
- OpenDAL acts as a client: services expose the full set of standard capabilities they implement without gating them behind `enable_*` configuration switches. If a deployment does not support a feature, the server's own error surfaces naturally as it would through a vendor SDK. Withhold a capability only when advertising it causes silent incorrectness (wrong data or skipped verification) and document why. Non-standard protocol extensions absent from most servers are the narrow exception.
- Every fallback or simulation must pass two gates:
  1. **Semantic fidelity**: It preserves the operation's contract under concurrency, partial failure, and retry. Never emulate an atomic conditional mutation with a `stat` followed by an unconditional write; return `ErrorKind::Unsupported` when the service cannot enforce the condition at commit.
  2. **Predictable cost**: Its request and I/O amplification is bounded and visible to callers. Never silently turn a listing query into per-entry `stat` requests.
- Cross-operation portable contracts live in [core/core/src/docs/specs/](core/core/src/docs/specs/README.md), and capability-gated behavior tests under `core/tests/behavior/` provide executable conformance coverage. Service behavior changes require reproduction and verification against the actual service rather than protocol docs, emulators, or fabricated responses alone.

## Data Model and Lifecycle Invariants

- `Buffer` stores contiguous or non-contiguous `Bytes` chunks and clones cheaply by reference count. Services and layers stream payloads without forcing extra copies or linearizing buffers until a caller requests contiguous memory.
- `Metadata` is an owned, immutable value detached from response, reader, and lister lifetimes, sharing immutable storage across clones. Evaluate representation changes across the full application lifecycle: holding and cloning large listing results can outweigh construction cost.
- Write calls are all-or-nothing per buffer: accepting a buffer means all bytes were accepted, never a silent partial write.
- Stateful mutation handles require explicit `close()` to flush, commit, and return resulting metadata or completion, and `abort()` to cancel pending work where supported. Terminal failure or abort poisons the handle so subsequent calls cannot drive it further or report success; dropping a handle must never fabricate a successful commit.

## Error Handling and Operational Safety

- `Error` separates *what failed* (`ErrorKind`) from *whether retrying makes sense* (retry status: permanent, temporary, or persistent).
- Separate single-request recovery from cross-request pacing. Per-operation retry backoff belongs in `RetryLayer`; server backpressure signals that affect subsequent requests (`Retry-After`, throttling, adaptive concurrency) belong in a shared cross-request pacing layer rather than per-operation retry or extra fields on `Error`.
- Restrict expensive diagnostics such as backtrace capture to unexpected errors so expected control-flow failures stay cheap, and attach structured operation context in a single core layer rather than repeating it per service.
- Credentials, tokens, and signed query parameters must never leak into `Debug` output, logs, or `Error` context.
- Services defend against real-world backend quirks even when extra checks add work—such as inspecting HTTP 200 error bodies, validating response lengths, and refusing to commit on verification mismatch.
