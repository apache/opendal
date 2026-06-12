- Proposal Name: `operator_composition`
- Start Date: 2026-06-12
- RFC PR: [apache/opendal#7740](https://github.com/apache/opendal/pull/7740)
- Tracking Issue: [apache/opendal#7741](https://github.com/apache/opendal/issues/7741)

# Summary

Rebuild how an `Operator` is composed. `Access` becomes the object-safe `Service` trait. `Layer` becomes an object-safe bundle of `apply_*` hooks. An `Operator` holds three base providers — the service, the HTTP transport, and the executor — plus an ordered layer list; the composed service stack and a `ServiceContext` snapshot are replayed from the bases on every mutation. Composed resources reach services as an explicit `ctx` parameter on every operation method. The static/dyn dual-trait machinery, `TypeEraseLayer`, `OperatorBuilder`, `finish()`, and all mutable shared state in `AccessorInfo` are deleted.

# Motivation

Three independent problems share one root: composition state is scattered across two trait worlds, one mutable context, and five overlapping names.

## Two worlds, one bridge

OpenDAL maintains a static world (`Access` with five associated types and RPITIT; every layer redeclares `LayeredAccess` and forwards all types), a dynamic world (`AccessDyn` plus the `ReadDyn` / `WriteDyn` / `ListDyn` / `DeleteDyn` dual traits), and a bridge (`TypeEraseLayer`). Yet every `Operator` call already goes through `Arc<dyn AccessDyn>`: static dispatch exists only between layers below the type-erasure point, so the production path pays one dyn boundary today while the codebase pays for both worlds. The concurrent-limit layer alone contains 20 associated-type declarations; retry, timeout, and foyer contain 10 each; monomorphizing 50 services across all layers is a significant share of compile time.

## Two extension mechanisms with conflicting semantics

The operation plane composes structurally: a layer wraps an accessor, lexically scoped to the new stack. The HTTP, task, and capability planes compose imperatively: layers mutate the shared `AccessorInfo` at apply time via `update_http_client` / `update_executor` / `update_full_capability` — a global side effect with temporal ordering semantics. The imperative `update_*(f: T -> T)` API stores only the folded result and erases the contribution history. Four defects follow:

1. **Replace destroys wrappers.** `update_http_client` documents that "requests must be forwarded to the old HTTP client", yet `HttpClientLayer` itself passes `|_| client`. With `.layer(TracingLayer).layer(HttpClientLayer::new(c))`, HTTP-level spans silently disappear — order-sensitive, unobservable partial degradation.
2. **Clone aliasing.** `op.clone().layer(...)` mutates the shared info and leaks the layer's context effects back into the original operator. The Java binding does exactly this today.
3. **Inconsistent double-apply.** Applying a layer twice stacks the operation wrapper twice with no defined contract on the context planes.
4. **Capability ordering hazard.** `set_native_capability` overwrites `full_capability`, silently dropping layer transforms applied earlier.

## Fragmented vocabulary

One storage implementation spans five words: the S3 service crate (`opendal-service-s3`), the `S3Backend` struct, the `Access` trait, and the `Accessor` / `AccessorInfo` erased forms. The crate, feature, module, and directory names already say "service"; the trait is the only holdout.

The expected outcome is one composition model with one rule: the four defects become unrepresentable by construction, thousands of lines of type plumbing disappear, the vocabulary collapses to one word, and the request hot path gets slightly faster.

# Guide-level explanation

## For operator users

```rust
let op = Operator::new(S3::default())?   // already a complete, usable operator
    .layer(RetryLayer::new())
    .layer(TracingLayer)
    .http_client(my_client)              // replace the HTTP transport (optional)
    .executor(my_executor);              // replace the task runtime (optional)
```

There is no `finish()` and no `OperatorBuilder`: `Operator::new` returns a working operator, and every call above consumes it and returns a new one. There is exactly one way to apply a layer, shared by Rust users and bindings alike.

Providers and layers cannot interfere, regardless of order: `.http_client()` replaces the bottom of the HTTP plane while every layer's instrumentation survives, because the operator replays the composition from its bases on each change. The two roles are distinguished by form — providers are resource setters named after the existing types (`http_client`, `executor`), while every `Layer` hook carries the `apply_` prefix. `HttpClientLayer` is deleted; migration is the one-line change from `.layer(HttpClientLayer::new(client))` to `.http_client(client)`.

`op.clone().layer(...)` affects only the new operator. Two operators branched from one base are independent views — separate layers, separate contexts — over the same shared backend instance.

## For layer authors

`Layer` is a bundle of hooks, one per plane, all defaulting to identity:

```rust
impl Layer for TracingLayer {
    fn apply_service(&self, inner: Arc<dyn Service>) -> Arc<dyn Service> {
        Arc::new(TracingService { inner })
    }

    fn apply_http_fetch(&self, inner: HttpFetcher) -> HttpFetcher {
        Arc::new(TracingFetcher { inner })
    }

    fn apply_execute(&self, inner: Executor) -> Executor {
        Executor::with(TracingExecutor { inner })
    }
}
```

No generic parameter, no `LayeredAccess`, no associated types, no `update_*` calls. A layer that changes capability does so in its wrapper's `capability()` method; for layers that transform capability without wrapping any operation, `raw` provides a `with_capability(service, f)` helper.

One contract is new and strict: **layer-owned state lives in the `Layer` struct; wrappers are cheap projections.** `apply_*` hooks are re-invoked whenever an operator replays its composition, so state created inside a hook would be silently duplicated. A cache layer holds its cache in the layer and lets wrappers reference it — which is how the foyer layer is already written; replay turns the convention into a contract. Consequently, one `Layer` instance applied to two operators shares its state.

## For service authors

The trait formerly named `Access` is now `Service`, collapsing the service / backend / accessor vocabulary into one word: `services::S3` (the builder) builds an S3 service, which implements `Service`. The conventional `backend.rs` becomes `service.rs` and `S3Backend` becomes `S3Service`.

```rust
impl Service for S3Service {
    fn info(&self) -> Arc<ServiceInfo> {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn read(&self, ctx: &ServiceContext, path: &str, args: OpRead) -> Result<(RpRead, oio::Reader)> {
        let req = self.build_read_request(path, &args)?;
        let resp = ctx.http_client().fetch(req).await?;
        // ...
    }
}
```

Every operation method takes `ctx: &ServiceContext` as its first parameter. The split between `ctx` and `args` is provenance: `Op*` structs carry the caller's intent (range, version, concurrency), `ctx` carries the environment provided from above (HTTP client, executor, effective capability). The `Op*` / `Rp*` grammar stays pure.

`ServiceInfo` is a plain immutable struct of identity facts (scheme, root, name) constructed once at build time — no lock, no setters. Capability is deliberately not part of it: info is forwarded by wrappers untouched, while capability is transformed by them, so they have different homes. Services that need HTTP during signing refresh their signer from `ctx` at the operation boundary (for example via reqsign's `with_context`).

# Reference-level explanation

## Object-safe traits

The dyn halves of the existing dual traits become the only form: `oio::Reader` is already `Box<dyn ReadDyn>`; `ReadDyn` is renamed `Read` and the static counterpart is deleted, likewise for write, list, and delete. `HttpFetch` drops `HttpFetchDyn`. `Execute` is already object-safe and unchanged. `Service` methods return `BoxedFuture` (keeping the `MaybeSend` conditioning for wasm), making `Arc<dyn Service>` the single stack type — no alias is introduced, since reducing the name count is part of the point. Layer wrapper structs keep their logic and lose their generics: `RetryReader<R: oio::Read>` becomes `RetryReader { inner: oio::Reader }`.

## The trait surface

```rust
pub trait Service: Send + Sync {
    fn info(&self) -> Arc<ServiceInfo>;
    fn capability(&self) -> Capability;

    fn read(&self, ctx: &ServiceContext, path: &str, args: OpRead)
        -> BoxedFuture<'_, Result<(RpRead, oio::Reader)>>;
    // create_dir, write, copy, rename, stat, delete, list, presign: same shape, all take ctx.
}

pub trait Layer: Send + Sync {
    fn apply_service(&self, inner: Arc<dyn Service>) -> Arc<dyn Service> { inner }
    fn apply_http_fetch(&self, inner: HttpFetcher) -> HttpFetcher { inner }
    fn apply_execute(&self, inner: Executor) -> Executor { inner }
}

pub struct ServiceContext {
    http_client: HttpClient,   // composed: middleware folded over the transport
    executor: Executor,
    capability: Capability,
}
```

`ServiceContext` is named for its consumer and pairs with `ServiceInfo`: info is what the service declares upward, context is what the service receives downward. The name is deliberately neither `OpContext` (the `Op*` prefix is reserved for operation argument structs) nor `OperatorContext` (bottom-layer code must not import top-of-stack vocabulary).

## Operator: bases, layers, applied

```rust
pub struct Operator {
    base_service: Arc<dyn Service>,
    base_http_fetch: HttpFetcher,
    base_executor: Executor,

    layers: Arc<[Arc<dyn Layer>]>,

    applied: Arc<Applied>,
}

struct Applied {
    service: Arc<dyn Service>,
    context: Arc<ServiceContext>,
}
```

The bases are the slots, the layers are the program, and `applied` is the memo, governed by one invariant: `applied == fold(layers, bases)`. Every mutation — `.layer()`, `.http_client()`, `.executor()` — consumes the operator, replays the fold from the bases, and returns a new operator:

1. `service' = layers.fold(base_service, apply_service)`
2. `http' = layers.fold(base_http_fetch, apply_http_fetch)`
3. `executor' = layers.fold(base_executor, apply_execute)`
4. `context' = ServiceContext { http', executor', capability: service'.capability() }`

Replay is always total — no plane-skipping optimizations, one code path. Layering is a configuration-time operation; the wasted intermediate folds cost a handful of `Arc` allocations.

| Plane     | Provider (slot, last-wins)        | Middleware (folded in stack order) |
| --------- | --------------------------------- | ---------------------------------- |
| operation | service, via `Operator::new`      | `Layer::apply_service`             |
| HTTP      | transport, via `.http_client()`   | `Layer::apply_http_fetch`          |
| task      | runtime, via `.executor()`        | `Layer::apply_execute`             |

Invariants, all guaranteed by construction:

- Temporal order equals structural order; the same stack order projects onto every plane (tracing outside retry on the operation plane implies tracing outside retry on the HTTP plane).
- Providers and middleware cannot interfere: replacing a slot refolds the middleware over the new bottom.
- Applying a layer twice stacks twice on every plane consistently.
- Layers are pure values; hooks take `&self` and must tolerate repeated invocation.

Capability is deliberately not a fourth plane: its bottom value is a property of the operation-plane provider, so it composes as a `Service` method — a wrapper overrides `capability()` and consults `self.inner.capability()`, in exactly the order a separate hook would have folded. This also makes a layer's capability claim and its operation implementation adjacent in one struct, where today they are two registrations that can silently drift. The `native_` / `full_` naming pair disappears: `op.info()` exposes `capability()` read from `applied` and `native_capability()` read from `base_service`.

## Operation dispatch

`op.read()` calls `applied.service.read(&applied.context, path, args)`. The operator itself is the public-API-to-raw boundary; there is no injection machinery and no entry layer. Readers, writers, listers, and deleters clone the individual pieces they need from `ctx` at construction; background tasks spawned by writers capture the executor the same way. A middle layer can override resources per operation by constructing a modified `ServiceContext` and forwarding it to `inner` — per-tenant HTTP clients and per-call executors need no additional mechanism.

Nothing in this structure is interior-mutable. A mutation creates new values; existing handles and in-flight operations keep theirs. The Java binding's `op.clone().layer(concurrent_limit)` leak is structurally impossible.

## Performance

Estimates, to be confirmed by a benchmark gate (five-layer fully-dyn stack versus current, memory backend) before implementation:

- Today every HTTP request pays an `RwLock` read plus an `HttpClient` clone (~30ns) to fetch the client from `AccessorInfo`. After this change, `ctx` is passed by reference for free, and operation bodies clone only the pieces they capture.
- The dyn delta is one `BoxedFuture` allocation per operation plus one extra vtable hop per layer per poll (~2–3ns each): ~10⁻⁶ of S3 first-byte latency, ~0.5% of a single 64KiB poll, a single-digit percentage on in-memory microbenchmarks.
- Memory: the operator retains its bases and layer list (a few `Arc`s); `Applied` replaces today's `AccessorInfo` sharing.

## Deletions

`TypeEraseLayer`; `Access` and its associated types (renamed `Service`); the five static/dyn dual traits and conversion impls; `HttpFetchDyn`; `HttpClientLayer`; `OperatorBuilder` and `finish()`; `update_http_client`, `update_executor`, `update_full_capability`, `Operator::update_executor`; `AccessorInfo`'s `RwLock`, poisoning recovery, and all `set_*` setters (identity facts are constructor arguments); the `set_native_capability` / `full_capability` overwrite hazard.

## No compatibility logic

This RFC is implemented in one shot with no migration scaffolding: no deprecated aliases, no shims, no staged intermediate states. Old APIs are deleted outright, and migration is a mechanical rewrite documented in the upgrade guide. Carrying compatibility logic would re-introduce exactly the kind of duplicated machinery this RFC exists to remove.

# Drawbacks

- This is a major breaking change to the raw API: every third-party `Service` and `Layer` implementation is rewritten, and in-repo roughly 50 services and 20 layers migrate. The changes are mechanical (delete associated-type plumbing; thread `ctx` through operation methods and request helpers) but wide, and the `Access` → `Service` rename further inflates the diff.
- It reverses the documented design tenet that OpenDAL uses static dispatch internally; in-memory microbenchmarks will regress by a visible single-digit percentage.
- The replay contract — state in the `Layer`, wrappers as projections — is a new obligation on layer authors, enforced by documentation rather than types.

# Rationale and alternatives

The root cause analysis: providers and wrappers have different merge disciplines — last-wins versus ordered folding — and any API that expresses both as `update(f: T -> T)` over an opaque composed value must erase the distinction. This cannot be fixed by documentation; the existing forwarding contract on `update_http_client` is violated by `HttpClientLayer` itself. The fix is to store the ingredients (bases and layers) and derive the result, never the reverse.

`finish()` and `OperatorBuilder` are rejected as vestiges: a deferred-build barrier matters only when the service must capture the composed context at construction time. With the explicit `ctx` parameter, services capture nothing, so eager replay per mutation is strictly simpler — it unifies the builder-phase and post-finish layer paths into one, and future stack validation (capability requirements) fails at the offending `.layer()` call with precise blame.

The `Access` → `Service` rename rides along because the rewrite is the only moment it is free: every implementor touches every impl anyway, and most of the old name's surface (`AccessDyn`, `LayeredAccess`, `TypeEraseLayer`) is deleted rather than migrated.

Alternatives considered:

1. **Structure `HttpClient` internally** (transport + middleware list), keeping all entry points: fixes replace-versus-wrap for HTTP only; the dual machinery, clone aliasing, and mutation architecture remain.
2. **A contribution store inside `AccessorInfo`** (declarative registry, derive on update): fixes the merge discipline but keeps shared mutation and both trait worlds.
3. **A lineage-shared snapshot cell** (services hold a swappable handle captured at build): rejected — it retains a shared mutation point and "layering affects the whole lineage" semantics; the explicit `ctx` parameter is pure, gives perfect clone isolation, and enables per-operation overrides.
4. **Keep `HttpClientLayer` as sugar over the slot**: rejected — a layer whose `apply_http_fetch` ignores `inner` has position-dependent cut-off semantics while the slot is position-independent last-wins; two spellings with divergent semantics recreate the original confusion.
5. **Flatten resources into `Op*` args instead of a `ctx` parameter**: rejected — args carry the caller's intent and ctx carries the framework-provided environment; merging the two provenances pollutes the `Op*` / `Rp*` grammar and complicates per-operation overrides.
6. **Do nothing**: the clone leak, the silent loss of HTTP instrumentation, and the per-layer boilerplate tax remain live.

# Prior art

- **object_store** is `Arc<dyn ObjectStore>` throughout and serves performance-sensitive consumers (DataFusion, Delta); **axum** boxes every route while **tower** chose generics with well-known complexity costs. The ecosystem evidence is that dyn-first is the right tradeoff for an IO library.
- The explicit `ctx` parameter follows Go's `context.Context` and Rust's own `std::task::Context`: an execution environment passed as a leading parameter rather than hidden in ambient state.
- **RFC-5479 (Context)** created the shared mutable context after rejecting per-operation threading, because layers then had to hijack every operation to extract and rebuild executors from args. That objection no longer applies: `ctx` is part of the method signature, so wrappers forward it like any other parameter, with zero extra machinery.
- The `Layer` hook design follows tower's `Layer` — a pure value that wraps a service — extended to multiple planes.

# Unresolved questions

- Benchmark gate: measure a five-layer fully-dyn stack against the current architecture on the memory backend before implementation starts.
- Whether the composed HTTP handle inside `ServiceContext` keeps the `HttpClient` type name together with its `send` / `fetch` helpers.
- Whether `blocking::Operator` needs any surface change (expected: no, it wraps the async operator).

# Future possibilities

- The retained layer list makes stack introspection nearly free: `op.info()` could list applied layers for debugging, and runtime reconfiguration (hot-swapping observability layers) becomes a replay with a modified list.
- Layers could declare capability *requirements*, checked against `inner.capability()` at each `.layer()` call, rejecting incoherent stacks at configuration time with precise blame.
- Per-operation `ServiceContext` overrides open per-tenant HTTP clients and per-call executors; convenience APIs can be added on demand without new mechanisms.
