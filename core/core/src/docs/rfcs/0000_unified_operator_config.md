- Proposal Name: `unified_operator_config`
- Start Date: 2026-11-25
- RFC PR: [apache/opendal#0000](https://github.com/apache/opendal/pull/0000)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

> Status note: One design decision remains open before acceptance (see [Unresolved questions](#unresolved-questions)): whether `Operator::from_iter::<B>` stays service-only.

# Summary

Extend OpenDAL's string-based operator configuration (`Operator::from_uri`, `Operator::via_iter`, and `OperatorRegistry::load`) so a single flat key-value configuration can assemble all three RFC 7740 planes of an `Operator`: the storage `Service`, the base `OperationContext`, and an ordered list of `Layer`s.

Configuration keys are partitioned by namespace: unprefixed keys configure the service, `context.<name>.*` configures `OperationContext` (starting with `context.http.*`), `layers` declares the ordered layer list, and `layer.<name>.*` configures each layer. `OperatorRegistry` adds only two methods — `register_context` and `register_layer` — and zero new public types to `opendal-core`. Every service, context provider, and layer parses its own scoped `HashMap<String, String>`, and `OperatorRegistry::load` shares the same core default-layer stack and single-fold RFC 7740 replay path as `Operator::new`. Unconfigured context slots preserve their incoming `OperationContext` unchanged, so existing URIs and custom process-wide transports behave identically to today.

# Motivation

RFC 7740 reorganized `Operator` into three explicit planes: a base `Service`, a base `OperationContext`, and an ordered `Vec<Arc<dyn Layer>>` replayed in a single fold. RFC 7749 decoupled `opendal-core` from concrete HTTP implementations behind `HttpTransport` and `HttpTransporter`.

Configuration-driven construction has not caught up with that model:

1. **Per-operator HTTP configuration is inaccessible outside Rust.** Rust callers can build a custom `ReqwestTransport` and install it via `Operator::with_context`. Language bindings (Python, Node.js, Java, C, and others) construct operators from `(scheme, map)` or URIs via `Operator::via_iter` / `Operator::from_uri` and cannot configure a per-operator proxy, `no_proxy`, or HTTP timeout without relying on process-wide environment variables such as `HTTPS_PROXY` (see [apache/opendal#6871](https://github.com/apache/opendal/issues/6871)).
2. **Layers cannot be declared in configuration.** Every layer must be wired in Rust or re-exposed individually through each binding's FFI surface.
3. **A transport-replacing layer would break RFC 7740's composition invariant.** Modeling HTTP configuration as a `Layer` that replaces `HttpTransporter` inside `apply_context` reintroduces hidden layer-order dependence: placing it after `ConcurrentLimitLayer` or `TracingLayer` silently drops their HTTP wrappers, and it overrides an explicit `Operator::with_context`. HTTP transport construction belongs in `base_ctx`, before layer replay.

A unified configuration contract lets services, context providers, and layers each parse their own namespace while assembling an `Operator` through the exact RFC 7740 model across Rust and language bindings.

# Guide-level explanation

## One configuration, three planes

Every operator constructed from a URI or key-value map uses one flat, case-insensitive string namespace split across the three RFC 7740 planes:

| Plane | Keys | Purpose |
| --- | --- | --- |
| Service | `<field>` (unprefixed) | Storage backend configuration (unchanged from today). |
| Context | `context.<name>.<field>` | Configures `OperationContext` runtime resources (initial built-in slot: `context.http.*`). |
| Layers | `layers`, `layer.<name>.<field>` | Declares ordered user layers and their options. |

Unconfigured planes keep their defaults: a URI with only service keys constructs the exact same operator as today, reusing the process-wide default HTTP transport and executor.

## Examples

### Rust: URI and key-value construction

```rust,ignore
use opendal::{Operator, Result};

fn build() -> Result<Operator> {
    let op = Operator::from_uri(
        "s3://my-bucket/data?region=us-east-1\
         &context.http.proxy=http://127.0.0.1:7890\
         &context.http.no_proxy=localhost,.internal\
         &context.http.connect_timeout=5s\
         &layers=concurrent_limit,timeout,retry\
         &layer.concurrent_limit.permits=64\
         &layer.concurrent_limit.http_permits=16\
         &layer.timeout.io_timeout=10s\
         &layer.retry.max_times=5",
    )?;
    Ok(op)
}
```

`layers` lists layers from inner to outer, matching chained Rust calls:

```rust,ignore
// Equivalent Rust layer order for `layers=concurrent_limit,timeout,retry`:
op.layer(concurrent_limit)
    .layer(timeout)
    .layer(retry)
```

Requests enter `retry` first, then `timeout`, then `concurrent_limit`, and finally reach the service. Because `context.http.*` transforms `base_ctx` before layer replay, `concurrent_limit` wraps the proxy-configured `HttpTransporter` automatically regardless of layer order.

### Rust: Custom context or layer factory

Applications with a custom TLS stack, corporate CA bundle, or shared layer state can register factories directly on `OperatorRegistry`:

```rust,ignore
use opendal::{OperatorRegistry, Result};

let registry = OperatorRegistry::get();
registry.register_context("http", move |ctx, cfg| {
    let Some(transport) = build_corp_transport(&corp_ca, cfg)? else {
        return Ok(ctx);
    };
    Ok(ctx.with_http_transport(transport))
});

let op = registry.load((
    "s3://my-bucket/data",
    [
        ("region".into(), "us-east-1".into()),
        ("context.http.proxy".into(), "http://proxy.corp:8080".into()),
    ],
))?;
```

### Python and Node.js

Bindings that forward a key-value map or URI to `Operator::via_iter` / `Operator::from_uri` accept the same keys with zero new FFI APIs:

```python
import opendal

op = opendal.Operator(
    "s3",
    bucket="my-bucket",
    region="us-east-1",
    **{
        "context.http.proxy": "http://127.0.0.1:7890",
        "context.http.no_proxy": "localhost,.internal",
        "layers": "timeout,retry",
        "layer.timeout.io_timeout": "10s",
        "layer.retry.max_times": "5",
    },
)
```

```ts
const op = new Operator("s3", {
  bucket: "my-bucket",
  region: "us-east-1",
  "context.http.proxy": "http://127.0.0.1:7890",
  layers: "timeout,retry",
  "layer.timeout.io_timeout": "10s",
  "layer.retry.max_times": "5",
});
```

## Teaching mental model

- `layers` activates layers; `layer.<name>.*` supplies options for an active layer. Writing `layer.retry.max_times=5` without listing `retry` in `layers` does not enable retry.
- `context.http.*` governs HTTP requests dispatched through `OperationContext::http_transport()`, including HTTP storage services and Reqsign credential fetches that share OpenDAL's `HttpTransporter` (such as S3). It does not govern non-HTTP protocols (local filesystem, SFTP, Redis) or services whose RPC transport bypasses `HttpTransporter` (such as tonic gRPC channels in `gcs-grpc`).
- `from_uri` treats the entire URI as trusted operator configuration. Never pass an untrusted external URL directly to `from_uri`.

# Reference-level explanation

## Delta from current behavior

Verified against `main` (`31b4558934`):

| Area | Current behavior | Behavior after this RFC |
| --- | --- | --- |
| `OperatorRegistry::load` | Calls `OperatorFactory = fn(&OperatorUri) -> Result<Operator>`, which invokes `C::from_uri` and `Operator::from_config`. | Partitions keys across service, context, and layers; builds `Servicer`, folds `context.<name>.*` factories onto `OperationContext::default()`, constructs user layers in declared order, prepends core invariant layers, and folds once via `Operator::apply_layers`. |
| Core default layers | `Operator::from_builder` applies `ErrorContextLayer`, optional `SimulateLayer` (`auto-simulate`), optional `GlobLayer`, `CompleteLayer`, `CorrectnessCheckLayer`. | `Operator::from_builder` and `OperatorRegistry::load` share one internal assembly helper so both produce the same core stack and replayable `layers` list. |
| `OperatorRegistry` public API | `get`, `register`, `schemes`, `load`. | Adds `register_context` and `register_layer` (0 new public types in `opendal-core`, 0 new methods on `Operator`). |
| `OperationContext` & `Layer` config | Rust-only via `Operator::with_context` / `.layer(...)` or binding-specific FFI types. | Configurable via `context.<name>.*`, `layers`, and `layer.<name>.*` across `Operator::from_uri`, `Operator::via_iter`, and `OperatorRegistry::load`. |
| `OperatorUri` diagnostics | `OperatorUri` derives `Debug` and prints raw `password` and `options`. | `OperatorUri` implements a redacting `Debug` that omits raw credentials and sensitive option values. |

## Configuration grammar and partitioning

### Normalization and precedence

Input normalization follows existing `OperatorUri::new` semantics (`core/core/src/types/operator/uri.rs`):

1. If the input contains `://`, parse it with `url::Url::parse`, percent-decode query pairs, and ASCII-lowercase every key. Duplicate query keys take the last value.
2. Apply `extra_options` (keys ASCII-lowercased), overriding any query parameter with the same key.
3. Because percent-decoding runs before list splitting, `%2C` in a query value decodes to `,` and acts as a separator in comma-delimited fields (`layers`, `context.http.no_proxy`).

This RFC defines only the flat string key-value contract. Flattening nested formats (TOML, JSON, YAML) into this namespace is out of scope until explicitly specified.

### Namespace partitioning

Partitioning scans the normalized options map once:

- `layers`: extracted as the ordered layer list.
- `context.<name>.<rest>`:
  - `<name>` must be non-empty and contain only `[a-z0-9_-]`; `<rest>` must be non-empty (`context.`, `context..foo`, `context.http.` return `ErrorKind::ConfigInvalid`).
  - Routed into a `BTreeMap<String, HashMap<String, String>>` keyed by `<name>`, with `<rest>` as the inner key. Storing slots in a `BTreeMap` guarantees deterministic fold order when multiple `context.<name>.*` slots are present.
- `layer.<name>.<rest>`:
  - `<name>` must be non-empty and contain only `[a-z0-9_-]`; `<rest>` must be non-empty (`layer.`, `layer..foo`, `layer.retry.` return `ErrorKind::ConfigInvalid`).
  - Routed into a per-layer `HashMap<String, String>` keyed by `<name>`, with `<rest>` as the inner key.
- All remaining keys remain in a filtered `OperatorUri` that preserves `scheme`, `authority`, `name`, `username`, `password`, and `root` verbatim.

Passing the filtered `OperatorUri` to `Configurator::from_uri(&filtered_uri)` preserves every service's URI precedence rules (for example, `S3Config::from_uri` lets the URI host/path override `bucket` and `root`). All existing service configs in `core/services/*/src/config.rs` have been audited and contain no `layers`, `context`, `layer`, or dotted field names, so no existing service key conflicts with this grammar.

### `layers` list syntax

When `layers` is present:

- Split on `,` and trim ASCII whitespace around each token.
- If `layers` is empty or whitespace-only, treat it as an empty layer list (zero user layers).
- If `layers` contains an empty token (e.g. `retry,,timeout` or trailing `,`), return `ErrorKind::ConfigInvalid`.
- Each token is a registered layer `<name>`:
  - Normalized to ASCII lowercase; must be non-empty and contain only `[a-z0-9_-]`.
  - Duplicate names within `layers` (e.g. `layers=timeout,timeout`) return `ErrorKind::ConfigInvalid`.
- For each `<name>` in left-to-right order, look up `<name>` in the registry's layer factories (missing `<name>` returns `ErrorKind::Unsupported`) and invoke it with its owned scoped `HashMap<String, String>` (empty map if no `layer.<name>.*` keys were provided).
- Scoped maps for layer names not present in `layers` are ignored.

## `OperatorRegistry` API and assembly path

### Public API

`opendal-core` adds two methods to `OperatorRegistry` and zero new public types, matching RFC 7740's three planes (`Service`, `OperationContext`, `Layer`):

```rust,ignore
impl OperatorRegistry {
    // Existing APIs remain unchanged:
    pub fn get() -> &'static Self;
    pub fn register<B: Builder>(&self, scheme: &str);
    pub fn schemes(&self) -> HashSet<String>;
    pub fn load(&self, uri: impl IntoOperatorUri) -> Result<Operator>;

    // New APIs:
    /// Register a context factory for `context.<name>.*` options.
    ///
    /// The factory receives the current base [`OperationContext`] and the scoped
    /// options map with the `context.<name>.` prefix stripped. If the options
    /// map contains no keys recognized by this factory, it MUST return `Ok(ctx)`
    /// unchanged so existing base resources are preserved.
    pub fn register_context<F>(&self, name: &str, factory: F)
    where
        F: Fn(OperationContext, HashMap<String, String>) -> Result<OperationContext>
            + Send
            + Sync
            + 'static;

    /// Register a layer factory for `layer.<name>.*` options.
    pub fn register_layer<L, F>(&self, name: &str, factory: F)
    where
        L: Layer,
        F: Fn(HashMap<String, String>) -> Result<L> + Send + Sync + 'static;
}
```

### Assembly sequence and invariants

`OperatorRegistry::load` executes four steps:

1. **Build `base_srv`**: Look up `parsed.scheme()` in the service map (missing returns `ErrorKind::Unsupported` listing available schemes). Internally, the service map stores `fn(&OperatorUri) -> Result<Servicer>` (`C::from_uri(&filtered_uri)?.into_builder().build()`).
2. **Assemble `base_ctx`**: Start from `OperationContext::default()`. For each `(name, scoped_map)` in the partitioned `BTreeMap`:
   - Look up `name` in the context factory map; if absent, return `ErrorKind::Unsupported` with `context` and `available` fields in the error context.
   - Clone the factory `Arc` handle, release the mutex, and invoke `base_ctx = factory(base_ctx, scoped_map)?`.
   - When no `context.*` keys are present, no context factory runs and `OperationContext::default()` is preserved at zero cost.
3. **Construct user layers**: For each `<name>` in `layers` (left to right):
   - Look up `name` in the layer factory map; if absent, return `ErrorKind::Unsupported` with `layer` and `available` fields in the error context.
   - Clone the factory `Arc` handle, release the mutex, and invoke `factory(scoped_map)?` (wrapped as `Arc<dyn Layer>`).
4. **Single-fold assembly**: Call the shared internal constructor used by `Operator::from_builder` (`core/core/src/types/operator/builder.rs`): prepend the core default layers (`ErrorContextLayer`, optional `SimulateLayer` under `auto-simulate`, `GlobLayer`, `CompleteLayer`, `CorrectnessCheckLayer`), append the user layers in declared order, and execute `Operator::apply_layers(&base_srv, &base_ctx, &layers)` once.

Ownership, concurrency, and compatibility invariants:

- **Stateful closures**: Factories accept `Fn(...) + Send + Sync + 'static` closures so host applications can capture CA bundles, shared semaphores, or runtime handles without global mutable statics.
- **Owned scoped maps**: Passing owned `HashMap<String, String>` lets every factory feed the map directly into `ConfigDeserializer::new(map)` with zero extra cloning.
- **Lock discipline**: `OperatorRegistry` never invokes a service, context, or layer factory while holding its internal mutex.
- **Snapshot isolation**: The returned `Operator` owns its `base_srv`, `base_ctx`, and `layers`; later calls to `register*` affect only subsequent `load` calls. Overwrite semantics on `register_context` and `register_layer` match `register<B>`.
- **Replay and capability invariants**: `CorrectnessCheckLayer` remains outermost among core layers so capability validation matches `Operator::new`. Replay history (`base_srv`, `base_ctx`, `layers`) is preserved on the returned `Operator`, so cloning and calling `.with_context(...)` or `.layer(...)` behaves identically to Rust-constructed operators. (`Operator::into_parts` continues to discard replay history as documented.)
- **Existing `OperatorFactory` alias**: `opendal_core::OperatorFactory` (`fn(&OperatorUri) -> Result<Operator>`) is not accepted by any public registration method today; retain the type alias for source compatibility.

## Facade initialization and built-in registrations

`core/src/lib.rs` preserves the distinction between `init_default_registry()` and `install_default()`:

- **`init_default_registry()`**:
  - Registers enabled services (`register<B>`, unchanged).
  - Registers enabled config-constructible built-in layers (`register_layer`).
  - Does **not** register `context.http` or install process-wide defaults. Applications that disable `auto-register-services`, call `init_default_registry()`, and install their own `HttpTransporter::install_default(custom)` will never have `custom` silently replaced by `reqwest`: URIs without `context.http.*` continue using `custom`, and URIs with `context.http.*` fail fast with `ErrorKind::Unsupported` unless the application registers its own `"http"` context factory.
- **`install_default()`**:
  - Calls `init_default_registry()`.
  - Calls `opendal_executor_tokio::install_default()` and `opendal_http_transport_reqwest::install_default()` (unchanged; first installed wins).
  - Registers `"http"` via `OperatorRegistry::get().register_context("http", ...)` when any `http-transport-reqwest*` feature is enabled.

### Built-in `context.http` (`opendal-http-transport-reqwest`)

Portable key conventions for `context.http.*` (parsed inside `opendal-http-transport-reqwest` into a `ReqwestConfig` struct via `ConfigDeserializer`, keeping `opendal-core` free of HTTP client config structs):

| Key | Format & validation | Reqwest mapping |
| --- | --- | --- |
| `proxy` | Non-empty URL validated with `url::Url::parse` (`ConfigInvalid` on empty/malformed; `Unsupported` for schemes requiring disabled features such as `socks5://` without `reqwest/socks`). | `reqwest::Proxy::all(proxy)` |
| `no_proxy` | Non-empty comma-separated host/domain/CIDR bypass list (`ConfigInvalid` on empty). | `reqwest::NoProxy::from_string(no_proxy)` attached to proxy |
| `connect_timeout` | Positive `SignedDuration` (e.g. `500ms`, `5s`; negative or zero returns `ConfigInvalid`). | `ClientBuilder::connect_timeout` |
| `read_timeout` | Positive `SignedDuration` (negative or zero returns `ConfigInvalid`). | `ClientBuilder::read_timeout` |
| `timeout` | Positive `SignedDuration` (negative or zero returns `ConfigInvalid`). | `ClientBuilder::timeout` |
| `user_agent` | Non-empty valid HTTP header value (`ConfigInvalid` otherwise). | `ClientBuilder::user_agent` |

Factory contract for `"http"`:

- **Zero side effects on unknown keys**: After deserializing `ReqwestConfig` from the scoped `HashMap<String, String>`, if all recognized fields are `None` (`reqwest_cfg.is_empty()`), the factory returns `Ok(ctx)` unchanged without building a `reqwest::Client` or replacing `ctx.http_transport()`.
- **Redaction**: `ReqwestConfig` implements a custom `Debug` that redacts any userinfo/credentials embedded in `proxy` (e.g. `http://***:***@proxy.example.com:8080`).

Other runtime crates are intentionally **not** auto-registered under `context.*`:

- `tokio` (`opendal-executor-tokio`): has zero configuration parameters and is already installed by `install_default()`.
- `compio` (`opendal-executor-compio`): `CompioExecutor::new(Arc<Dispatcher>)` requires a host-owned dispatcher that cannot be safely created from a config string.
- `cyper` (`opendal-http-transport-cyper`): requires requests to run on a thread entered into a Compio runtime that outlives response consumption.

### Initial built-in layer factories

Only layers that can be constructed safely from flat string values are registered by default. Every factory MUST validate ranges and required parameters before calling constructors that assert or panic (such as `ThrottleLayer::new` and `ChaosLayer::new`):

| Layer name | Crate / feature | Initial supported options |
| --- | --- | --- |
| `retry` | `layers-retry` | `jitter` (`bool`), `factor` (`f32`), `min_delay` (`SignedDuration`), `max_delay` (`SignedDuration`), `max_times` (`usize`) |
| `timeout` | `layers-timeout` | `timeout` (`SignedDuration`), `io_timeout` (`SignedDuration`) |
| `concurrent_limit` | `layers-concurrent-limit` | `permits` (required, `usize`, > 0), `read_permits`, `write_permits`, `list_permits`, `http_permits` (optional, `usize`, > 0) |
| `throttle` | `layers-throttle` | `bandwidth` (required, `u32`, > 0), `burst` (required, `u32`, > 0) |
| `logging` | `layers-logging` | Default `LoggingLayer::default()` |
| `tracing` | `layers-tracing` | Default `TracingLayer` |
| `mime_guess` | `layers-mime-guess` | Default `MimeGuessLayer` |
| `chaos` | `layers-chaos` | `error_ratio` (required, `f64`, `[0.0, 1.0]`) |

Layers requiring Rust callbacks, caches, or external telemetry handles (`foyer`, `cache`, `metrics`, `prometheus`, `prometheus_client`, `fastmetrics`, `otelmetrics`, `oteltrace`, `fastrace`, `route`, `immutable_index`) are not auto-registered, though Rust applications can register custom factories for them via `OperatorRegistry::register_layer`.

## Cross-language binding contracts

Unified configuration guarantees that **any binding forwarding a URI or `(scheme, map)` to `OperatorRegistry` accepts the same configuration keys** without adding binding-specific FFI APIs:

- **Direct map/URI bindings** (`python`, `nodejs`, `cpp`, `php`, `ruby`, `lua`, `ocaml`, `haskell`, `dart`, `dotnet`): construct operators directly through `Operator::via_iter` / `Operator::from_uri` without post-construction `with_context` replacement. They inherit unified config support automatically.
- **Java (`bindings/java/src/async_operator.rs`)**:
  - Today Java calls `Operator::via_iter(scheme, map)?`, then executes `op.context().with_executor(jni_exec)` and `op.with_context(ctx)`. Passing the already-composed `op.context()` back as `base_ctx` would cause context-wrapping layers (such as `TracingLayer` or `ConcurrentLimitLayer`) to wrap the HTTP transport twice.
  - Fixing Java requires only changing `op.context()` to `op.base_context().clone()` before `.with_executor(jni_exec)` and `op.with_context(ctx)`. Because `op.base_context()` holds the `HttpTransporter` assembled from `context.http.*` and RFC 7740 guarantees `apply_*` hooks are cheap, replayable projections, Java preserves `context.http.*`, installs its JNI executor, and wraps every layer exactly once with zero new core APIs.
- **C (`bindings/c/src/operator.rs`)**:
  - `opendal_operator_new` keeps its current behavior unchanged: it calls `Operator::via_iter(schema, map)` and appends its historical default `RetryLayer::new()` and `LoggingLayer::default()`. Documentation notes that passing `layers=retry` to `opendal_operator_new` stacks an inner configured retry underneath that outer default retry.
  - `opendal_operator_new_with_layers` forwards `(scheme, map)` to `Operator::via_iter` and applies only explicit C layer handles (`NULL` applies none); C callers who want full control over `layers` from options use `opendal_operator_new_with_layers(scheme, options, NULL)`.
- **MoonBit (`bindings/moonbit/native/lib.rs`)**: currently constructs only a hard-coded memory operator; out of scope until it adopts scheme/map construction.

## Failure semantics and security

### Error kinds

All three planes share the same failure model:

| Condition | ErrorKind |
| --- | --- |
| Malformed URI, malformed `layers` syntax, duplicate layer name, invalid or empty `context.<name>.<rest>` / `layer.<name>.<rest>`, negative/zero/unparseable duration, invalid proxy URL, missing required layer option, invalid option type or out-of-range value (e.g. `throttle.bandwidth=0`, `chaos.error_ratio=1.5`) | `ConfigInvalid` |
| Unregistered service `<scheme>`, unregistered `context.<name>`, unregistered layer `<name>` in `layers`, or unsupported option in the registered provider (e.g. `socks5://` proxy without `reqwest/socks`) | `Unsupported` |
| Unknown `<field>` inside a registered service, context slot, or layer, or `layer.<name>.*` for a layer not listed in `layers` | Ignored (zero side effects on `base_ctx`) |

### Security and redaction

- **Trust boundary** (aligned with `SECURITY-THREAT-MODEL.md`): The entire URI and options map passed to `Operator::from_uri`, `Operator::via_iter`, and `OperatorRegistry::load` is trusted operator configuration. Passing a trusted `extra_options` map does not neutralize attacker-controlled `context.http.proxy` or `layer.*` query parameters in an untrusted URI; hosts accepting external object URLs must parse and allowlist only scheme, bucket/host, and path before constructing an operator.
- **Credential redaction**: `OperatorUri::fmt` (`Debug`) replaces its derived `Debug` implementation with a redacting implementation that omits raw `password` and redacts sensitive option values (including `context.http.proxy` userinfo and secret keys). `ReqwestConfig::fmt` (`Debug`) redacts credentials in `proxy`. Error context attached during config parsing must never include raw proxy passwords or secret values.

## Validation

The implementation is verified against these observable acceptance scenarios (contract-derived; no runtime benchmarks are required because unconfigured slots incur zero allocations and configured transports build once at operator construction time):

1. **Zero side effects on unknown HTTP keys**: With `"http"` registered via `install_default()`, loading a URI containing only an unrecognized `context.http.unknown_key=1` leaves `op.base_context().http_transport()` as the default transport (does not construct a new `reqwest::Client`).
2. **Fail fast when `"http"` context is unregistered**: With `auto-register-services` disabled, `init_default_registry()` called, and a custom `HttpTransporter::install_default(custom)` installed (without calling `register_context("http", ...)`), a URI without `context.http.*` dispatches through `custom`, while a URI with `context.http.proxy=http://127.0.0.1:8080` returns `ErrorKind::Unsupported`.
3. **RFC 7740 replay preserved with proxy and layers**: Loading `context.http.proxy=...` together with `layers=concurrent_limit,tracing,retry` routes requests through the proxy while enforcing the HTTP concurrency limit and observing each HTTP request exactly once in `TracingLayer`; subsequently calling `.with_context(op.base_context().clone().with_executor(...))` still observes each HTTP request exactly once.
4. **Layer ordering and validation**: `layers=timeout,retry` places `timeout` inside `retry` (matching `.layer(timeout).layer(retry)`); duplicate layer names (`layers=retry,retry`), empty tokens (`layers=retry,`), unregistered layer or context names (`layers=nonexistent`, `context.unknown.k=v`), and invalid option ranges (`layer.throttle.bandwidth=0`, `layer.chaos.error_ratio=1.5`) return the documented `ConfigInvalid` / `Unsupported` errors without panicking.
5. **Service URI precedence preserved**: `s3://uri-bucket/uri-root?bucket=opt-bucket&context.http.proxy=...` still resolves bucket `uri-bucket` and root `uri-root` via `S3Config::from_uri`.
6. **Binding contracts**: Java `AsyncOperator` constructed with `layers=tracing` and a custom JNI executor wraps HTTP tracing once (not twice) and preserves `context.http.proxy`; C `opendal_operator_new` preserves its existing default retry/logging behavior while `opendal_operator_new_with_layers(..., NULL)` applies only config-declared layers; Python and Node.js configure a per-operator proxy against a local loopback proxy without setting `HTTPS_PROXY`.
7. **Redaction**: Formatting `OperatorUri`, `ReqwestConfig`, and parse errors for `context.http.proxy=http://user:secret@proxy:8080` never emits `secret`.

# Drawbacks

- **Stringly-typed configuration surface**: Typographical mistakes in optional keys inside a registered component (such as `context.http.proxi` or `layer.retry.max_time`) are ignored under OpenDAL's permissive unknown-key model, consistent with existing service configs.
- **Registry maintenance surface**: Built-in layer and transport crates gain small config-parsing adapters and validation tests, though each stays inside its own crate and decoupled from `opendal-core`.
- **Two ways to wire layers**: Rust users now have both typed `.layer(...)` and string `layers=...`; documentation must make clear that statically typed Rust code should continue preferring `.layer(...)`, while unified config targets URI/map-driven construction and cross-language bindings.

# Rationale and alternatives

## Why `register_context` instead of `register_http_transport`?

In RFC 7740, `Operator` and `Layer` interact with runtime resources exclusively through `OperationContext` (`with_context`, `apply_context`), not per-resource methods. Exposing `register_http_transport` on `OperatorRegistry` would leak an internal `OperationContext` slot into the registry API and force a new method for every future runtime resource. `register_context(name, |ctx, cfg| -> Result<OperationContext>)` aligns `OperatorRegistry` 1-to-1 with the three planes (`register`, `register_context`, `register_layer`), lets factories naturally preserve `ctx` by returning `Ok(ctx)` when no recognized key is set, and extends to future context slots with zero API additions.

## Why no `HttpConfig` struct in `opendal-core`?

RFC 7749 removed HTTP client details from `opendal-core`. Defining `pub struct HttpConfig` in `opendal-core` would re-couple core semver to HTTP client options even though `opendal-http-transport-reqwest` is currently the only HTTP transport constructible from flat strings. Letting each context provider deserialize its own scoped `HashMap<String, String>` via `ConfigDeserializer` mirrors how services parse `S3Config` / `FsConfig`, avoids `#[serde(flatten)]` incompatibilities with `ConfigDeserializer`, and keeps `opendal-core` at zero new public types while documenting the `context.http.*` key names as the portable convention.

## Why omit `load_with_context`, `register_executor`, and registry listing methods?

- **`load_with_context`**: Existing `Operator::base_context()` + `Operator::with_context()` already covers Java (by fixing `op.context()` to `op.base_context()`) and per-operator Rust overrides; `HttpTransporter::install_default` covers process-wide custom transports. Deferring `load_with_context` avoids adding a second load entry point until a non-global conditional-override use case appears.
- **`register_executor`**: Neither built-in executor benefits from string configuration today (`tokio` has zero options and is already default; `compio` requires a host-owned `Arc<Dispatcher>`). `register_context("executor", ...)` can plug in later with zero API changes if a string-configurable executor is added.
- **Registry listing methods (`contexts()`, `layers()`) and factory type aliases**: `register_context` and `register_layer` are generic over `Fn`, so public `Arc<dyn Fn(...)>` aliases are unnecessary; `Unsupported` errors already include the available names in error context.

## Why not `HttpConfigLayer` (RFC PR #8526)?

Implementing HTTP configuration as a `Layer` that replaces `HttpTransporter` in `apply_context`:

- Breaks RFC 7740's order-independence guarantee for base resources: applying `HttpConfigLayer` after `ConcurrentLimitLayer` or `TracingLayer` silently drops their HTTP wrappers.
- Silently overrides an explicit `Operator::with_context(...)` on the operator.
- Solves only HTTP transport options while still requiring per-binding FFI boilerplate for `HttpConfigLayer` and every future context option or layer.

## Why not per-binding `HttpConfig` APIs?

Adding an explicit `http_config` parameter to every binding's constructor preserves RFC 7740 replay order, but:

- Requires designing, maintaining, and releasing parallel APIs across 10+ language bindings.
- Leaves `Layer` composition and future context resources unaddressed, forcing another round of binding-specific APIs later.
- Still does not let URI-driven callers (`Operator::from_uri`) configure proxies or layers from a single configuration string.

## Why `context.<name>.*` and `layer.<name>.*` instead of `http.*` and `<layer>.*`?

- Flat top-level prefixes like `http.*` or `retry.*` claim a new service-key prefix every time OpenDAL adds a layer or context slot, risking collisions with future service options.
- Scoping under `context.<name>.*` and `layer.<name>.*` reserves only two namespace roots (`context.*`, `layer.*`) plus `layers`, mirrors RFC 7740's exact three-plane model, and accommodates future `OperationContext` slots cleanly.

# Prior art

- **RFC 5444 (`operator_from_uri`)**: Introduced `OperatorUri` and `OperatorRegistry` for scheme-driven service construction. This RFC extends the same registry from service-only to full `Operator` assembly.
- **RFC 7740 (`operator_composition`)**: Established the three-plane `(base_srv, base_ctx, layers)` model and single-fold replay invariant that this RFC preserves.
- **RFC 7749 (`http_transporter`)**: Decoupled `opendal-core` from `reqwest` and introduced `install_default()` in the facade crate.
- **JDBC / Go database/sql / Arrow Flight / SQLAlchemy connection URIs**: Configure both endpoint parameters and transport/timeout options in one connection URI or flat options map.

# Unresolved questions

1. **Should `Operator::from_iter::<B>` stay service-only or participate in unified config assembly?**
   - *Recommendation*: Keep `Operator::from_iter::<B>` as the statically typed service-config-only entry point (matching `Operator::from_config` and `Operator::new`) and route unified assembly through `OperatorRegistry::load` / `Operator::from_uri` / `Operator::via_iter`.
   - *Decisive reason*: `Operator::from_iter::<B>` currently calls `B::Config::from_iter(iter)` directly without touching `OperatorRegistry`, and `Configurator::from_uri` defaults to `Unsupported`. Mixing registry-driven context/layers into a typed, registry-free constructor blurs the boundary between static and registry-driven construction.
   - *To close*: Maintainer confirmation on whether any Rust caller expects `Operator::from_iter::<S3>(map)` to honor `context.http.*` / `layers`.

# Future possibilities

- **Multi-instance layer aliases (`kind:alias`)**: Because `:` is disallowed in layer names (`[a-z0-9_-]`), supporting multiple instances of the same layer in one config can be added backward-compatibly if a real use case emerges.
- **Additional `OperationContext` slots**: Future runtime resources added to `OperationContext` (such as configurable executors, DNS resolvers, TLS roots, or clocks) can plug directly into `OperatorRegistry::register_context("<slot>", ...)` without adding any new registry APIs.
- **Additional HTTP options**: Custom CA paths (`ca_cert_path`), HTTP/2 controls, or pool tuning can be added to `context.http.*` without changing `opendal-core`.
- **Conditional base-context loading (`OperatorRegistry::load_with_context`)**: Can be added backward-compatibly if callers need a non-global base `OperationContext` overridden only when `context.*` keys appear in a URI.
- **Structured config ingestion (TOML / JSON / YAML)**: A dedicated follow-up RFC can define deterministic flattening from nested tables (`[service]`, `[context.http]`, `[[layers]]`) into this contract once the flat semantics are stable.
- **Python dict ergonomics**: Allowing `opendal.Operator("s3", {"bucket": ..., "context.http.proxy": ...})` as a positional options mapping alongside `**kwargs` avoids `**{...}` unpacking for dotted keys.
