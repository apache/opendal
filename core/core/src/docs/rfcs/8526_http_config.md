- Proposal Name: `http_config`
- Start Date: 2026-11-18
- RFC PR: [apache/opendal#8526](https://github.com/apache/opendal/pull/8526)
- Tracking Issue: [apache/opendal#0000](https://github.com/apache/opendal/issues/0000)

# Summary

Expose HTTP transport configuration to every OpenDAL user through a layer.
`opendal-core` gains a portable `HttpConfig` value and a `HttpTransport::configure`
hook; a new `HttpConfigLayer` applies a `HttpConfig` to an operator by asking the
installed HTTP transport to build a configured one. Every binding that already
exposes layers exposes `HttpConfigLayer` the same way it exposes `RetryLayer` and
`TimeoutLayer`.

Service configuration is untouched: no new config keys, no change to how
`Configurator` parses or validates options, and no change to `OperatorUri`.

# Motivation

The Python binding cannot configure the HTTP client of an operator. A user who
needs a proxy for one S3 operator has to set `HTTPS_PROXY` for the whole process
([#6871](https://github.com/apache/opendal/issues/6871)), which changes the
network path of every other operator, every other library, and every other thread
in that process.

The gap is not Python-specific:

- Every binding constructs operators from a flat `HashMap<String, String>` via
  `Operator::via_iter` or `Operator::from_uri`. None of them can build an
  `HttpTransporter`.
- Rust users can replace the transport with
  `Operator::with_context(op.context().with_http_transport(transport))` and
  `ReqwestTransport::new(reqwest::Client)`. Bindings have no equivalent: a
  `reqwest::Client` is a Rust type and cannot cross the FFI boundary.
- Environment variables are the only fallback. They are process-wide and are
  read by reqwest, not by OpenDAL, so OpenDAL cannot scope them to one operator.

`TimeoutLayer` does not cover the same need. It bounds a whole operation,
including retries and backoff. It cannot set a proxy, a connection timeout, a
per-request deadline, or a user agent.

Layers are already the mechanism OpenDAL uses to expose operator behavior to
bindings: `RetryLayer`, `TimeoutLayer`, `ConcurrentLimitLayer`, and
`CapabilityOverrideLayer` exist in Python, Java, Node.js, Ruby (as middlewares),
.NET, C, and C++. HTTP transport configuration belongs in the same place, because
that is where bindings can reach it, and because `Layer::apply_context` already
receives the `OperationContext` that owns the transport: `ConcurrentLimitLayer`
already wraps `inner.http_transport()` there.

This RFC deliberately does not add configuration keys. Transport options are not
service options: putting them in the flat map would give one namespace two sets
of parsing and validation rules, and it would change the semantics of service
configuration for a feature that is not about services.

Expected outcome:

- An operator can be given its own proxy, timeouts, and user agent from any
  binding that supports layers, at operator construction time.
- The option vocabulary is defined once in `opendal-core` and is identical in
  every language.
- Options that cannot be honored fail when the layer is created instead of being
  ignored.
- Nothing about service configuration, `Configurator`, or `OperatorUri` changes.

# Guide-level explanation

Users configure the transport of an operator by applying a layer:

```python
import datetime
import opendal

op = (
    opendal.Operator("s3", bucket="my-bucket", region="us-east-1")
    .layer(
        opendal.layers.HttpConfigLayer(
            proxy="http://127.0.0.1:7890",
            no_proxy="169.254.169.254,.internal.example.com",
            connect_timeout=datetime.timedelta(seconds=5),
            timeout=datetime.timedelta(seconds=30),
        )
    )
)
```

The asynchronous operator takes the same layer, and the Rust API is the same
concept:

```rust
use opendal::HttpConfig;
use opendal::layers::HttpConfigLayer;
use opendal::raw::SignedDuration;

let op = Operator::new(S3::default().bucket("my-bucket"))?
    .layer(HttpConfigLayer::new(HttpConfig {
        proxy: Some("http://127.0.0.1:7890".to_string()),
        no_proxy: Some("169.254.169.254,.internal.example.com".to_string()),
        connect_timeout: Some(SignedDuration::from_secs(5)),
        timeout: Some(SignedDuration::from_secs(30)),
        ..Default::default()
    })?);
```

Users should think about `HttpConfig` as "how this operator talks HTTP", not as
service configuration:

- Options apply to the operator, not to the process. Two operators in the same
  process can use different proxies, or one proxy and one direct connection.
- Setting `proxy` replaces environment proxy variables for that operator.
  Without an `HttpConfigLayer`, request routing is unchanged and reqwest keeps
  honoring `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` / `NO_PROXY`.
- `no_proxy` narrows the proxy to selected hosts. It is needed, for example, when
  the proxy must not intercept credential requests to `169.254.169.254` (EC2
  instance metadata) or to a container credential endpoint. It requires `proxy`:
  a config with `no_proxy` but no `proxy` is rejected when the layer is created,
  because without an explicit proxy the client keeps its environment proxy
  settings and reads `NO_PROXY` from there.
- `timeout` bounds a single HTTP request. `TimeoutLayer` bounds a whole
  operation, which may span several requests and retries. They compose: with
  `RetryLayer` and `TimeoutLayer`, each retry attempt gets its own `timeout`.
- The layer replaces the operator's transport. Because replacement discards
  whatever the operator had, layer order matters: add `HttpConfigLayer` before
  layers that wrap the transport, such as `ConcurrentLimitLayer`, so those
  wrappers wrap the configured transport. Added after a wrapping layer, the
  configured transport replaces the wrapper and silently drops it.

# Reference-level explanation

## `HttpConfig`

`opendal_core::types::http_transport::config` adds a portable value type,
exported as `opendal_core::HttpConfig` and `opendal::HttpConfig`:

```rust
#[derive(Clone, Default)]
pub struct HttpConfig {
    pub proxy: Option<String>,
    pub no_proxy: Option<String>,
    pub connect_timeout: Option<SignedDuration>,
    pub read_timeout: Option<SignedDuration>,
    pub timeout: Option<SignedDuration>,
    pub user_agent: Option<String>,
}
```

Fields are public and the struct is not `#[non_exhaustive]`, so Rust users write
struct literals with `..Default::default()` and keep compiling when options are
added later. `Debug` is implemented manually to mask userinfo in `proxy`, the way
OpenDAL treats other credential-bearing values. `SignedDuration` is the duration
type service configs already use, so `5s`, `1500ms`, and `PT5S` all parse and
sub-second values are expressible.

`HttpConfig` is a plain value: it does not parse text, and it is not a
`Configurator`. Parsing it from a language-specific representation is each
binding's job, which keeps this RFC out of the configuration system.

## Building a transport from `HttpConfig`

`opendal-core` must not depend on reqwest
([RFC 7749](./7749_http_transporter.md)). The transport implementation builds the
configured transport through a new provided method on the existing trait:

```rust
pub trait HttpTransport: Send + Sync + Unpin + 'static {
    fn fetch(&self, req: Request<Buffer>) -> impl Future<Output = Result<Response<HttpBody>>> + Send;

    /// Build a transport that applies portable [`HttpConfig`] options.
    ///
    /// The default implementation returns `Unsupported` so a transport that
    /// cannot honor the options fails loudly instead of ignoring them.
    fn configure(&self, config: &HttpConfig) -> Result<HttpTransporter> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "http transport does not support configuration",
        ))
    }
}
```

`configure` is provided, so existing transport implementations keep compiling.
The private erased adapter mirrors it:

```rust
pub(crate) trait HttpTransportDyn: Send + Sync + Unpin + 'static {
    fn fetch_dyn(&self, req: Request<Buffer>) -> BoxedFuture<'_, Result<Response<HttpBody>>>;
    fn configure_dyn(&self, config: &HttpConfig) -> Result<HttpTransporter>;
}
```

`HttpTransporter` gains the entry point that reuses the installed default
transport, exactly like `fetch` does:

```rust
impl HttpTransporter {
    /// Build a transport from `config` using the installed default transport.
    pub fn from_config(config: &HttpConfig) -> Result<HttpTransporter>;
}
```

`DefaultHttpTransport::configure` resolves the installed default and delegates,
mirroring `fetch_with_default_transport`. When nothing is installed it returns
the same `ConfigInvalid` "default HTTP transport is not installed" error that a
request would produce.

## `HttpConfigLayer`

A new crate `core/layers/http-config/` (`opendal-layer-http-config`, feature
`layers-http-config`) implements the layer:

```rust
pub struct HttpConfigLayer {
    config: HttpConfig,
    transport: HttpTransporter,
}

impl HttpConfigLayer {
    /// Resolve the configured transport now so invalid options fail here,
    /// not on the first request.
    pub fn new(config: HttpConfig) -> Result<Self>;

    pub fn config(&self) -> &HttpConfig;
}

impl Layer for HttpConfigLayer {
    fn apply_context(&self, _srv: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_http_transport(self.transport.clone())
    }
}
```

`Layer::apply_context` cannot return an error, so configuration is resolved in
`new`. That makes the layer fallible to construct and infallible to apply. The
precedent is the bindings, not Rust: `TimeoutLayer` itself takes
`std::time::Duration` and cannot fail, and the bindings' layer wrappers validate
options when the layer object is created — for example the Python `TimeoutLayer`
rejects non-positive or non-finite seconds with `ConfigInvalid` at construction.
The layer stores an `HttpTransporter`, so applying it is a handle clone, not a
client rebuild, and an operator that copies the layer does not create a new
connection pool per copy.

The layer sets the operator's transport rather than wrapping whatever transport
the operator already had. `ConcurrentLimitLayer` is the existing precedent for a
layer that rewrites the transport slot in `apply_context`; that layer wraps
because its job is to forward traffic, while a proxy, connect timeout, or TLS
option takes effect when the connection is established, so the configured client
must own the connections and cannot forward to a transport built from different
client options. Rust users who own a custom transport call `HttpTransport::configure`
themselves instead of using the layer; that is also why `configure` lives on the
transport trait rather than inside the layer.

Replace semantics interact with layer replay
([RFC 7749](./7749_http_transporter.md)): both `Operator::layer` and
`Operator::with_context` replay every layer over the composed context, so this
layer replaces the transport on every replay. Two consequences follow, and the
rustdoc of `HttpConfigLayer` states both:

- The configured transport wins over an explicit context replacement. With this
  layer applied, `op.with_context(ctx.with_http_transport(custom))` re-applies
  the layer during replay and replaces `custom` again. A user who owns a custom
  transport must not use the layer and calls `HttpTransport::configure` directly.
- The configured transport wins over transport wrappers applied before it in the
  layer list. `op.layer(HttpConfigLayer).layer(ConcurrentLimitLayer)` composes:
  the concurrent limit wraps the configured transport. The reverse order makes
  the configured transport replace the wrapper and silently drops the limit.

Wrapping instead of replacing was rejected for this RFC. A wrapper can only
decorate requests, while proxy, connect timeout, and TLS options change how
connections are established and must therefore be owned by the client itself.

## Request-level mapping

`opendal-http-transport-reqwest` implements `configure` on `ReqwestTransport`:

```rust
fn configure(&self, config: &HttpConfig) -> Result<HttpTransporter> {
    let mut builder = reqwest::Client::builder();

    if let Some(proxy) = &config.proxy {
        let mut proxy = reqwest::Proxy::all(proxy).map_err(invalid_proxy)?;
        if let Some(no_proxy) = &config.no_proxy {
            proxy = proxy.no_proxy(reqwest::NoProxy::from_string(no_proxy));
        }
        builder = builder.proxy(proxy);
    }
    if let Some(v) = config.connect_timeout {
        builder = builder.connect_timeout(signed_duration_to_duration(v)?);
    }
    // read_timeout, timeout, user_agent

    Ok(HttpTransporter::new(Self::new(
        builder.build().map_err(invalid_client)?,
    )))
}
```

Notes on the mapping:

- `reqwest::Error` values are mapped to `ConfigInvalid` with the reqwest error as
  the source and the offending option as context, so a bad proxy URL or a proxy
  scheme the build does not support reads as a configuration error rather than a
  transport failure. Values echoed into error context are masked first: a `proxy`
  URL is rendered without userinfo, the same redaction `Debug` applies, and the
  reqwest source is recorded with `without_url()`, as the transport already does
  for fetch errors.
- The configured transport is a new `reqwest::Client` built from
  `Client::builder()` defaults. It does not inherit from the process-wide default
  client and does not share its connection pool.
- `ClientBuilder::proxy` disables reqwest's automatic environment proxy for that
  client, which is the requested per-operator semantic. `no_proxy` is applied with
  `Proxy::no_proxy`, so an environment `NO_PROXY` is replaced rather than merged.
  A config with `no_proxy` but no `proxy` is rejected in `HttpConfigLayer::new`
  before any transport is consulted: without an explicit proxy the client keeps
  its environment proxy settings, so the option cannot take effect and must fail
  instead of being ignored.
- The facade installs `LazyReqwestTransport` as the process-wide default. Its
  `configure` delegates to `ReqwestTransport::configure` on the lazy-initialized
  default `ReqwestTransport` inside `opendal-http-transport-reqwest` — the same
  client its `fetch` already uses. The delegation is dispatch only: the new client
  is still built from `Client::builder()` defaults and shares no state with the
  default client. Dispatching through the reqwest crate's own lazy static, rather
  than `DEFAULT_HTTP_TRANSPORTER`, also keeps the call from recursing into
  `DefaultHttpTransport`.
- Credential requests use the operator transport: `reqsign_core::HttpSend` is
  implemented for `HttpTransporter`, so IMDS, container credential, and STS
  requests issued while signing also take the proxy. Users exclude those
  endpoints with `no_proxy`.

## Errors

All failures happen when the layer is created (`HttpConfigLayer::new`, or
`opendal.layers.HttpConfigLayer(...)` in bindings), before any request:

| situation | kind |
| --- | --- |
| no default transport installed | `ConfigInvalid` |
| malformed duration, or a proxy scheme this build cannot use | `ConfigInvalid` |
| `no_proxy` set without `proxy` | `ConfigInvalid` |
| installed transport returns the default `configure` | `Unsupported` |

No error path is added to `Operator`, `Configurator`, `OperatorUri`, or layer
application.

## Bindings

Each binding wraps the same layer, mirroring its existing layer bindings:

- Python: `opendal.layers.HttpConfigLayer(proxy=..., no_proxy=...,
  connect_timeout=..., read_timeout=..., timeout=..., user_agent=...)` in
  `bindings/python/src/layers/http_config.rs`, registered in the flat
  `opendal.layers` module next to `TimeoutLayer`. Durations accept
  `datetime.timedelta`, which pyo3 already converts to `SignedDuration` through
  the enabled `jiff-02` feature.
- Java, Node.js, Ruby, .NET, C, and C++ expose the same constructor through their
  existing layer files (Ruby names the same concept `middlewares.rs`).
- Every binding that exposes the layer enables the `layers-http-config` feature
  next to the `layers-*` features it already enables.

## Documentation and testing

- rustdoc for `HttpConfig`, `HttpTransport::configure`, and `HttpConfigLayer`,
  including the "replaces environment proxy", "replaces the operator transport",
  and layer-ordering notes; `core/core/src/docs/concepts.rs` gains a short pointer next
  to the transport description.
- `HttpConfig` unit tests: empty config, `Debug` redaction, duration conversion.
- `HttpConfigLayer` tests: `new` fails with `Unsupported` when the installed
  transport does not implement `configure`, fails with `ConfigInvalid` when no
  transport is installed, rejects `no_proxy` without `proxy` with `ConfigInvalid`,
  and succeeds with the reqwest transport.
- `opendal-http-transport-reqwest` test: start a TCP listener that acts as an
  HTTP proxy, build a transport with `proxy: Some(local)`, send a request to a
  non-local address, and assert the proxy received the absolute-URI request. This
  is the only way to observe that the option was honored.
- Python tests: an operator with `HttpConfigLayer` builds and performs a request
  through a local test proxy; an invalid proxy URL raises `ConfigInvalid`.

# Drawbacks

Every binding needs a thin wrapper class and a feature flag. The feature is not
free for a new binding, although the work is the same shape as the existing
`TimeoutLayer` and `RetryLayer` wrappers.

The layer replaces the operator's transport instead of wrapping it. A Rust user
who installs a custom transport and then applies `HttpConfigLayer` loses the
custom transport — a later `Operator::with_context` cannot restore it, and a
transport-wrapping layer applied earlier in the layer list is dropped the same
way; the migration is to call `HttpTransport::configure` directly.

The configured transport is independent of the process-wide default transport: it
has its own connection pool and TLS session state, so an application that mixes
operators with different proxies uses more connections.

Users must translate `NO_PROXY` settings into `no_proxy` for a configured
operator, because setting an explicit proxy opts that client out of environment
proxy configuration including its exclusions. Proxy URLs may carry credentials,
which then travel through binding objects and whatever the application logs.

A custom `HttpTransport` that does not implement `configure` makes the layer
unusable for that operator. The error is explicit, but the options cannot be
honored.

# Rationale and alternatives

The layer is chosen because it is the only mechanism that already reaches every
binding. `Operator.layer` exists in Python, Java, Node.js, Ruby, .NET, C, and C++,
and `Layer::apply_context` already receives the transport slot that needs to
change. No new construction path is introduced for bindings to adopt.

Adding `http_*` keys to the flat service configuration was considered and
rejected for this RFC. It would give one key-value namespace two sets of parsing
and validation rules, and it would change configuration behavior — reserved keys,
unknown-key handling, and `OperatorUri::options` semantics — for a feature that
is about the transport, not about services. Any future decision to route
cross-cutting options through configuration is a separate question.

Adding transport fields to each service config (`S3Config::proxy`, ...) was
rejected for the same reason plus duplication: the more than thirty service
crates that call `ctx.http_transport()` would repeat the same fields, every
binding would regenerate typed configs per service, and a service still cannot set
its own `OperationContext`.

Handing `reqwest::Client` to bindings is not possible across an FFI boundary, and a
language-specific client abstraction per binding would multiply maintenance and
diverge the semantics.

Passing `HttpConfig` through request extensions so any transport can read it from
`http::Request` avoids `configure`, but it makes unsupported options silent (the
request just goes out unproxied) and turns per-request metadata into a
configuration channel.

Putting `HttpConfig` in a new constructor parameter on every binding's operator
constructor was rejected because it grows seven unrelated constructor surfaces,
while layers already exist and compose with `Operator.layer`.

The chosen design keeps the layering of [RFC 7749](./7749_http_transporter.md):
`opendal-core` owns the portable vocabulary and the transport hook, the installed
transport owns the implementation, the layer applies the result, and bindings only
wrap the layer.

# Prior art

boto3 is the closest analogue. It exposes per-client transport options in
`botocore.config.Config` (`proxies`, `connect_timeout`, `read_timeout`,
`ca_bundle`, `max_pool_connections`), because a Python program cannot hand the
client its own `http.Client` object the way a Go program hands over an
`*http.Client`.

AWS SDK for Go v2 solves the same problem from the other side with
`config.WithHTTPClient`, which replaces the client wholesale. OpenDAL keeps the
equivalent mechanism for Rust (`OperationContext::with_http_transport`) and adds a
declarative form for languages that cannot build the client.

reqwest's own proxy handling is the prior art for the semantics: explicit
`ClientBuilder::proxy` disables the system proxy, `NoProxy` provides host
exclusions, and `connect_timeout`, `read_timeout`, and `timeout` bound different
stages of a request.

Inside OpenDAL, `TimeoutLayer`'s `timeout`/`io_timeout` split is the existing
precedent for naming separate deadlines, and `ConcurrentLimitLayer` and
`TimeoutLayer` are the existing precedents for a layer that replaces a runtime
resource in `apply_context` instead of wrapping the operation service.

# Unresolved questions

- Naming: `HttpConfigLayer` matches the value type, while `HttpTransportLayer` or
  `HttpClientLayer` would describe the thing being configured. Which name should
  the cross-language API use?
- Are TLS trust options (a custom CA bundle, skipping verification) part of the
  first implementation? They interact with the reqwest TLS feature matrix
  (`native-tls` versus `rustls`) and are deliberately absent from `HttpConfig`.
- Python durations: `datetime.timedelta` is what pyo3 converts for free, while
  `TimeoutLayer` takes seconds as a float. Which one should `HttpConfigLayer`
  accept, or both?
- PHP and Lua have no layer surface today. Do they get one for this, or ship the
  feature later?

# Future possibilities

The transport option set can grow without new mechanisms: pool sizing, keep-alive
and HTTP/2 preference, TLS trust material, and SOCKS proxies once the reqwest
transport enables that feature. Each addition is a field on `HttpConfig` plus a
mapping in the transport.

`configure` could report which options a transport honors, so a transport can
reject only the options it cannot apply instead of failing the whole config.

Because `HttpConfigLayer` applies an `OperationContext` change through the layer
mechanism, a future routing layer could choose a different `HttpConfig` per bucket
or prefix, making per-destination proxy policies expressible without splitting
operators.

Whether other operator components (executor, retry, and so on) should get the same
treatment is an open question; this RFC answers it for HTTP only.
