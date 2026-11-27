# opendal-http-transport-reqwest

Reqwest-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

This crate provides `ReqwestTransport`, an implementation of OpenDAL's
`HttpTransport` trait backed by [reqwest](https://crates.io/crates/reqwest).

## TLS configuration

OpenDAL does not add another TLS backend selector on top of reqwest. Pick the
Cargo feature set you need, build a `reqwest::Client` with reqwest's
`ClientBuilder`, and pass that client to `ReqwestTransport::new`.

The default feature is `rustls`. Both `ReqwestTransport::default()` and the
`context.http.*` factory use reqwest's automatic TLS backend selection.

If this crate is built with only `rustls-no-provider`, install a Rustls default
crypto provider before using `ReqwestTransport::default()` or loading an
operator that builds a client from `context.http.*` options. You can also pass a
preconfigured `reqwest::Client` to `ReqwestTransport::new`.

### Reqwest TLS resolution

Reqwest has its own TLS resolution rules. For OpenDAL users, the practical
resolution order is:

- Explicit reqwest client configuration wins. Call
  `ClientBuilder::tls_backend_rustls()`,
  `ClientBuilder::tls_backend_native()`, or
  `ClientBuilder::tls_backend_preconfigured()`, but reqwest documents that API
  as semver-unstable because the concrete TLS config type must match reqwest's
  dependency versions.
- Otherwise, reqwest's automatic TLS backend selection applies. The reqwest
  `default-tls` feature takes precedence if any crate in the dependency tree
  enables it. Disable reqwest defaults with `default-features = false` when you
  need deterministic TLS features.
- Cargo features are additive. If multiple TLS backends are compiled by feature
  unification, use explicit client configuration instead of relying on automatic
  selection.

See reqwest's TLS documentation:

- [reqwest TLS module](https://docs.rs/reqwest/latest/reqwest/tls/index.html)
- [ClientBuilder::tls_backend_rustls](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.tls_backend_rustls)
- [ClientBuilder::tls_backend_native](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.tls_backend_native)
- [ClientBuilder::tls_backend_preconfigured](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.tls_backend_preconfigured)

### Feature matrix

| Feature | Crypto provider | Certificate roots | Use when |
|---------|-----------------|-------------------|----------|
| `rustls` (default) | reqwest default | Platform verifier | Pure-Rust TLS with the system trust store |
| `rustls-no-provider` | You provide | You provide | BYO crypto provider, roots, or FIPS module |
| `native-tls` | OS library | OS trust store | You want platform TLS |

## Cargo features

Most users depend on the `opendal` facade crate:

```toml
[dependencies]
# Default: reqwest transport with rustls.
opendal = { version = "0.58" }
```

To select one transport TLS feature explicitly, disable default features:

```toml
[dependencies]
opendal = { version = "0.58", default-features = false, features = [
    "http-transport-reqwest-rustls",
] }
```

## Configure HTTP per operator

Use `context.http.*` options with [`Operator::from_uri`] or
[`Operator::via_iter`] to set a proxy, timeout, or user agent on an individual
operator. On Wasm, only `user_agent` is supported, and the host Fetch API
controls whether it is sent.

The facade's `opendal::install_default()` registers the provider when a
`http-transport-reqwest*` feature is enabled. The default
`auto-register-services` feature uses a startup constructor to call it. Call
`opendal::install_default()` explicitly before loading operators if you
turn off this feature or your linker or target skips the constructor, as
can happen with `staticlib` builds.

With the split crates, call [`register_http_context`] with
[`OperatorRegistry::get`] before loading the operator. The operator's
configured layers wrap its resulting base context.

[`Operator::from_uri`]: https://opendal.apache.org/docs/rust/opendal_core/struct.Operator.html#method.from_uri
[`Operator::via_iter`]: https://opendal.apache.org/docs/rust/opendal_core/struct.Operator.html#method.via_iter
[`register_http_context`]: https://opendal.apache.org/docs/rust/opendal_http_transport_reqwest/fn.register_http_context.html
[`OperatorRegistry::get`]: https://opendal.apache.org/docs/rust/opendal_core/struct.OperatorRegistry.html#method.get

## Use with `opendal-core`

Applications that use the split crates can attach reqwest to an operator
without installing a process-wide default transport:

```shell
cargo add opendal-core opendal-http-transport-reqwest
```

```rust
use opendal_core::Builder;
use opendal_core::HttpTransporter;
use opendal_core::OperationContext;
use opendal_core::Operator;
use opendal_core::Result;
use opendal_http_transport_reqwest::ReqwestTransport;

fn build_operator<B: Builder>(builder: B) -> Result<Operator> {
    let transport = HttpTransporter::new(ReqwestTransport::default());
    let context = OperationContext::new().with_http_transport(transport);

    Ok(Operator::new(builder)?.with_context(context))
}
```

Pass a configured service builder to `build_operator`. The resulting operator
uses this reqwest transport for HTTP-backed services.

## Build different TLS backends

### Rustls

Use reqwest's Rustls backend explicitly when you build the client:

```toml
[dependencies]
opendal = { version = "0.58", default-features = false, features = [
    "http-transport-reqwest-rustls",
] }
opendal-http-transport-reqwest = { version = "0.58", default-features = false }
reqwest = { version = "0.13.4", default-features = false, features = [
    "rustls",
    "stream",
] }
```

```rust,ignore
use std::time::Duration;

use opendal::HttpTransporter;
use opendal_http_transport_reqwest::ReqwestTransport;

fn build_transport() -> Result<HttpTransporter, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .tls_backend_rustls()
        .connect_timeout(Duration::from_secs(10))
        .build()?;

    Ok(HttpTransporter::new(ReqwestTransport::new(client)))
}
```

### Native TLS

Use `native-tls` when you want reqwest to delegate TLS to the platform stack:

```toml
[dependencies]
opendal = { version = "0.58", default-features = false, features = [
    "http-transport-reqwest-native-tls",
] }
opendal-http-transport-reqwest = { version = "0.58", default-features = false }
reqwest = { version = "0.13.4", default-features = false, features = [
    "native-tls",
    "stream",
] }
```

```rust,ignore
use opendal::HttpTransporter;
use opendal_http_transport_reqwest::ReqwestTransport;

fn build_transport() -> Result<HttpTransporter, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .tls_backend_native()
        .build()?;

    Ok(HttpTransporter::new(ReqwestTransport::new(client)))
}
```

### rustls-no-provider

Use `rustls-no-provider` when the application owns the Rustls provider and
certificate roots. Install a Rustls crypto provider before building the client
configuration. This example uses Mozilla roots from `webpki-roots`.

```toml
[dependencies]
opendal = { version = "0.58", default-features = false, features = [
    "http-transport-reqwest-rustls-no-provider",
] }
opendal-http-transport-reqwest = { version = "0.58", default-features = false }
reqwest = { version = "0.13.4", default-features = false, features = [
    "rustls-no-provider",
    "stream",
] }
rustls = { version = "0.23", default-features = false, features = ["std"] }
webpki-roots = "1"
```

```rust,ignore
use opendal::HttpTransporter;
use opendal_http_transport_reqwest::ReqwestTransport;

fn build_transport() -> Result<HttpTransporter, Box<dyn std::error::Error>> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(tls_config)
        .build()?;

    Ok(HttpTransporter::new(ReqwestTransport::new(client)))
}
```

### FIPS TLS

Enable `http-transport-reqwest-rustls-no-provider` with OpenDAL's default
features disabled, and enable `fips` on Rustls 0.23 in your application. If you
depend directly on `opendal-http-transport-reqwest` to construct a
`ReqwestTransport`, disable its default features as well. Cargo features are
additive across the dependency graph.

The [standalone FIPS TLS example](https://github.com/apache/opendal/tree/main/examples/rust/fips-tls)
installs `rustls::crypto::default_fips_provider()`, builds a client configuration
with Mozilla roots, and asserts that `ClientConfig::fips()` returns `true`.
It passes that configuration to reqwest and uses the resulting transport for
an OpenDAL HTTPS read. Run the example from the repository root with network
access to `www.iana.org` and the [AWS-LC FIPS build prerequisites](https://github.com/aws/aws-lc-rs/blob/main/aws-lc-fips-sys/README.md#build-prerequisites).

```shell
cargo run --locked --manifest-path examples/rust/fips-tls/Cargo.toml
```

The example covers TLS only. Rustls's `fips` feature can include `aws-lc-sys`
in the dependency graph even though the provider used here selects
`aws-lc-fips-sys`. Verify the cryptographic module and version, target platform,
build configuration, and approved operating mode for your deployment. OpenDAL's
S3, Azure Blob Storage, and GCS request signing still uses Reqsign 3.x, and
`opendal-core` still depends on `md-5`. See [#7592](https://github.com/apache/opendal/issues/7592)
for crate-wide FIPS readiness.

## WASM targets

On `wasm32-unknown-unknown` and `wasm32-none`, reqwest switches to its WASM
client implementation. It uses the browser or host Fetch API instead of hyper.
TLS is provided by the browser or host environment, so the effective choice is
the system TLS stack exposed to that environment.

This means `native-tls`, `rustls`, `rustls-no-provider`, custom root stores,
and custom Rustls crypto providers do not select the TLS implementation for
wasm requests. See [reqwest's WASM documentation](https://docs.rs/reqwest/latest/reqwest/#wasm)
for the target-specific limitations.

## License and Trademarks

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
