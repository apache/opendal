# opendal-http-transport-reqwest

Reqwest-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

This crate provides `ReqwestTransport`, an implementation of OpenDAL's
`HttpTransport` trait backed by [reqwest](https://crates.io/crates/reqwest).

## TLS configuration

OpenDAL does not add another TLS backend selector on top of reqwest. Pick the
Cargo feature set you need, build a `reqwest::Client` with reqwest's
`ClientBuilder`, and pass that client to `ReqwestTransport::new`.

The default feature is `rustls`. When `ReqwestTransport::default()` is used on
native targets, this crate explicitly asks reqwest for the Rustls backend if the
`rustls` feature is compiled in.

If this crate is built with only `rustls-no-provider`, install a Rustls default
crypto provider before using `ReqwestTransport::default()`, or build a
preconfigured `reqwest::Client` yourself and pass it to `ReqwestTransport::new`.

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
opendal = { version = "0.57" }
```

To select one transport TLS feature explicitly, disable default features:

```toml
[dependencies]
opendal = { version = "0.57", default-features = false, features = [
    "http-transport-reqwest-rustls",
] }
```

## Code snippets

### Rustls

Use reqwest's Rustls backend explicitly when you build the client:

```toml
[dependencies]
opendal = { version = "0.57", default-features = false, features = [
    "http-transport-reqwest-rustls",
] }
reqwest = { version = "0.13.4", default-features = false, features = [
    "rustls",
    "stream",
] }
```

```rust
use std::time::Duration;

use opendal::HttpTransporter;
use opendal::ReqwestTransport;

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
opendal = { version = "0.57", default-features = false, features = [
    "http-transport-reqwest-native-tls",
] }
reqwest = { version = "0.13.4", default-features = false, features = [
    "native-tls",
    "stream",
] }
```

```rust
use opendal::HttpTransporter;
use opendal::ReqwestTransport;

fn build_transport() -> Result<HttpTransporter, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .tls_backend_native()
        .build()?;

    Ok(HttpTransporter::new(ReqwestTransport::new(client)))
}
```

### Rustls no provider with WebPKI roots

Use `rustls-no-provider` when the application owns the Rustls provider and
certificate roots. This example uses `ring` for crypto and `webpki-roots` for
Mozilla roots.

```toml
[dependencies]
opendal = { version = "0.57", default-features = false, features = [
    "http-transport-reqwest-rustls-no-provider",
] }
reqwest = { version = "0.13.4", default-features = false, features = [
    "rustls-no-provider",
    "stream",
] }
rustls = { version = "0.23", default-features = false, features = ["ring"] }
webpki-roots = "1"
```

```rust
use opendal::HttpTransporter;
use opendal::ReqwestTransport;

fn build_transport() -> Result<HttpTransporter, Box<dyn std::error::Error>> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder_with_provider(
            rustls::crypto::ring::default_provider().into(),
        )
        .with_safe_default_protocol_versions()?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(tls_config)
        .build()?;

    Ok(HttpTransporter::new(ReqwestTransport::new(client)))
}
```

## Wasm targets

On `wasm32-unknown-unknown` and `wasm32-none`, reqwest switches to its wasm
client implementation. It uses the browser or host Fetch API instead of hyper.
TLS is provided by the browser or host environment, so the effective choice is
the system TLS stack exposed to that environment.

This means `native-tls`, `rustls`, `rustls-no-provider`, custom root stores,
and custom Rustls crypto providers do not select the TLS implementation for
wasm requests. See [reqwest's WASM documentation](https://docs.rs/reqwest/latest/reqwest/#wasm)
for the target-specific limitations.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
