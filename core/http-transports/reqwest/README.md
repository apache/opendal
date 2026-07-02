# opendal-http-transport-reqwest

Reqwest-based HTTP transport for [Apache OpenDAL](https://opendal.apache.org).

This crate provides `ReqwestTransport`, an implementation of OpenDAL's
`HttpTransport` trait backed by [reqwest](https://crates.io/crates/reqwest).

## TLS configuration

When using Rustls, TLS configuration has two independent axes:

| Axis | What it decides | Options |
|------|----------------|---------|
| **Crypto provider** | Who performs the cryptographic operations (key exchange, symmetric ciphers, hashing) | reqwest's default provider, `ring`, or any custom `CryptoProvider` |
| **Certificate verification** | How the server's TLS certificate chain is validated | Platform verifier (default in `rustls`), bundled Mozilla roots (`rustls-webpki-roots`), or custom |

The `native-tls` feature sidesteps both axes by delegating everything to
the OS TLS library (SChannel / Secure Transport / OpenSSL).

### Feature matrix

| Feature | Crypto provider | Certificate roots | Use when |
|---------|----------------|-------------------|----------|
| `native-tls` (default) | OS library | OS trust store | You want zero Rust-side TLS config |
| `rustls` | reqwest default | Platform verifier | Pure-Rust TLS with OS trust store |
| `rustls-ring` | ring | Platform verifier | Rustls with ring and OS trust store |
| `rustls-webpki-roots` | aws-lc-rs | Bundled Mozilla roots | Fully self-contained, no OS dependency |
| `rustls-no-provider` | **you provide** | **you provide** | BYO crypto (ring, FIPS module, etc.) |

### Usage via the `opendal` facade crate

Most users depend on `opendal` rather than this crate directly. The facade
installs this transport when any `http-transport-reqwest-*` feature is enabled.

```toml
# Default — reqwest transport with native-tls
opendal = { version = "0.57" }
```

To select a different TLS backend, disable default features and enable the
one you need:

```toml
opendal = { version = "0.57", default-features = false, features = ["http-transport-reqwest-rustls"] }
```

### Feature usage with `rustls`

The `rustls` feature uses reqwest's own Rustls configuration through
`ClientBuilder::tls_backend_rustls()`, so settings such as custom root
certificates, client identity, SNI, TLS info, and dangerous certificate
verification flags should be configured with reqwest's builder methods.
The `rustls-ring` feature builds a `rustls::ClientConfig` directly so it can
select ring without installing a process-global Rustls crypto provider.
The `rustls-webpki-roots` feature still builds a `rustls::ClientConfig` directly
because `webpki-roots` exposes Rustls trust anchors rather than
`reqwest::Certificate` values.

```toml
[dependencies]
opendal-http-transport-reqwest = { version = "0.57", default-features = false, features = ["rustls"] }
```

```rust
use std::time::Duration;

use opendal_http_transport_reqwest::ReqwestTlsBackend;
use opendal_http_transport_reqwest::ReqwestTransport;
use opendal::HttpTransporter;

// You can configure reqwest dynamically and select a compiled TLS backend.
let tls_backend = "rustls".parse::<ReqwestTlsBackend>().unwrap();
let transport = ReqwestTransport::builder()
    .tls_backend(tls_backend)
    .configure(|builder| builder.connect_timeout(Duration::from_secs(10)))
    .build()
    .unwrap();
```

### Bringing your own reqwest client

When you need full control over the TLS stack — custom `ClientConfig`,
client certificates, proxy settings, or connection pool tuning — build a
`reqwest::Client` yourself and wrap it:

```toml
[dependencies]
opendal = { version = "0.57", default-features = false, features = [
    "services-s3",
    "http-transport-reqwest-rustls-no-provider",
] }
opendal-http-transport-reqwest = { version = "0.57", default-features = false, features = ["rustls-no-provider"] }
rustls = { version = "0.23", features = ["ring"], default-features = false }
webpki-roots = "1"
```

```rust
use std::time::Duration;

use opendal::HttpTransporter;
use opendal::OperationContext;
use opendal_http_transport_reqwest::ReqwestTransport;

fn main() {
    // 1. Configure your crypto provider and certificate roots.
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth();

    // 2. Build a reqwest client with your TLS config.
    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(tls_config)
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(20)
        .build()
        .unwrap();

    // 3. Wrap it as a ReqwestTransport and attach to an operator.
    let transport = HttpTransporter::new(ReqwestTransport::new(client));

    let op = opendal::Operator::via_iter("s3", [
        ("bucket".to_string(), "my-bucket".to_string()),
        ("region".to_string(), "us-east-1".to_string()),
    ])
    .expect("failed to build operator")
    .with_context(OperationContext::new().with_http_transport(transport));
}
```

This approach gives you complete ownership over TLS and client.

## License and Trademarks

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
