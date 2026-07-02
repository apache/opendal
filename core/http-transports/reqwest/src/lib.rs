// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reqwest based HTTP transport for Apache OpenDAL.
//!
//! # TLS backends
//!
//! Enable one of the following Cargo features to select a TLS backend.
//! The `default` feature enables `native-tls`.
//!
//! - **`native-tls`** — Platform TLS links against the OS TLS library:
//!    - Windows: SChannel
//!    - macOS: Secure Transport
//!    - Linux: OpenSSL, requires system development packages
//!
//! - **`rustls`** — [Rustls](https://crates.io/crates/rustls) configured by
//!   reqwest with its default crypto provider and platform certificate
//!   verification. Pure-Rust TLS stack, no system TLS dependency.
//!
//! - **`rustls-ring`** — Rustls with the ring crypto provider and platform
//!   certificate verification.
//!
//! - **`rustls-no-provider`** — Rustls without a built-in crypto provider.
//!   You must install a [`rustls::crypto::CryptoProvider`] before building a
//!   client. Use this when you want to bring your own provider
//!   (e.g., a FIPS-certified module).
//!
//! - **`rustls-webpki-roots`** — Rustls with bundled
//!   [Mozilla root certificates](https://crates.io/crates/webpki-roots).
//!   Fully self-contained: no platform certificate store dependency.
//!   When opendal compiles, cargo will download webpki and bundle certificates.
//!   Good for reproducible builds and environments with a stripped-down root store.
//!   But you can't update root certificates without rebuilding opendal.
//!
//! In application or language binding builds, prefer selecting a single backend.
//! e.g., native-tls. In workspace or `--all-features` builds, Cargo may enable
//! multiple backend features via feature unification; Users can set a transport
//! by calling a builder to pick desired backend.
//!
//! # Builder example
//!
//! Use [`ReqwestTransport::builder`] when applications need to select a TLS
//! backend at runtime while still configuring reqwest-specific options:
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use opendal_core::HttpTransporter;
//! use opendal_http_transport_reqwest::ReqwestTransport;
//!
//! # fn build() -> opendal_core::Result<()> {
//! let transport = ReqwestTransport::builder()
//!     .tls_backend("rustls")
//!     .configure(|builder| builder.connect_timeout(Duration::from_secs(10)))
//!     .build()?;
//! let _transport = HttpTransporter::new(transport);
//! # Ok(())
//! # }
//! ```
//!
//! Building the transport returns [`ErrorKind::ConfigInvalid`] when the chosen
//! TLS backend feature was not compiled into this crate.

use std::fmt::{Debug, Formatter};
use std::future;
use std::mem;
use std::sync::LazyLock;

use futures::TryStreamExt;
use http::Request;
use http::Response;
use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::HttpBody;
use opendal_core::HttpTransport;
use opendal_core::Result;
use opendal_core::raw::parse_content_encoding;
use opendal_core::raw::parse_content_length;

static DEFAULT_REQWEST_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(build_default_client);

fn build_default_client() -> reqwest::Client {
    ReqwestTransportBuilder::new()
        .build_client()
        .expect("failed to build default reqwest client")
}
#[cfg(not(any(
    feature = "native-tls",
    feature = "rustls",
    feature = "rustls-ring",
    feature = "rustls-no-provider",
    feature = "rustls-webpki-roots",
)))]
compile_error!(
    "At least one reqwest TLS backend feature must be enabled: native-tls, rustls, rustls-ring, rustls-no-provider, rustls-webpki-roots"
);

/// Builder for [`ReqwestTransport`].
///
/// This builder enables applications choose an TLS backend at runtime
/// while preserving access to reqwest's own [`reqwest::ClientBuilder`] options.
pub struct ReqwestTransportBuilder {
    client_builder: reqwest::ClientBuilder,
    tls_backend: String,
}

impl Default for ReqwestTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestTransportBuilder {
    /// Create a new builder from [`reqwest::Client::builder`].
    pub fn new() -> Self {
        Self {
            client_builder: reqwest::Client::builder(),
            tls_backend: default_tls_backend().to_string(),
        }
    }

    /// Create a new builder from an existing [`reqwest::ClientBuilder`].
    pub fn from_client_builder(client_builder: reqwest::ClientBuilder) -> Self {
        Self {
            client_builder,
            tls_backend: default_tls_backend().to_string(),
        }
    }

    /// Select the TLS backend to use while building the reqwest client.
    ///
    /// [`Self::build`] and [`Self::build_client`] return [`ErrorKind::ConfigInvalid`]
    /// if the matching Cargo feature is not compiled into this crate.
    ///
    /// Supported values are `native-tls`, `rustls`, `rustls-ring`,
    /// `rustls-no-provider`, and `rustls-webpki-roots`.
    pub fn tls_backend(mut self, tls_backend: impl Into<String>) -> Self {
        self.tls_backend = tls_backend.into().trim().to_string();
        self
    }

    /// Configure the underlying [`reqwest::ClientBuilder`].
    pub fn configure(
        mut self,
        configure: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
    ) -> Self {
        self.client_builder = configure(self.client_builder);
        self
    }

    /// Build a [`reqwest::Client`].
    pub fn build_client(self) -> Result<reqwest::Client> {
        let tls_backend = self.tls_backend;
        let client_builder = apply_tls_backend(self.client_builder, tls_backend.as_str())?;

        client_builder.build().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "reqwest client config is invalid")
                .with_operation("ReqwestTransportBuilder::build")
                .with_context("tls_backend", tls_backend.as_str())
                .set_source(err)
        })
    }

    /// Build a [`ReqwestTransport`].
    pub fn build(self) -> Result<ReqwestTransport> {
        self.build_client().map(ReqwestTransport::new)
    }
}

/// A [`reqwest::Client`] backed HTTP transport.
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl Debug for ReqwestTransport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestTransport").finish()
    }
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self::new(DEFAULT_REQWEST_CLIENT.clone())
    }
}

impl From<reqwest::Client> for ReqwestTransport {
    fn from(client: reqwest::Client) -> Self {
        Self::new(client)
    }
}

impl ReqwestTransport {
    /// Create a builder for [`ReqwestTransport`].
    pub fn builder() -> ReqwestTransportBuilder {
        ReqwestTransportBuilder::new()
    }

    /// Create a new transport from a [`reqwest::Client`].
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Create a new transport from a [`reqwest::ClientBuilder`] and TLS backend.
    pub fn from_client_builder(
        client_builder: reqwest::ClientBuilder,
        tls_backend: impl Into<String>,
    ) -> Result<Self> {
        ReqwestTransportBuilder::from_client_builder(client_builder)
            .tls_backend(tls_backend)
            .build()
    }
}

fn apply_tls_backend(
    client_builder: reqwest::ClientBuilder,
    tls_backend: &str,
) -> Result<reqwest::ClientBuilder> {
    match tls_backend {
        #[cfg(feature = "native-tls")]
        "native-tls" => Ok(client_builder.tls_backend_native()),
        #[cfg(feature = "rustls")]
        "rustls" => Ok(client_builder.tls_backend_rustls()),
        #[cfg(feature = "rustls-ring")]
        "rustls-ring" => Ok(client_builder.tls_backend_preconfigured(rustls_ring_tls_config())),
        #[cfg(feature = "rustls-no-provider")]
        "rustls-no-provider" => Ok(client_builder.tls_backend_rustls()),
        #[cfg(feature = "rustls-webpki-roots")]
        "rustls-webpki-roots" => {
            Ok(client_builder.tls_backend_preconfigured(webpki_roots_tls_config()))
        }
        _ => Err(
            Error::new(ErrorKind::ConfigInvalid, "unknown reqwest TLS backend")
                .with_operation("ReqwestTransportBuilder::tls_backend")
                .with_context("tls_backend", tls_backend),
        ),
    }
}

#[allow(unreachable_code, clippy::needless_return)]
fn default_tls_backend() -> &'static str {
    #[cfg(feature = "native-tls")]
    {
        return "native-tls";
    }

    #[cfg(feature = "rustls")]
    {
        return "rustls";
    }

    #[cfg(feature = "rustls-webpki-roots")]
    {
        return "rustls-webpki-roots";
    }

    #[cfg(feature = "rustls-ring")]
    {
        return "rustls-ring";
    }

    #[cfg(feature = "rustls-no-provider")]
    {
        "rustls-no-provider"
    }
}

#[cfg(feature = "rustls-ring")]
fn rustls_ring_tls_config() -> rustls::ClientConfig {
    use rustls_platform_verifier::BuilderVerifierExt;

    rustls::ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .expect("ring provider must support the default rustls protocol versions")
        .with_platform_verifier()
        .expect("platform verifier must be available")
        .with_no_client_auth()
}

#[cfg(feature = "rustls-webpki-roots")]
fn webpki_roots_tls_config() -> rustls::ClientConfig {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    rustls::ClientConfig::builder_with_provider(
        rustls::crypto::aws_lc_rs::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .expect("aws-lc-rs provider must support the default rustls protocol versions")
    .with_root_certificates(root_store)
    .with_no_client_auth()
}

impl HttpTransport for ReqwestTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;

        let (parts, body) = req.into_parts();

        let url = reqwest::Url::parse(&uri.to_string()).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "request url is invalid")
                .with_operation("reqwest::fetch")
                .with_context("url", uri.to_string())
                .set_source(err)
        })?;

        let mut req_builder = self
            .client
            .request(parts.method, url)
            .headers(parts.headers);

        // Client under wasm doesn't support set version.
        #[cfg(not(target_arch = "wasm32"))]
        {
            req_builder = req_builder.version(parts.version);
        }

        // Don't set body if body is empty.
        if !body.is_empty() {
            #[cfg(not(target_arch = "wasm32"))]
            {
                req_builder = req_builder.body(reqwest::Body::wrap(HttpBufferBody(body)))
            }
            #[cfg(target_arch = "wasm32")]
            {
                req_builder = req_builder.body(reqwest::Body::from(body.to_bytes()))
            }
        }

        let mut resp = req_builder.send().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "send http request")
                .with_operation("reqwest::send")
                .with_context("url", uri.to_string())
                .with_temporary(is_temporary_error(&err))
                .set_source(err)
        })?;

        // Get content length from header so that we can check it.
        //
        // - If the request method is HEAD, we will ignore content length.
        // - If response contains content_encoding, we should omit its content length.
        let content_length = if is_head || parse_content_encoding(resp.headers())?.is_some() {
            None
        } else {
            parse_content_length(resp.headers())?
        };

        let mut hr = Response::builder()
            .status(resp.status())
            // Insert uri into response extension so that we can fetch
            // it later.
            .extension(uri.clone());

        // Response builder under wasm doesn't support set version.
        #[cfg(not(target_arch = "wasm32"))]
        {
            hr = hr.version(resp.version());
        }

        // Swap headers directly instead of copy the entire map.
        mem::swap(hr.headers_mut().unwrap(), resp.headers_mut());

        let bs = HttpBody::new(
            resp.bytes_stream()
                .try_filter(|v| future::ready(!v.is_empty()))
                .map_ok(Buffer::from)
                .map_err(move |err| {
                    Error::new(ErrorKind::Unexpected, "read data from http response")
                        .with_operation("reqwest::fetch")
                        .with_context("url", uri.to_string())
                        .with_temporary(is_temporary_error(&err))
                        .set_source(err)
                }),
            content_length,
        );

        let resp = hr.body(bs).expect("response must build succeed");
        Ok(resp)
    }
}

#[inline]
fn is_temporary_error(err: &reqwest::Error) -> bool {
    // error sending request
    err.is_request()||
    // request or response body error
    err.is_body() ||
    // error decoding response body, for example, connection reset.
    err.is_decode()
}

#[cfg(not(target_arch = "wasm32"))]
struct HttpBufferBody(Buffer);

#[cfg(not(target_arch = "wasm32"))]
impl http_body::Body for HttpBufferBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.0.next() {
            Some(bs) => std::task::Poll::Ready(Some(Ok(http_body::Frame::data(bs)))),
            None => std::task::Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.0.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_backend_accepts_native_tls() {
        let transport = ReqwestTransportBuilder::new()
            .tls_backend("native-tls")
            .build();
        assert!(transport.is_ok());
    }

    #[test]
    fn test_tls_backend_trims_whitespace() {
        let transport = ReqwestTransportBuilder::new()
            .tls_backend("  native-tls  ")
            .build();
        assert!(transport.is_ok());
    }

    #[test]
    fn test_tls_backend_unknown() {
        let err = ReqwestTransportBuilder::new()
            .tls_backend("bogus")
            .build()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn test_tls_backend_context_on_unknown() {
        let err = ReqwestTransportBuilder::new()
            .tls_backend("bogus")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("tls_backend: bogus"));
    }

    #[test]
    fn test_builder_defaults_to_native_tls() {
        let builder = ReqwestTransportBuilder::new();
        assert_eq!(builder.tls_backend, "native-tls");
    }

    #[test]
    fn test_builder_configure() {
        let transport = ReqwestTransportBuilder::new()
            .configure(|b| b.connect_timeout(std::time::Duration::from_secs(5)))
            .build();
        assert!(transport.is_ok());
    }

    #[test]
    fn test_default_transport_succeeds() {
        let transport = ReqwestTransport::default();
        assert_eq!(format!("{:?}", transport), "ReqwestTransport");
    }

    #[test]
    fn test_from_reqwest_client() {
        let client = reqwest::Client::new();
        let transport = ReqwestTransport::from(client);
        assert_eq!(format!("{:?}", transport), "ReqwestTransport");
    }
}
