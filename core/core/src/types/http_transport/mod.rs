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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;

#[cfg(feature = "reqsign")]
use bytes::Bytes;
use http::Request;
use http::Response;

use crate::raw::BoxedFuture;
use crate::raw::oio::ReadStream;
use crate::types::Buffer;
use crate::types::Error;
use crate::types::ErrorKind;
use crate::types::Result;

mod body;
pub use body::HttpBody;

static DEFAULT_HTTP_TRANSPORTER: OnceLock<HttpTransporter> = OnceLock::new();

/// HTTP transport used by OpenDAL services.
///
/// Implement this trait to provide a custom HTTP backend. A transport must
/// support 3xx redirection.
pub trait HttpTransport: Send + Sync + Unpin + 'static {
    /// Fetch a request and return a streamable [`HttpBody`].
    #[cfg(not(target_arch = "wasm32"))]
    fn fetch(
        &self,
        req: Request<Buffer>,
    ) -> impl Future<Output = Result<Response<HttpBody>>> + Send;

    /// Fetch a request and return a streamable [`HttpBody`].
    #[cfg(target_arch = "wasm32")]
    fn fetch(&self, req: Request<Buffer>) -> impl Future<Output = Result<Response<HttpBody>>>;
}

/// Object-safe version of [`HttpTransport`].
pub(crate) trait HttpTransportDyn: Send + Sync + Unpin + 'static {
    /// Fetch a request through a boxed future.
    fn fetch_dyn(&self, req: Request<Buffer>) -> BoxedFuture<'_, Result<Response<HttpBody>>>;
}

impl<T: HttpTransport + ?Sized> HttpTransportDyn for T {
    fn fetch_dyn(&self, req: Request<Buffer>) -> BoxedFuture<'_, Result<Response<HttpBody>>> {
        Box::pin(self.fetch(req))
    }
}

/// Type-erased HTTP transport handle.
#[derive(Clone)]
pub struct HttpTransporter {
    inner: Arc<dyn HttpTransportDyn>,
}

impl Debug for HttpTransporter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransporter").finish()
    }
}

impl Default for HttpTransporter {
    fn default() -> Self {
        Self::new(DefaultHttpTransport)
    }
}

impl HttpTransporter {
    /// Create a new `HttpTransporter` from an [`HttpTransport`].
    pub fn new(transport: impl HttpTransport) -> Self {
        Self {
            inner: Arc::new(transport),
        }
    }

    /// Install the process-wide default HTTP transport.
    ///
    /// The first installed transport wins. Later calls are ignored.
    ///
    /// The `opendal` facade calls this before `main` when
    /// `auto-register-services` and a default HTTP transport feature are enabled.
    /// Applications that need to install their own process-wide default should
    /// disable `auto-register-services`. This does not affect transports
    /// configured directly on an operator.
    pub fn install_default(transport: impl HttpTransport) {
        let _ = DEFAULT_HTTP_TRANSPORTER.set(Self::new(transport));
    }

    /// Send a request and consume the response body into a [`Buffer`].
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        let (parts, mut body) = self.fetch(req).await?.into_parts();
        let buffer = body.read_all().await?;
        Ok(Response::from_parts(parts, buffer))
    }

    /// Fetch a request and return a streamable [`HttpBody`].
    pub async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.inner.fetch_dyn(req).await
    }
}

impl HttpTransport for HttpTransporter {
    #[cfg(not(target_arch = "wasm32"))]
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        HttpTransporter::fetch(self, req).await
    }

    #[cfg(target_arch = "wasm32")]
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        HttpTransporter::fetch(self, req).await
    }
}

#[cfg(feature = "reqsign")]
impl reqsign_core::HttpSend for HttpTransporter {
    async fn http_send(&self, req: Request<Bytes>) -> reqsign_core::Result<Response<Bytes>> {
        let req = req.map(Buffer::from);
        let resp = self.send(req).await.map_err(|err| {
            let retryable = err.is_temporary();
            reqsign_core::Error::unexpected("send request via OpenDAL HttpTransporter")
                .with_source(err)
                .set_retryable(retryable)
        })?;

        let (parts, body) = resp.into_parts();
        Ok(Response::from_parts(parts, body.to_bytes()))
    }
}

#[derive(Debug)]
struct DefaultHttpTransport;

impl HttpTransport for DefaultHttpTransport {
    #[cfg(not(target_arch = "wasm32"))]
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        fetch_with_default_transport(req).await
    }

    #[cfg(target_arch = "wasm32")]
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        fetch_with_default_transport(req).await
    }
}

async fn fetch_with_default_transport(req: Request<Buffer>) -> Result<Response<HttpBody>> {
    match DEFAULT_HTTP_TRANSPORTER.get() {
        Some(transport) => transport.fetch(req).await,
        None => Err(Error::new(
            ErrorKind::ConfigInvalid,
            "default HTTP transport is not installed",
        )
        .with_operation("HttpTransporter::default")
        .with_context(
            "advice",
            "call HttpTransporter::install_default before using HTTP services",
        )),
    }
}
