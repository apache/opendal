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
use std::ops::Deref;
use std::sync::Arc;

use futures::Future;
use http::Request;
use http::Response;

use super::HttpBody;
use crate::raw::oio::Read;
use crate::raw::*;
use crate::*;

/// HttpFetcher is a type erased [`HttpFetch`].
pub type HttpFetcher = Arc<dyn HttpFetchDyn>;

/// A HTTP client instance for OpenDAL's services.
///
/// # Notes
///
/// * A http client must support redirections that follows 3xx response.
#[derive(Clone)]
pub struct HttpClient {
    fetcher: HttpFetcher,
}

/// We don't want users to know details about our clients.
impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").finish()
    }
}

#[cfg(feature = "http-client-reqwest")]
impl Default for HttpClient {
    fn default() -> Self {
        Self {
            fetcher: Arc::new(super::reqwest_impl::GLOBAL_REQWEST_CLIENT.clone()),
        }
    }
}

#[cfg(not(feature = "http-client-reqwest"))]
impl Default for HttpClient {
    fn default() -> Self {
        Self {
            fetcher: Arc::new(StubHttpClient),
        }
    }
}

/// A stub HTTP client that returns an error when used.
/// This is used when no HTTP client implementation is available.
#[cfg(not(feature = "http-client-reqwest"))]
struct StubHttpClient;

#[cfg(not(feature = "http-client-reqwest"))]
impl HttpFetch for StubHttpClient {
    async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
        Err(Error::new(
            ErrorKind::ConfigInvalid,
            "No HTTP client available. Enable the 'http-client-reqwest' feature or provide a custom HTTP client via HttpClient::with().",
        ))
    }
}

impl HttpClient {
    /// Create a new http client in async context.
    #[cfg(feature = "http-client-reqwest")]
    pub fn new() -> Result<Self> {
        Ok(Self::default())
    }

    /// Create a new http client in async context.
    ///
    /// Returns an error when no HTTP client implementation is available.
    /// Either enable the `http-client-reqwest` feature or use [`HttpClient::with`]
    /// to provide a custom client.
    #[cfg(not(feature = "http-client-reqwest"))]
    pub fn new() -> Result<Self> {
        Err(Error::new(
            ErrorKind::ConfigInvalid,
            "No HTTP client available. Enable the 'http-client-reqwest' feature or provide a custom HTTP client via HttpClient::with().",
        ))
    }

    /// Construct `Self` with given client that implements [`HttpFetch`]
    pub fn with(client: impl HttpFetch) -> Self {
        let fetcher = Arc::new(client);
        Self { fetcher }
    }

    /// Get the inner http client.
    pub fn into_inner(self) -> HttpFetcher {
        self.fetcher
    }

    /// Send a request and consume response.
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        let (parts, mut body) = self.fetch(req).await?.into_parts();
        let buffer = body.read_all().await?;
        Ok(Response::from_parts(parts, buffer))
    }

    /// Fetch a request and return a streamable [`HttpBody`].
    ///
    /// Services can use [`HttpBody`] as [`Access::Read`].
    pub async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.fetcher.fetch(req).await
    }
}

/// HttpFetch is the trait to fetch a request in async way.
/// User should implement this trait to provide their own http client.
pub trait HttpFetch: Send + Sync + Unpin + 'static {
    /// Fetch a request in async way.
    fn fetch(
        &self,
        req: Request<Buffer>,
    ) -> impl Future<Output = Result<Response<HttpBody>>> + MaybeSend;
}

/// HttpFetchDyn is the dyn version of [`HttpFetch`]
/// which make it possible to use as `Arc<dyn HttpFetchDyn>`.
/// User should never implement this trait, but use `HttpFetch` instead.
pub trait HttpFetchDyn: Send + Sync + Unpin + 'static {
    /// The dyn version of [`HttpFetch::fetch`].
    ///
    /// This function returns a boxed future to make it object safe.
    fn fetch_dyn(&self, req: Request<Buffer>) -> BoxedFuture<'_, Result<Response<HttpBody>>>;
}

impl<T: HttpFetch + ?Sized> HttpFetchDyn for T {
    fn fetch_dyn(&self, req: Request<Buffer>) -> BoxedFuture<'_, Result<Response<HttpBody>>> {
        Box::pin(self.fetch(req))
    }
}

impl<T: HttpFetchDyn + ?Sized> HttpFetch for Arc<T> {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.deref().fetch_dyn(req).await
    }
}
