// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Deref;

#[cfg(feature = "rustls")]
use hyper_rustls::HttpsConnector;
#[cfg(not(feature = "rustls"))]
use hyper_tls::HttpsConnector;

/// HttpClient that used across opendal.
///
/// NOTE: we could change or support more underlying http backend.
#[derive(Debug, Clone)]
pub struct HttpClient {
    inner: hyper::Client<HttpsConnector<hyper::client::HttpConnector>, hyper::Body>,
}

#[cfg(not(feature = "rustls"))]
#[inline]
fn https_connector() -> HttpsConnector<hyper::client::HttpConnector> {
    HttpsConnector::new()
}

#[cfg(feature = "rustls")]
#[inline]
fn https_connector() -> HttpsConnector<hyper::client::HttpConnector> {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build()
}

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Self {
        HttpClient {
            inner: hyper::Client::builder()
                // Disable connection pool to address weird async runtime hang.
                //
                // ref: https://github.com/datafuselabs/opendal/issues/473
                .pool_max_idle_per_host(0)
                .build(https_connector()),
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::new()
    }
}

/// Forward all function to http backend.
impl Deref for HttpClient {
    type Target = hyper::Client<HttpsConnector<hyper::client::HttpConnector>, hyper::Body>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
