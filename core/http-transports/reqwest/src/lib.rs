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

#![deny(missing_docs)]

use std::fmt::Debug;
use std::fmt::Formatter;
use std::future;
use std::mem;
use std::str::FromStr;
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

static DEFAULT_REQWEST_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

/// A [`reqwest::Client`] backed HTTP transport.
///
/// # Notes
///
/// Reqwest must be configured with a TLS feature before sending HTTPS requests.
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
    /// Create a new transport from a [`reqwest::Client`].
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl HttpTransport for ReqwestTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;

        let (parts, body) = req.into_parts();

        let url = reqwest::Url::from_str(&uri.to_string()).map_err(|err| {
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
