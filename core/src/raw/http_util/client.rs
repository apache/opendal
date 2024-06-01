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
use std::future;
use std::mem;
use std::str::FromStr;

use futures::TryStreamExt;
use http::Request;
use http::Response;
use raw::oio::Read;

use super::parse_content_encoding;
use super::parse_content_length;
use super::HttpBody;
use crate::*;

/// HttpClient that used across opendal.
#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
}

/// We don't want users to know details about our clients.
impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").finish()
    }
}

impl HttpClient {
    /// Create a new http client in async context.
    pub fn new() -> Result<Self> {
        Self::build(reqwest::ClientBuilder::new())
    }

    /// Construct `Self` with given [`reqwest::Client`]
    pub fn with(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Build a new http client in async context.
    pub fn build(builder: reqwest::ClientBuilder) -> Result<Self> {
        Ok(Self {
            client: builder.build().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "http client build failed").set_source(err)
            })?,
        })
    }

    /// Get the async client from http client.
    pub fn client(&self) -> reqwest::Client {
        self.client.clone()
    }

    /// Send a request in async way.
    pub async fn send(&self, req: Request<Buffer>) -> Result<Response<Buffer>> {
        let (parts, mut body) = self.fetch(req).await?.into_parts();
        let buffer = body.read_all().await?;
        Ok(Response::from_parts(parts, buffer))
    }

    /// Fetch a request in async way.
    pub async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
        let is_head = req.method() == http::Method::HEAD;

        let (parts, body) = req.into_parts();

        let mut req_builder = self
            .client
            .request(
                parts.method,
                reqwest::Url::from_str(&uri.to_string()).expect("input request url must be valid"),
            )
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
                req_builder = req_builder.body(reqwest::Body::wrap_stream(body))
            }
            #[cfg(target_arch = "wasm32")]
            {
                req_builder = req_builder.body(reqwest::Body::from(body.to_bytes()))
            }
        }

        let mut resp = req_builder.send().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "send http request")
                .with_operation("http_util::Client::send")
                .with_context("url", uri.to_string())
                .with_temporary(is_temporary_error(&err))
                .set_source(err)
        })?;

        // Get content length from header so that we can check it.
        //
        // - If the request method is HEAD, we will ignore content length.
        // - If response contains content_encoding, we should omit it's content length.
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
                        .with_operation("http_util::Client::send")
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
