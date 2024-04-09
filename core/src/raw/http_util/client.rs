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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future;
use std::mem;
use std::str::FromStr;

use bytes::Buf;
use bytes::Bytes;
use futures::TryStreamExt;
use http::Request;
use http::Response;

use super::parse_content_encoding;
use super::parse_content_length;
use super::AsyncBody;
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
    #[cfg(not(target_arch = "wasm32"))]
    pub fn build(builder: reqwest::ClientBuilder) -> Result<Self> {
        Ok(Self {
            client: builder.build().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "http client build failed").set_source(err)
            })?,
        })
    }

    /// Build a new http client in async context.
    #[cfg(target_arch = "wasm32")]
    pub fn build(mut builder: reqwest::ClientBuilder) -> Result<Self> {
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
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<Buffer>> {
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

        req_builder = match body {
            AsyncBody::Empty => req_builder.body(reqwest::Body::from("")),
            AsyncBody::Bytes(bs) => req_builder.body(reqwest::Body::from(bs)),
            AsyncBody::Stream(s) => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    req_builder.body(reqwest::Body::wrap_stream(s))
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let bs = oio::StreamExt::collect(s).await?;
                    req_builder.body(reqwest::Body::from(bs))
                }
            }
        };

        let mut resp = req_builder.send().await.map_err(|err| {
            let is_temporary = !(
                // Builder related error should not be retried.
                err.is_builder() ||
                // Error returned by RedirectPolicy.
                //
                // Don't retry error if we redirect too many.
                err.is_redirect() ||
                // We never use `Response::error_for_status`, just don't allow retry.
                //
                // Status should be checked by our services.
                err.is_status()
            );

            let mut oerr = Error::new(ErrorKind::Unexpected, "send http request")
                .with_operation("http_util::Client::send")
                .with_context("url", uri.to_string())
                .set_source(err);
            if is_temporary {
                oerr = oerr.set_temporary();
            }

            oerr
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

        let bs: Vec<Bytes> = resp
            .bytes_stream()
            .try_filter(|v| future::ready(!v.is_empty()))
            .try_collect()
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read data from http response")
                    .with_context("url", uri.to_string())
                    .set_source(err)
            })?;

        let buffer = Buffer::from(bs);

        if let Some(expect) = content_length {
            check(expect, buffer.remaining() as u64)?;
        }

        let resp = hr.body(buffer).expect("response must build succeed");

        Ok(resp)
    }
}

#[inline]
fn check(expect: u64, actual: u64) -> Result<()> {
    match actual.cmp(&expect) {
        Ordering::Equal => Ok(()),
        Ordering::Less => Err(Error::new(
            ErrorKind::ContentIncomplete,
            &format!("reader got too little data, expect: {expect}, actual: {actual}"),
        )
        .set_temporary()),
        Ordering::Greater => Err(Error::new(
            ErrorKind::ContentTruncated,
            &format!("reader got too much data, expect: {expect}, actual: {actual}"),
        )
        .set_temporary()),
    }
}
