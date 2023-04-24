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
use std::mem;
use std::str::FromStr;

use futures::TryStreamExt;
use http::Request;
use http::Response;
use reqwest::redirect::Policy;
use reqwest::Url;

use super::body::IncomingAsyncBody;
use super::parse_content_length;
use super::AsyncBody;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

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

    /// Build a new http client in async context.
    pub fn build(mut builder: reqwest::ClientBuilder) -> Result<Self> {
        // Make sure we don't enable auto gzip decompress.
        builder = builder.no_gzip();
        // Make sure we don't enable auto brotli decompress.
        builder = builder.no_brotli();
        // Make sure we don't enable auto deflate decompress.
        builder = builder.no_deflate();
        // Redirect will be handled by ourselves.
        builder = builder.redirect(Policy::none());

        #[cfg(feature = "trust-dns")]
        let builder = builder.trust_dns(true);

        Ok(Self {
            client: builder.build().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "async client build failed").set_source(err)
            })?,
        })
    }

    /// Get the async client from http client.
    pub fn client(&self) -> reqwest::Client {
        self.client.clone()
    }

    /// Send a request in async way.
    pub async fn send(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        let url = req.uri().to_string();
        let is_head = req.method() == http::Method::HEAD;

        let (parts, body) = req.into_parts();

        let mut req_builder = self
            .client
            .request(
                parts.method,
                Url::from_str(&url).expect("input request url must be valid"),
            )
            .version(parts.version)
            .headers(parts.headers);

        req_builder = if let AsyncBody::Multipart(field, r) = body {
            let mut form = reqwest::multipart::Form::new();
            let part = reqwest::multipart::Part::stream(AsyncBody::Bytes(r));
            form = form.part(field, part);

            req_builder.multipart(form)
        } else {
            req_builder.body(body)
        };

        let mut resp = req_builder.send().await.map_err(|err| {
            let is_temporary = !(
                // Builder related error should not be retried.
                err.is_builder() ||
                // Error returned by RedirectPolicy.
                //
                // We don't set this by hand, just don't allow retry.
                err.is_redirect() ||
                 // We never use `Response::error_for_status`, just don't allow retry.
                //
                // Status should be checked by our services.
                err.is_status()
            );

            let mut oerr = Error::new(ErrorKind::Unexpected, "send async request")
                .with_operation("http_util::Client::send_async")
                .with_context("url", &url)
                .set_source(err);
            if is_temporary {
                oerr = oerr.set_temporary();
            }

            oerr
        })?;

        // Get content length from header so that we can check it.
        // If the request method is HEAD, we will ignore this.
        let content_length = if is_head {
            None
        } else {
            parse_content_length(resp.headers()).expect("response content length must be valid")
        };

        let mut hr = Response::builder()
            .version(resp.version())
            .status(resp.status());
        // Swap headers directly instead of copy the entire map.
        mem::swap(hr.headers_mut().unwrap(), resp.headers_mut());

        let stream = resp.bytes_stream().map_err(move |err| {
            // If stream returns a body related error, we can convert
            // it to interrupt so we can retry it.
            Error::new(ErrorKind::Unexpected, "read data from http stream")
                .map(|v| if err.is_body() { v.set_temporary() } else { v })
                .with_context("url", &url)
                .set_source(err)
        });

        let body = IncomingAsyncBody::new(Box::new(stream), content_length);

        let resp = hr.body(body).expect("response must build succeed");

        Ok(resp)
    }
}
