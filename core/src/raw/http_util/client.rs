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

use bytes::Bytes;
use futures::Stream;
use futures::TryStreamExt;
use http::Request;
use http::Response;

use super::RequestBody;
use crate::raw::http_util::body::ResponseBody;
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

    /// Send an http requests and got the response in streaming.
    pub async fn send(
        &self,
        req: Request<RequestBody>,
    ) -> Result<Response<ResponseBody<impl Stream<Item = Result<Bytes>>>>> {
        // Uri stores all string alike data in `Bytes` which means
        // the clone here is cheap.
        let uri = req.uri().clone();
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
            RequestBody::Empty => req_builder.body(reqwest::Body::from("")),
            RequestBody::Bytes(bs) => req_builder.body(reqwest::Body::from(bs)),
            RequestBody::Stream(s) => {
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
                .with_operation("http_util::Client::execute")
                .with_context("url", uri.to_string())
                .set_source(err);
            if is_temporary {
                oerr = oerr.set_temporary();
            }

            oerr
        })?;

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

        let stream = resp.bytes_stream().map_err(move |err| {
            Error::new(ErrorKind::Unexpected, "read data from http response")
                .with_context("url", uri.to_string())
                .set_source(err)
        });

        let resp = hr
            .body(ResponseBody::new(stream))
            .expect("response must build succeed");
        Ok(resp)
    }
}
