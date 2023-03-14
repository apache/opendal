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

use std::env;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;

use futures::TryStreamExt;
use http::Request;
use http::Response;
use log::debug;
use reqwest::redirect::Policy;
use reqwest::ClientBuilder;
use reqwest::Url;

use super::body::IncomingAsyncBody;
use super::dns::*;
use super::parse_content_length;
use super::AsyncBody;
use super::Body;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// HttpClient that used across opendal.
#[derive(Clone)]
pub struct HttpClient {
    async_client: reqwest::Client,
    sync_client: ureq::Agent,
}

/// We don't want users to know details about our clients.
impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient").finish()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Result<Self> {
        let async_client = {
            let mut builder = ClientBuilder::new();

            // Make sure we don't enable auto gzip decompress.
            builder = builder.no_gzip();
            // Make sure we don't enable auto brotli decompress.
            builder = builder.no_brotli();
            // Make sure we don't enable auto deflate decompress.
            builder = builder.no_deflate();
            // Redirect will be handled by ourselves.
            builder = builder.redirect(Policy::none());

            #[cfg(feature = "trust-dns")]
            let builder = builder.dns_resolver(Arc::new(AsyncTrustDnsResolver::new().unwrap()));
            #[cfg(not(feature = "trust-dns"))]
            let builder = builder.dns_resolver(Arc::new(AsyncStdDnsResolver::default()));

            builder.build().map_err(|err| {
                Error::new(ErrorKind::Unexpected, "async client build failed").set_source(err)
            })?
        };

        let sync_client = {
            let mut builder = ureq::AgentBuilder::new();

            for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"] {
                if let Ok(proxy) = env::var(key) {
                    // Ignore proxy setting if proxy is invalid.
                    if let Ok(proxy) = ureq::Proxy::new(proxy) {
                        debug!("sync client: set proxy to {proxy:?}");
                        builder = builder.proxy(proxy);
                    }
                }
            }

            let builder = builder.resolver(StdDnsResolver::default());

            builder.build()
        };

        Ok(HttpClient {
            async_client,
            sync_client,
        })
    }

    /// Build a new http client from already built clients.
    ///
    /// # Notes
    ///
    /// By using this method, it's caller's duty to make sure everything
    /// configured correctly. Like proxy, dns resolver and so on.
    ///
    /// And this API is an internal API, OpenDAL could change it while bumping
    /// minor versions.
    ///
    /// ## Reminders
    /// ### no auto redirect
    /// OpenDAL will handle all HTTP responses, including redirections.
    /// Auto redirect may cause OpenDAL to fail.
    ///
    /// For reqwest client, please make sure your client's redirect policy is `Policy::none()`.
    /// ```no_run
    /// # use anyhow::Result;
    /// # use reqwest::redirect::Policy;
    /// # fn main() -> Result<()> {
    /// let _client = reqwest::ClientBuilder::new()
    ///     .redirect(Policy::none())
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    /// For ureq client, please make sure your client's redirect count is `0`:
    /// ```no_run
    /// # fn main() {
    /// let _client = ureq::AgentBuilder::new().redirects(0).build();
    /// # }
    /// ```
    pub fn with_client(async_client: reqwest::Client, sync_client: ureq::Agent) -> Self {
        Self {
            async_client,
            sync_client,
        }
    }

    /// Get the async client from http client.
    pub fn async_client(&self) -> reqwest::Client {
        self.async_client.clone()
    }

    /// Get the sync client from http client.
    pub fn sync_client(&self) -> ureq::Agent {
        self.sync_client.clone()
    }

    /// Send a request in blocking way.
    pub fn send(&self, req: Request<Body>) -> Result<Response<Body>> {
        let (parts, body) = req.into_parts();

        let mut ur = self
            .sync_client
            .request(parts.method.as_str(), &parts.uri.to_string());
        for (k, v) in parts.headers.iter() {
            ur = ur.set(k.as_str(), v.to_str().expect("must be valid header"));
        }

        let resp = match ur.send(body) {
            Ok(resp) => resp,
            Err(err_resp) => match err_resp {
                ureq::Error::Status(_code, resp) => resp,
                ureq::Error::Transport(transport) => {
                    let is_temporary = matches!(
                        transport.kind(),
                        ureq::ErrorKind::Dns
                            | ureq::ErrorKind::ConnectionFailed
                            | ureq::ErrorKind::Io
                    );

                    let mut err = Error::new(ErrorKind::Unexpected, "send blocking request")
                        .with_operation("http_util::Client::send")
                        .set_source(transport);
                    if is_temporary {
                        err = err.set_temporary();
                    }

                    return Err(err);
                }
            },
        };

        let mut hr = Response::builder().status(resp.status());
        for name in resp.headers_names() {
            if let Some(value) = resp.header(&name) {
                hr = hr.header(name, value);
            }
        }

        let resp = hr
            .body(Body::Reader(Box::new(resp.into_reader())))
            .expect("response must build succeed");

        Ok(resp)
    }

    /// Send a request in async way.
    pub async fn send_async(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        let is_head = req.method() == http::Method::HEAD;
        let (parts, body) = req.into_parts();

        let mut req_builder = self
            .async_client
            .request(
                parts.method,
                Url::from_str(&parts.uri.to_string()).expect("input request url must be valid"),
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

        let resp = req_builder.send().await.map_err(|err| {
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
        for (k, v) in resp.headers().iter() {
            hr = hr.header(k, v);
        }

        let stream = resp.bytes_stream().map_err(|err| {
            // If stream returns a body related error, we can convert
            // it to interrupt so we can retry it.
            Error::new(ErrorKind::Unexpected, "read data from http stream")
                .map(|v| if err.is_body() { v.set_temporary() } else { v })
                .set_source(err)
        });

        let body = IncomingAsyncBody::new(Box::new(stream), content_length);

        let resp = hr.body(body).expect("response must build succeed");

        Ok(resp)
    }
}
