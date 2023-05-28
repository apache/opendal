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
use http::Response;
use http::{header, Request, StatusCode};
use log::debug;
use reqwest::redirect::Policy;
use url::{ParseError, Url};

use super::body::IncomingAsyncBody;
use super::parse_content_length;
use super::AsyncBody;
use crate::raw::parse_location;
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

        req_builder = match body {
            AsyncBody::Empty => req_builder.body(reqwest::Body::from("")),
            AsyncBody::Bytes(bs) => req_builder.body(reqwest::Body::from(bs)),
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

    /// Send a request in async way with handling redirection logic.
    /// Now we only support redirect GET request.
    /// # Arguments
    /// * `times` - how many times do you want to send request when we need to handle redirection
    pub async fn send_with_redirect(
        &self,
        req: Request<AsyncBody>,
        times: usize,
    ) -> Result<Response<IncomingAsyncBody>> {
        if req.method() != http::Method::GET {
            // for now we only handle redirection for GET request
            // and please note that we don't support stream request either.
            return Err(Error::new(
                ErrorKind::Unsupported,
                "redirect for unsupported HTTP method",
            )
            .with_operation("http_util::Client::send_with_redirect_async")
            .with_context("method", req.method().as_str()));
        }

        let mut prev_req = self.clone_request(&req);
        let mut prev_resp = self.send(req).await?;
        let mut retries = 0;

        let resp = loop {
            let status = prev_resp.status();
            // for now we only handle 302/308 for 3xx status
            // notice that our redirect logic may not follow the HTTP standard
            let should_redirect = match status {
                StatusCode::FOUND => {
                    // theoretically we need to handle following status also:
                    // - StatusCode::MOVED_PERMANENTLY
                    // - StatusCode::SEE_OTHER
                    let mut new_req = self.clone_request(&prev_req);
                    for header in &[
                        header::TRANSFER_ENCODING,
                        header::CONTENT_ENCODING,
                        header::CONTENT_TYPE,
                        header::CONTENT_LENGTH,
                    ] {
                        new_req.headers_mut().remove(header);
                    }
                    // see https://www.rfc-editor.org/rfc/rfc9110.html#section-15.4.2
                    // theoretically for 301, 302 and 303 should change
                    // original http method to GET except HEAD
                    // even though we only support GET request for now,
                    // just in case we support other HTTP method in the future
                    // add method modification logic here
                    match new_req.method() {
                        &http::Method::GET | &http::Method::HEAD => {}
                        _ => *new_req.method_mut() = http::Method::GET,
                    }
                    Some(new_req)
                }
                // theoretically we need to handle following status also:
                // - StatusCode::PERMANENT_REDIRECT
                StatusCode::TEMPORARY_REDIRECT => Some(self.clone_request(&prev_req)),
                _ => None,
            };

            retries += 1;
            if retries > times || should_redirect.is_none() {
                // exceeds maximum retry times or no need to redirect request
                // just return last response
                debug!("no need to redirect or reach the maximum retry times");
                break prev_resp;
            }
            debug!(
                "it is the {} time for http client to retry. maximum times: {}",
                retries, times
            );

            if let Some(mut redirect_req) = should_redirect {
                let prev_url_str = redirect_req.uri().to_string();
                let prev_url = Url::parse(&prev_url_str).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "url is not valid")
                        .with_context("url", &prev_url_str)
                        .set_source(e)
                })?;

                let loc = parse_location(prev_resp.headers())?
                    // no location means invalid redirect response
                    .ok_or_else(|| {
                        debug!(
                            "no location headers in response, url: {}, headers: {:?}",
                            &prev_url_str,
                            &prev_resp.headers()
                        );
                        Error::new(
                            ErrorKind::Unexpected,
                            "no location header in redirect response",
                        )
                        .with_context("method", redirect_req.method().as_str())
                        .with_context("url", &prev_url_str)
                    })?;

                // one url with origin and path
                let loc_url = Url::parse(loc).or_else(|err| {
                    match err {
                        ParseError::RelativeUrlWithoutBase => {
                            debug!("redirected location is relative url, will join it to original base url. loc: {}", loc);
                            let url = prev_url.clone().join(loc).map_err(|err| {
                                Error::new(ErrorKind::Unexpected, "invalid redirect base url and path")
                                    .with_context("base", &prev_url_str)
                                    .with_context("path", loc)
                                    .set_source(err)
                            })?;
                            Ok(url)
                        }
                        err => {
                            Err(
                                Error::new(ErrorKind::Unexpected, "invalid location header")
                                    .with_context("location", loc)
                                    .set_source(err)
                            )
                        }
                    }
                })?;

                debug!("redirecting '{}' to '{}'", &prev_url_str, loc_url.as_str());
                self.remove_sensitive_headers(&mut redirect_req, &loc_url, &prev_url);
                // change the request uri
                *redirect_req.uri_mut() = loc_url.as_str().parse().map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "new redirect url is invalid")
                        .with_context("loc", loc_url.as_str())
                        .set_source(err)
                })?;
                prev_req = self.clone_request(&redirect_req);
                prev_resp = self.send(redirect_req).await?;
            }
        };
        Ok(resp)
    }
}

impl HttpClient {
    fn clone_request(&self, req: &Request<AsyncBody>) -> Request<AsyncBody> {
        let (mut parts, body) = Request::new(match req.body() {
            AsyncBody::Empty => AsyncBody::Empty,
            AsyncBody::Bytes(bytes) => AsyncBody::Bytes(bytes.clone()),
        })
        .into_parts();

        // we just ignore extensions of request, because we won't use it
        parts.method = req.method().clone();
        parts.uri = req.uri().clone();
        parts.version = req.version();
        parts.headers = req.headers().clone();

        Request::from_parts(parts, body)
    }

    fn remove_sensitive_headers(&self, req: &mut Request<AsyncBody>, next: &Url, previous: &Url) {
        let cross_host = next.host_str() != previous.host_str()
            || next.port_or_known_default() != previous.port_or_known_default();
        if cross_host {
            let headers = req.headers_mut();
            headers.remove(header::AUTHORIZATION);
            headers.remove(header::COOKIE);
            headers.remove("cookie2");
            headers.remove(header::PROXY_AUTHORIZATION);
            headers.remove(header::WWW_AUTHENTICATE);
        }
    }
}
