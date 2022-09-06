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

use std::time::Duration;

use super::AsyncBody;
use super::Body;
use crate::io_util::into_reader;
use futures::TryStreamExt;
use http::{HeaderMap, Response};
use http::{Request, Version};
use reqwest::{ResponseBuilderExt, Url};
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::str::FromStr;

/// HttpClient that used across opendal.
#[derive(Debug, Clone)]
pub struct HttpClient {
    async_client: reqwest::Client,
    sync_client: ureq::Agent,
}

impl HttpClient {
    /// Create a new http client.
    pub fn new() -> Self {
        let async_client = reqwest::Client::new();
        let sync_client = ureq::Agent::new();

        HttpClient {
            async_client,
            sync_client,
        }
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
                ureq::Error::Status(code, resp) => resp,
                ureq::Error::Transport(transport) => {
                    let kind = match transport.kind() {
                        ureq::ErrorKind::Dns
                        | ureq::ErrorKind::ConnectionFailed
                        | ureq::ErrorKind::Io => ErrorKind::Interrupted,
                        _ => ErrorKind::Other,
                    };

                    return Err(Error::new(kind, transport));
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
    pub async fn send_async(&self, req: Request<AsyncBody>) -> Result<Response<AsyncBody>> {
        let (parts, body) = req.into_parts();

        let mut resp = self
            .async_client
            .request(
                parts.method,
                Url::from_str(&parts.uri.to_string()).expect("input request url must be valid"),
            )
            .version(parts.version)
            .headers(parts.headers)
            .body(body)
            .send()
            .await
            .map_err(|err| {
                let kind = if err.is_timeout() {
                    ErrorKind::Interrupted
                } else if err.is_connect() {
                    ErrorKind::Interrupted
                } else {
                    ErrorKind::Other
                };

                Error::new(kind, err)
            })?;

        let mut hr = Response::builder()
            .version(resp.version())
            .status(resp.status());
        mem::swap(&mut resp.headers_mut(), &mut hr.headers_mut().unwrap());

        let resp = hr
            .body(AsyncBody::Reader(Box::new(into_reader(
                resp.bytes_stream().map_err(|err| {
                    let kind = if err.is_timeout() {
                        ErrorKind::Interrupted
                    } else if err.is_connect() {
                        ErrorKind::Interrupted
                    } else {
                        ErrorKind::Other
                    };

                    Error::new(kind, err)
                }),
            ))))
            .expect("response must build succeed");

        Ok(resp)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::new()
    }
}
