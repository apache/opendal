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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::str::FromStr;

use crate::http_util::body::IncomingAsyncBody;
use futures::TryStreamExt;
use http::Request;
use http::Response;
use reqwest::Url;

use super::AsyncBody;
use super::Body;
use crate::io_util::into_reader;

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
                ureq::Error::Status(_code, resp) => resp,
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

    pub async fn send_async_multipart(
        &self,
        req: Request<AsyncBody>,
        field_name: String,
    ) -> Result<Response<IncomingAsyncBody>> {
        let (parts, body) = req.into_parts();

        let mut form = reqwest::multipart::Form::new();
        let part = reqwest::multipart::Part::stream(body);
        form = form.part(field_name, part);

        let req_builder = self
            .async_client
            .request(
                parts.method,
                Url::from_str(&parts.uri.to_string()).expect("input request url must be valid"),
            )
            .version(parts.version)
            .headers(parts.headers)
            .multipart(form);

        self.send_async_req(req_builder).await
    }

    /// Send a request in async way.
    pub async fn send_async(&self, req: Request<AsyncBody>) -> Result<Response<IncomingAsyncBody>> {
        let (parts, body) = req.into_parts();

        let req_builder = self
            .async_client
            .request(
                parts.method,
                Url::from_str(&parts.uri.to_string()).expect("input request url must be valid"),
            )
            .version(parts.version)
            .headers(parts.headers)
            .body(body);

        self.send_async_req(req_builder).await
    }

    async fn send_async_req(
        &self,
        req_builder: reqwest::RequestBuilder,
    ) -> Result<Response<IncomingAsyncBody>> {
        let resp = req_builder.send().await.map_err(|err| {
            let kind = if err.is_timeout() || err.is_connect() {
                ErrorKind::Interrupted
            } else {
                ErrorKind::Other
            };

            Error::new(kind, err)
        })?;

        let mut hr = Response::builder()
            .version(resp.version())
            .status(resp.status());
        for (k, v) in resp.headers().iter() {
            hr = hr.header(k, v);
        }

        let stream = resp.bytes_stream().map_err(|err| {
            let kind = if err.is_timeout() || err.is_connect() {
                ErrorKind::Interrupted
            } else {
                ErrorKind::Other
            };

            Error::new(kind, err)
        });
        let body = IncomingAsyncBody::new(Box::new(into_reader(stream)));

        let resp = hr.body(body).expect("response must build succeed");

        Ok(resp)
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient::new()
    }
}
