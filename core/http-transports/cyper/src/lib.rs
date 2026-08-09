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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

use std::future;
use std::mem;

use futures::TryStreamExt;
use http::Request;
use http::Response;
use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::HttpBody;
use opendal_core::HttpTransport;
use opendal_core::HttpTransporter;
use opendal_core::Result;
use opendal_core::raw::parse_content_encoding;
use opendal_core::raw::parse_content_length;
use send_wrapper::SendWrapper;

thread_local! {
    static CLIENT: cyper::Client =
        cyper::Client::new().expect("default Cyper client must initialize");
}

/// A Cyper-backed HTTP transport for Compio runtimes.
///
/// Each runtime thread lazily creates and reuses its own [`cyper::Client`]. The
/// request future and response body must stay on the Compio runtime thread that
/// first polls them. Moving either after polling begins will result in a panic.
#[derive(Clone, Copy, Debug, Default)]
pub struct CyperTransport {}

impl CyperTransport {
    /// Create a Cyper transport.
    pub fn new() -> Self {
        Self {}
    }
}

impl HttpTransport for CyperTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        CLIENT
            .with(|client| {
                let client = client.clone();
                SendWrapper::new(async move { fetch(&client, req).await })
            })
            .await
    }
}

async fn fetch(client: &cyper::Client, req: Request<Buffer>) -> Result<Response<HttpBody>> {
    // Uri stores all string-like data in `Bytes`, so this clone is cheap.
    let uri = req.uri().clone();
    let is_head = req.method() == http::Method::HEAD;
    let (parts, body) = req.into_parts();

    let mut req_builder = client
        .request(parts.method, uri.to_string())
        .map_err(|err| {
            Error::new(ErrorKind::Unexpected, "request url is invalid")
                .with_operation("cyper::fetch")
                .with_context("url", uri.to_string())
                .set_source(err)
        })?
        .headers(parts.headers)
        .version(parts.version);

    if !body.is_empty() {
        let stream = futures::stream::iter(body.map(Ok::<_, cyper::Error>));
        req_builder = req_builder.body(cyper::Body::stream(stream));
    }

    let mut resp = client.execute(req_builder.build()).await.map_err(|err| {
        Error::new(ErrorKind::Unexpected, "send http request")
            .with_operation("cyper::send")
            .with_context("url", uri.to_string())
            .with_temporary(is_temporary_error(&err))
            .set_source(err)
    })?;

    // HEAD responses have no body even when their headers describe the resource
    // size. Encoded response bodies do not have the encoded content length after
    // decoding.
    let content_length = if is_head || parse_content_encoding(resp.headers())?.is_some() {
        None
    } else {
        parse_content_length(resp.headers())?
    };

    let mut builder = Response::builder()
        .status(resp.status())
        .version(resp.version())
        .extension(uri.clone());
    mem::swap(builder.headers_mut().unwrap(), resp.headers_mut());

    let stream = resp
        .bytes_stream()
        .try_filter(|v| future::ready(!v.is_empty()))
        .map_ok(Buffer::from)
        .map_err(move |err| {
            Error::new(ErrorKind::Unexpected, "read data from http response")
                .with_operation("cyper::fetch")
                .with_context("url", uri.to_string())
                .with_temporary(is_temporary_error(&err))
                .set_source(err)
        });
    let body = HttpBody::new(SendWrapper::new(stream), content_length);

    Ok(builder.body(body).expect("response must build succeed"))
}

/// Install Cyper as the process-wide default HTTP transport.
#[doc(hidden)]
pub fn install_default() {
    HttpTransporter::install_default(CyperTransport::new());
}

#[inline]
fn is_temporary_error(err: &cyper::Error) -> bool {
    matches!(
        err,
        cyper::Error::Timeout
            | cyper::Error::System(_)
            | cyper::Error::Hyper(_)
            | cyper::Error::HyperClient(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_install_default_is_lazy() {
        install_default();
    }

    #[test]
    fn test_default_transport_succeeds() {
        let transport = CyperTransport::new();
        assert_eq!(format!("{transport:?}"), "CyperTransport");
    }
}
