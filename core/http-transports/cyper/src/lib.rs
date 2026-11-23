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

use std::cell::RefCell;
use std::future;
use std::pin::pin;
use std::task::Waker;

use compio::runtime::Runtime;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::channel::oneshot;
use futures::future::Either;
use futures::future::select;
use http::Request;
use http::Response;
use http::StatusCode;
use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::HttpBody;
use opendal_core::HttpTransport;
use opendal_core::HttpUri;
use opendal_core::Result;
use opendal_core::raw::parse_content_encoding;
use opendal_core::raw::parse_content_length;

thread_local! {
    static CLIENT: RefCell<Option<(Waker, cyper::Client)>> = const { RefCell::new(None) };
}

/// A Cyper-backed HTTP transport for Compio runtimes.
///
/// Requests must start inside an active Compio runtime. The runtime that
/// starts a request owns its response stream and must keep running while the
/// response is consumed. The returned body can be read or dropped from any thread.
#[derive(Clone, Copy, Debug, Default)]
pub struct CyperTransport;

impl CyperTransport {
    /// Create a Cyper transport.
    pub fn new() -> Self {
        Self
    }
}

impl HttpTransport for CyperTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let (reply, response) = oneshot::channel();
        {
            let runtime = Runtime::try_current().ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "Cyper requires a Compio runtime")
                    .with_operation("cyper::fetch")
            })?;
            let client = client_for_runtime(&runtime)?;
            runtime.spawn(drive_request(client, req, reply)).detach();
        }
        response.await.map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "Compio stopped before response headers",
            )
            .with_operation("cyper::fetch")
            .set_source(err)
        })?
    }
}

fn client_for_runtime(runtime: &Runtime) -> Result<cyper::Client> {
    CLIENT.with_borrow_mut(|cached| {
        // A different runtime on this thread cannot drive the cached pool's connections.
        let waker = runtime.waker();
        if let Some((owner, client)) = cached
            && owner.will_wake(&waker)
        {
            return Ok(client.clone());
        }

        let builder = cyper::Client::builder();
        #[cfg(feature = "rustls")]
        let builder = {
            use rustls_platform_verifier::BuilderVerifierExt;
            use std::sync::Arc;

            // Reqwest can enable another Rustls provider in the same executable.
            let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .and_then(BuilderVerifierExt::with_platform_verifier)
            .map_err(|err| {
                Error::new(ErrorKind::ConfigInvalid, "initialize Cyper Rustls client")
                    .with_operation("cyper::fetch")
                    .set_source(err)
            })?
            .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            builder.use_rustls(Arc::new(config))
        };
        let client = builder.build().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "initialize Cyper client")
                .with_operation("cyper::fetch")
                .set_source(err)
        })?;
        *cached = Some((waker, client.clone()));
        Ok(client)
    })
}

async fn drive_request(
    client: cyper::Client,
    req: Request<Buffer>,
    mut reply: oneshot::Sender<Result<Response<HttpBody>>>,
) {
    // Cancel a request even when it has yet to receive its response headers.
    let response = match select(reply.cancellation(), pin!(fetch(&client, req))).await {
        Either::Left(_) => return,
        Either::Right((result, _)) => result,
    };
    let (response, content_length) = match response {
        Ok(response) => response,
        Err(err) => {
            let _ = reply.send(Err(err));
            return;
        }
    };
    let (parts, stream) = response.into_parts();

    // Only the runtime task owns the Cyper stream. The bounded channel applies
    // backpressure, and closing it wakes the task even during a pending read.
    let (tx, rx) = async_channel::bounded(1);
    let body = futures::stream::unfold(Some(rx), |rx| async {
        let rx = rx?;
        match rx.recv().await {
            Ok(Some(item)) => Some((item, Some(rx))),
            Ok(None) => None,
            Err(err) => Some((
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "Compio stopped before the response ended",
                )
                .with_operation("cyper::fetch")
                .set_source(err)),
                None,
            )),
        }
    });
    let response = Response::from_parts(parts, HttpBody::new(Box::pin(body), content_length));
    if reply.send(Ok(response)).is_err() {
        return;
    }

    let forward = async {
        {
            let mut stream = pin!(stream);
            while let Some(item) = stream.next().await {
                let failed = item.is_err();
                if tx.send(Some(item)).await.is_err() {
                    return;
                }
                if failed {
                    break;
                }
            }
        }
        // Send EOF after releasing the stream to distinguish completion from runtime shutdown.
        let _ = tx.send(None).await;
    };
    let _ = select(pin!(tx.closed()), pin!(forward)).await;
}

async fn fetch(
    client: &cyper::Client,
    req: Request<Buffer>,
) -> Result<(Response<impl Stream<Item = Result<Buffer>>>, Option<u64>)> {
    // Uri stores all string-like data in `Bytes`, so this clone is cheap.
    let uri = req.uri().clone();
    let is_head = req.method() == http::Method::HEAD;
    let (parts, body) = req.into_parts();

    let mut req_builder = client
        .request(parts.method, uri.to_string())
        .map_err(|err| {
            Error::new(ErrorKind::Unexpected, "request url is invalid")
                .with_operation("cyper::fetch")
                .with_context("url", HttpUri::new(uri.to_string()).redacted_uri())
                .set_source(err)
        })?
        .headers(parts.headers)
        .version(parts.version);

    if !body.is_empty() {
        // `Buffer` is already fully buffered in memory, and cyper can only replay
        // a `Bytes` body on 307 and 308 redirects; it returns the redirect
        // response for a stream body. Converting a contiguous `Buffer` is
        // zero-copy, and only a multi-chunk `Buffer` is copied, so release the
        // source chunks before sending.
        let bytes = body.to_bytes();
        drop(body);
        req_builder = req_builder.body(cyper::Body::from(bytes));
    }

    let mut resp = client.execute(req_builder.build()).await.map_err(|err| {
        Error::new(ErrorKind::Unexpected, "send http request")
            .with_operation("cyper::send")
            .with_context("url", HttpUri::new(uri.to_string()).redacted_uri())
            .with_temporary(is_temporary_error(&err))
            .set_source(err)
    })?;

    // RFC 9110 §8.6: responses to HEAD and 1xx, 204, and 304 responses never
    // carry a message body. A 304 may still carry the Content-Length of the
    // selected representation, so it cannot validate the body. Encoded bodies
    // also do not have the encoded content length after decoding.
    let status = resp.status();
    let is_bodyless = is_head
        || status == StatusCode::NOT_MODIFIED
        || status == StatusCode::NO_CONTENT
        || status.is_informational();
    let content_length = if is_bodyless || parse_content_encoding(resp.headers())?.is_some() {
        None
    } else {
        parse_content_length(resp.headers())?
    };

    let mut builder = Response::builder()
        .status(status)
        .version(resp.version())
        .extension(uri.clone());
    *builder.headers_mut().unwrap() = std::mem::take(resp.headers_mut());

    let stream = resp
        .bytes_stream()
        .try_filter(|v| future::ready(!v.is_empty()))
        .map_ok(Buffer::from)
        .map_err(move |err| {
            Error::new(ErrorKind::Unexpected, "read data from http response")
                .with_operation("cyper::fetch")
                .with_context("url", HttpUri::new(uri.to_string()).redacted_uri())
                .with_temporary(is_temporary_error(&err))
                .set_source(err)
        });
    Ok((
        builder.body(stream).expect("response headers are valid"),
        content_length,
    ))
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
