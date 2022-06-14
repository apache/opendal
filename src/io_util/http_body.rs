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

use anyhow::anyhow;
use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::future::Future;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::{self};
use futures::ready;
use futures::AsyncWrite;
use futures::Sink;
use futures::StreamExt;
use hyper::body::HttpBody;
use hyper::client::ResponseFuture;
use hyper::Body;
use log::debug;
use pin_project::pin_project;

use crate::error::{other, ObjectError};
use crate::ops::OpWrite;

/// Create a HTTP channel.
///
/// Read [`opendal::services::s3`]'s `write` implementations for more details.
pub fn new_http_channel() -> (Sender<Bytes>, Body) {
    let (tx, rx) = mpsc::channel(0);

    (tx, Body::wrap_stream(rx.map(Ok::<_, Error>)))
}

#[pin_project]
pub struct HttpBodyWriter {
    op: OpWrite,
    tx: Sender<Bytes>,
    state: State,
    accepted_codes: HashSet<http::StatusCode>,
    error_parser: fn(http::StatusCode) -> ErrorKind,
}

enum State {
    Sending(ResponseFuture),
    Receiving(http::response::Parts, Body, Vec<u8>),
}

impl HttpBodyWriter {
    /// Create a HTTP body writer.
    ///
    /// # Params
    ///
    /// - op: the OpWrite that input by `write` operation.
    /// - tx: the Sender created by [`new_http_channel`]
    /// - fut: the future created by HTTP client.
    /// - handle: the handle which parse response to result.
    ///
    /// Read [`opendal::services::s3`]'s `write` implementations for more details.
    pub fn new(
        op: &OpWrite,
        tx: Sender<Bytes>,
        fut: ResponseFuture,
        accepted_codes: HashSet<http::StatusCode>,
        error_parser: fn(http::StatusCode) -> ErrorKind,
    ) -> HttpBodyWriter {
        HttpBodyWriter {
            op: op.clone(),
            tx,
            state: State::Sending(fut),
            accepted_codes,
            error_parser,
        }
    }

    fn poll_response(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        let op = self.op.clone();
        let accepted_codes = self.accepted_codes.clone();
        let error_parser = self.error_parser;

        match self.state.borrow_mut() {
            State::Sending(fut) => match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    let (parts, body) = resp.into_parts();
                    self.state = State::Receiving(parts, body, Vec::new());
                    self.poll_response(cx)
                }
                // TODO: we need to inject an object error here.
                Poll::Ready(Err(e)) => Poll::Ready(Err(other(e))),
                Poll::Pending => Poll::Pending,
            },
            State::Receiving(parts, body, bs) => {
                if accepted_codes.contains(&parts.status) {
                    debug!("object {} write finished: size {:?}", op.path(), op.size());
                    return Poll::Ready(Ok(()));
                }

                match ready!(Pin::new(body).poll_data(cx)) {
                    None => Poll::Ready(Err(Error::new(
                        (error_parser)(parts.status),
                        ObjectError::new(
                            "write",
                            op.path(),
                            anyhow!(
                                "response parts: {:?}, body: {:?}",
                                parts,
                                String::from_utf8_lossy(bs)
                            ),
                        ),
                    ))),
                    Some(Ok(data)) => {
                        bs.extend_from_slice(&data);
                        // Only read 4KiB from the response to avoid broken services.
                        if bs.len() >= 4 * 1024 {
                            Poll::Ready(Err(Error::new(
                                (error_parser)(parts.status),
                                ObjectError::new(
                                    "write",
                                    op.path(),
                                    anyhow!(
                                        "response parts: {:?}, body: {:?}",
                                        parts,
                                        String::from_utf8_lossy(bs)
                                    ),
                                ),
                            )))
                        } else {
                            self.poll_response(cx)
                        }
                    }
                    Some(Err(e)) => Poll::Ready(Err(Error::new(
                        (error_parser)(parts.status),
                        ObjectError::new(
                            "write",
                            op.path(),
                            anyhow!(
                                "error response parts: {:?}, read body: {:?}, remaining {:?}",
                                parts,
                                String::from_utf8_lossy(bs),
                                e
                            ),
                        ),
                    ))),
                }
            }
        }
    }
}

impl AsyncWrite for HttpBodyWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        if let Poll::Ready(v) = Pin::new(&mut *self).poll_response(cx) {
            unreachable!("response returned too early: {:?}", v)
        }

        ready!(self.tx.poll_ready(cx).map_err(other))?;

        let size = buf.len();
        self.tx
            .start_send(Bytes::from(buf.to_vec()))
            .map_err(other)?;

        Poll::Ready(Ok(size))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.tx).poll_flush(cx).map_err(other)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(e) = ready!(Pin::new(&mut self.tx).poll_close(cx)) {
            return Poll::Ready(Err(other(e)));
        }

        self.poll_response(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures::SinkExt;
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct HttpBin {
        data: String,
    }

    #[tokio::test]
    async fn test_http_channel() {
        let (mut tx, body) = new_http_channel();

        let fut = tokio::spawn(async {
            let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());
            let req = hyper::Request::put("https://httpbin.org/anything")
                .body(body)
                .expect("request must be valid");
            let resp = client.request(req).await.expect("request must succeed");
            let bs = hyper::body::to_bytes(resp.into_body())
                .await
                .expect("read body must succeed");
            serde_json::from_slice::<HttpBin>(&bs).expect("deserialize must succeed")
        });

        tx.feed(Bytes::from("Hello, World!"))
            .await
            .expect("feed must succeed");
        tx.close().await.expect("close must succeed");

        let content = fut.await.expect("future must polled");
        assert_eq!(&content.data, "Hello, World!")
    }
}
