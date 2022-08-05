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

use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::future::Future;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use bytes::Bytes;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::ready;
use futures::AsyncRead;
use futures::AsyncWrite;
use futures::SinkExt;
use futures::StreamExt;
use http::response::Parts;
use http::Response;
use http::StatusCode;
use isahc::AsyncBody;
use log::debug;
use pin_project::pin_project;

use super::HttpResponseFuture;
use crate::error::other;
use crate::error::ObjectError;
use crate::io_util::into_reader;
use crate::ops::OpWrite;

/// Create a HTTP channel.
///
/// Read [`opendal::services::s3`]'s `write` implementations for more details.
pub fn new_http_channel(size: u64) -> (Sender<Bytes>, AsyncBody) {
    let (tx, rx) = mpsc::channel(0);

    (
        tx,
        AsyncBody::from_reader_sized(into_reader(rx.map(Ok::<_, Error>)), size),
    )
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
    Sending(HttpResponseFuture),
    /// this variant is 232 bytes.
    ParseError(Box<ParseErrorResponse>),
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
        fut: HttpResponseFuture,
        accepted_codes: HashSet<StatusCode>,
        error_parser: fn(StatusCode) -> ErrorKind,
    ) -> HttpBodyWriter {
        HttpBodyWriter {
            op: op.clone(),
            tx,
            state: State::Sending(fut),
            accepted_codes,
            error_parser,
        }
    }

    fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Error>> {
        let op = &self.op;
        let accepted_codes = &self.accepted_codes;

        match self.state.borrow_mut() {
            State::Sending(fut) => match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    if accepted_codes.contains(&resp.status()) {
                        debug!("object {} write finished: size {:?}", op.path(), op.size());
                        return Poll::Ready(Ok(()));
                    }

                    self.state = State::ParseError(Box::new(parse_error_response(
                        "write",
                        op.path(),
                        self.error_parser,
                        resp,
                    )));
                    self.poll_response(cx)
                }
                // TODO: we need to inject an object error here.
                Poll::Ready(Err(e)) => Poll::Ready(Err(other(e))),
                Poll::Pending => Poll::Pending,
            },
            State::ParseError(resp) => Poll::Ready(Err(ready!(Pin::new(resp).poll(cx)))),
        }
    }
}

impl AsyncWrite for HttpBodyWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        if let Poll::Ready(v) = (*self).poll_response(cx) {
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
        self.tx.poll_flush_unpin(cx).map_err(other)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(e) = ready!(self.tx.poll_close_unpin(cx)) {
            return Poll::Ready(Err(other(e)));
        }

        self.poll_response(cx)
    }
}

/// parse_error_response will try to read and parse error response.
pub fn parse_error_response(
    op: &'static str,
    path: &str,
    parser: fn(StatusCode) -> ErrorKind,
    resp: Response<isahc::AsyncBody>,
) -> ParseErrorResponse {
    let (parts, body) = resp.into_parts();

    ParseErrorResponse {
        op,
        path: path.to_string(),
        parser,
        parts,
        body,
        buf: Vec::with_capacity(1024),
    }
}

pub struct ParseErrorResponse {
    op: &'static str,
    path: String,
    parser: fn(StatusCode) -> ErrorKind,
    parts: Parts,
    body: isahc::AsyncBody,

    buf: Vec<u8>,
}

impl Future for ParseErrorResponse {
    type Output = Error;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut data = vec![0; 1024];
        match ready!(Pin::new(&mut self.body).poll_read(cx, &mut data)) {
            Ok(0) => Poll::Ready(Error::new(
                (self.parser)(self.parts.status),
                ObjectError::new(
                    self.op,
                    &self.path,
                    anyhow!(
                        "status code: {:?}, headers: {:?}, body: {:?}",
                        self.parts.status,
                        self.parts.headers,
                        String::from_utf8_lossy(&self.buf)
                    ),
                ),
            )),
            Ok(size) => {
                // Only read 4KiB from the response to avoid broken services.
                if self.buf.len() < 4 * 1024 {
                    self.buf.extend_from_slice(&data[..size]);
                }

                // Make sure the whole body consumed, even we don't need them.
                self.poll(cx)
            }
            Err(e) => Poll::Ready(Error::new(
                (self.parser)(self.parts.status),
                ObjectError::new(
                    self.op,
                    &self.path,
                    anyhow!(
                        "status code: {:?}, headers: {:?}, read body: {:?}, remaining {:?}",
                        self.parts.status,
                        self.parts.headers,
                        String::from_utf8_lossy(&self.buf),
                        e
                    ),
                ),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::SinkExt;
    use isahc::AsyncReadResponseExt;
    use serde::Deserialize;

    use super::*;
    use crate::io_util::HttpClient;

    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct HttpBin {
        data: String,
    }

    #[tokio::test]
    async fn test_http_channel() {
        let (mut tx, body) = new_http_channel(13);

        let fut = tokio::spawn(async {
            let client = HttpClient::new();
            let req = isahc::Request::put("https://httpbin.org/anything")
                .body(body)
                .expect("request must be valid");
            let mut resp = client.send_async(req).await.expect("request must succeed");
            let bs = resp.bytes().await.expect("read body must succeed");
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
