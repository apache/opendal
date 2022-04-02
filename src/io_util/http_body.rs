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

use std::future::Future;
use std::io::Error;
use std::io::Result;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::{self};
use futures::ready;
use futures::Sink;
use futures::StreamExt;
use http::Response;
use hyper::client::ResponseFuture;
use hyper::Body;

use crate::error::other;
use crate::ops::OpWrite;

/// Create a HTTP channel.
///
/// # Example
///
/// ```no_run
/// use opendal::io_util::new_http_channel;
/// # use std::io::Result;
/// # use bytes::Bytes;
/// # use futures::SinkExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// # let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());
/// let (mut tx, body) = new_http_channel();
///
/// // Send request via another future.
/// let _ = tokio::spawn(async move {
///     let req = hyper::Request::put("https://httpbin.org/anything")
///         .body(body)
///         .expect("request must be valid");
///     client.request(req).await.expect("request must succeed");
/// });
///
///  tx.feed(Bytes::from("Hello, World!")).await.expect("send must succeed");
///  tx.close().await.expect("close must succeed");
/// # Ok(())
/// # }
/// ```
pub fn new_http_channel() -> (Sender<Bytes>, Body) {
    let (tx, rx) = mpsc::channel(0);

    (tx, Body::wrap_stream(rx.map(Ok::<_, Error>)))
}

/// Sink support for http channel.
pub struct HttpBodySinker {
    op: OpWrite,
    tx: Sender<Bytes>,
    fut: ResponseFuture,
    handle: fn(&OpWrite, Response<Body>) -> Result<()>,
}

impl HttpBodySinker {
    /// Create a HTTP body sinker.
    ///
    /// # Params
    ///
    /// - op: the OpWrite that input by `write` operation.
    /// - tx: the Sender created by [`new_http_channel`]
    /// - fut: the future created by HTTP client.
    /// - handle: the handle which parse response to result.
    ///
    /// # Example
    ///
    /// ```rust
    /// use opendal::io_util::new_http_channel;
    /// # use opendal::io_util::HttpBodySinker;
    /// # use http::StatusCode;
    /// # use bytes::Bytes;
    /// # use futures::SinkExt;
    /// # use opendal::ops::OpWrite;
    /// # use anyhow::anyhow;
    /// # use anyhow::Result;
    /// use std::io;
    /// use std::io::ErrorKind;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());
    /// # let op = OpWrite::default();
    /// let (mut tx, body) = new_http_channel();
    /// let req = hyper::Request::put("https://httpbin.org/anything")
    ///         .body(body)
    ///         .expect("request must be valid");
    ///
    /// let mut bs = HttpBodySinker::new(&op, tx, client.request(req), |op, resp| {
    ///     match resp.status() {
    ///         StatusCode::OK => Ok(()),
    ///         _ => Err(io::Error::from(ErrorKind::Other))
    ///     }
    /// });
    /// bs.send(Bytes::from("Hello, World!")).await?;
    /// bs.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(
        op: &OpWrite,
        tx: Sender<Bytes>,
        fut: ResponseFuture,
        handle: fn(&OpWrite, Response<Body>) -> Result<()>,
    ) -> HttpBodySinker {
        HttpBodySinker {
            op: op.clone(),
            tx,
            fut,
            handle,
        }
    }

    fn poll_response(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        match Pin::new(&mut self.fut).poll(cx) {
            Poll::Ready(Ok(resp)) => Poll::Ready((self.handle)(&self.op, resp)),
            // TODO: we need to inject an object error here.
            Poll::Ready(Err(e)) => Poll::Ready(Err(other(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<Bytes> for HttpBodySinker {
    type Error = Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = &mut *self;

        if let Poll::Ready(v) = Pin::new(this).poll_response(cx) {
            unreachable!("response returned too early: {:?}", v)
        }

        self.tx.poll_ready(cx).map_err(other)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> std::result::Result<(), Self::Error> {
        let this = &mut *self;

        this.tx.start_send(item).map_err(other)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
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
