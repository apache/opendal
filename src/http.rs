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
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::channel::mpsc::{self, Sender};
use futures::{ready, Sink, StreamExt};
use http::Response;
use hyper::client::ResponseFuture;
use hyper::Body;

use crate::error::{Error, Result};
use crate::ops::OpWrite;

pub fn new_channel() -> (Sender<Bytes>, Body) {
    let (tx, rx) = mpsc::channel(0);

    (tx, Body::wrap_stream(rx.map(Ok::<_, Error>)))
}

pub struct BodySinker {
    op: OpWrite,
    tx: Sender<Bytes>,
    fut: ResponseFuture,
    handle: fn(&OpWrite, Response<Body>) -> Result<()>,
}

impl BodySinker {
    pub fn new(
        op: &OpWrite,
        tx: Sender<Bytes>,
        fut: ResponseFuture,
        handle: fn(&OpWrite, Response<Body>) -> Result<()>,
    ) -> BodySinker {
        BodySinker {
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
            // TODO: we need better error output.
            Poll::Ready(Err(e)) => Poll::Ready(Err(Error::Unexpected(anyhow!(e)))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<Bytes> for BodySinker {
    type Error = Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let this = &mut *self;

        if let Poll::Ready(v) = Pin::new(this).poll_response(cx) {
            unreachable!("response returned too early: {:?}", v)
        }

        self.tx
            .poll_ready(cx)
            .map_err(|e| Error::Unexpected(anyhow!(e)))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> std::result::Result<(), Self::Error> {
        let this = &mut *self;

        this.tx
            .start_send(item)
            .map_err(|e| Error::Unexpected(anyhow!(e)))
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
            return Poll::Ready(Err(Error::Unexpected(anyhow!(e))));
        }

        self.poll_response(cx)
    }
}
