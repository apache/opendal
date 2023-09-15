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

use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::Bytes;
use futures::StreamExt;

use crate::raw::*;
use crate::*;

/// Convert given stream `futures::Stream<Item = Result<Bytes>>` into [`oio::Reader`].
pub fn into_read_from_stream<S>(stream: S) -> FromStreamReader<S> {
    FromStreamReader {
        inner: stream,
        buf: Bytes::new(),
    }
}

/// FromStreamReader will convert a `futures::Stream<Item = Result<Bytes>>` into `oio::Read`
pub struct FromStreamReader<S> {
    inner: S,
    buf: Bytes,
}

impl<S, T> oio::Read for FromStreamReader<S>
where
    S: futures::Stream<Item = Result<T>> + Send + Sync + Unpin + 'static,
    T: Into<Bytes>,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        if !self.buf.is_empty() {
            let len = std::cmp::min(buf.len(), self.buf.len());
            buf[..len].copy_from_slice(&self.buf[..len]);
            self.buf.advance(len);
            return Poll::Ready(Ok(len));
        }

        match futures::ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(bytes)) => {
                let bytes = bytes.into();
                let len = std::cmp::min(buf.len(), bytes.len());
                buf[..len].copy_from_slice(&bytes[..len]);
                self.buf = bytes.slice(len..);
                Poll::Ready(Ok(len))
            }
            Some(Err(err)) => Poll::Ready(Err(err)),
            None => Poll::Ready(Ok(0)),
        }
    }

    fn poll_seek(&mut self, _: &mut Context<'_>, _: SeekFrom) -> Poll<Result<u64>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "FromStreamReader can't support operation",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if !self.buf.is_empty() {
            return Poll::Ready(Some(Ok(std::mem::take(&mut self.buf))));
        }

        match futures::ready!(self.inner.poll_next_unpin(cx)) {
            Some(Ok(bytes)) => Poll::Ready(Some(Ok(bytes.into()))),
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
