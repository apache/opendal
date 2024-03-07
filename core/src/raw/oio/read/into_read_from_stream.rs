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

use std::cmp::min;
use std::future::Future;
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
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if !self.buf.is_empty() {
            let len = std::cmp::min(buf.len(), self.buf.len());
            buf[..len].copy_from_slice(&self.buf[..len]);
            self.buf.advance(len);
            return Ok(len);
        }

        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let bytes = bytes.into();
                let len = std::cmp::min(buf.len(), bytes.len());
                buf[..len].copy_from_slice(&bytes[..len]);
                self.buf = bytes.slice(len..);
                Ok(len)
            }
            Some(Err(err)) => Err(err),
            None => Ok(0),
        }
    }

    async fn seek(&mut self, _: SeekFrom) -> Result<u64> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "FromStreamReader can't support operation",
        ))
    }

    async fn next(&mut self) -> Option<Result<Bytes>> {
        if !self.buf.is_empty() {
            return Some(Ok(std::mem::take(&mut self.buf)));
        }

        self.inner.next().await.map(|v| v.map(|v| v.into()))
    }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        if self.buf.is_empty() {
            self.buf = match self.inner.next().await.transpose()? {
                Some(v) => v.into(),
                None => return Ok(Bytes::new()),
            };
        }

        let bs = self.buf.split_to(min(size, self.buf.len()));
        Ok(bs)
    }
}
