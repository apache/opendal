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
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::raw::*;
use crate::*;

/// FuturesReader implements [`oio::Read`] via [`AsyncRead`] + [`AsyncSeek`].
pub struct FuturesReader<R: AsyncRead + AsyncSeek> {
    inner: R,
}

impl<R: AsyncRead + AsyncSeek> FuturesReader<R> {
    /// Create a new futures reader.
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R> oio::Read for FuturesReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Read)
                .with_context("source", "FuturesReader")
        })
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        Pin::new(&mut self.inner).poll_seek(cx, pos).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Seek)
                .with_context("source", "FuturesReader")
        })
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "FuturesReader doesn't support poll_next",
        ))))
    }
}
