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
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio::io::AsyncSeek;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

/// FuturesReader implements [`oio::Read`] via [`AsyncRead`] + [`AsyncSeek`].
pub struct TokioReader<R: AsyncRead + AsyncSeek> {
    inner: R,

    seek_pos: Option<SeekFrom>,
}

impl<R: AsyncRead + AsyncSeek> TokioReader<R> {
    /// Create a new tokio reader.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            seek_pos: None,
        }
    }
}

impl<R> oio::Read for TokioReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let mut buf = ReadBuf::new(buf);

        ready!(Pin::new(&mut self.inner).poll_read(cx, &mut buf)).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Read)
                .with_context("source", "TokioReader")
        })?;

        Poll::Ready(Ok(buf.filled().len()))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        if self.seek_pos != Some(pos) {
            Pin::new(&mut self.inner).start_seek(pos).map_err(|err| {
                new_std_io_error(err)
                    .with_operation(oio::ReadOperation::Seek)
                    .with_context("source", "TokioReader")
            })?;
            self.seek_pos = Some(pos)
        }

        // NOTE: don't return error by `?` here, we need to reset seek_pos.
        let pos = ready!(Pin::new(&mut self.inner).poll_complete(cx).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Seek)
                .with_context("source", "TokioReader")
        }));
        self.seek_pos = None;
        Poll::Ready(pos)
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "TokioReader doesn't support poll_next",
        ))))
    }
}
