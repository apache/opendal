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
use futures::AsyncSeek;
use futures::{AsyncRead, Stream};
use pin_project_lite::pin_project;

use crate::raw::*;
use crate::*;

pin_project! {
    /// NucleiReader implements [`oio::Read`] via [`AsyncRead`] + [`AsyncSeek`].
    pub struct NucleiReader<R>
    where
        R: AsyncRead,
        R: AsyncSeek
    {
        #[pin]
        inner: R,

        seek_pos: Option<SeekFrom>,
    }
}

impl<R: AsyncRead + AsyncSeek> NucleiReader<R> {
    /// Create a new tokio reader.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            seek_pos: None,
        }
    }
}

impl<R> oio::Read for NucleiReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, mut buf: &mut [u8]) -> Poll<Result<usize>> {
        let this = Pin::new(self).project();

        this.inner.poll_read(cx, &mut buf).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Read)
                .with_context("source", "NucleiReader")
        })
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let mut this = Pin::new(self).project();
        let mut seek_pos = this.seek_pos.as_mut();

        let mut read_pos: u64 = 0;
        if seek_pos != Some(pos).as_mut() {
            let p = this.inner.poll_seek(cx, pos).map_err(|err| {
                new_std_io_error(err)
                    .with_operation(oio::ReadOperation::Seek)
                    .with_context("source", "NucleiReader")
            });

            match p {
                Poll::Ready(o) => match o {
                    Ok(x) => {
                        read_pos = x;
                        seek_pos = Some(pos).as_mut();
                    }
                    Err(err) => {
                        seek_pos = None.as_mut();
                    }
                },
                Poll::Pending => {}
            }

            // Ensure that next iteration comes with a position.
            seek_pos = Some(pos).as_mut()
        }

        Poll::Ready(Ok(read_pos))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "TokioReader doesn't support poll_next",
        ))))
    }
}
