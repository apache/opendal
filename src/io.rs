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

use std::io::SeekFrom;
use std::pin::Pin;

use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncSeek};

pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Unpin + Send {}

impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncReadSeek for T {}

#[pin_project::pin_project]
pub struct Reader {
    #[pin]
    inner: Box<dyn AsyncRead + Unpin + Send>,
}

impl Reader {
    pub fn new(inner: Box<dyn AsyncRead + Unpin + Send>) -> Self {
        Self { inner }
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        self.project().inner.poll_read(cx, buf)
    }
}

#[pin_project::pin_project]
pub struct StatefulReader {
    #[pin]
    inner: Box<dyn AsyncReadSeek + Unpin + Send>,
}

impl StatefulReader {
    pub fn new(inner: Box<dyn AsyncReadSeek + Unpin + Send>) -> Self {
        Self { inner }
    }
}

impl AsyncRead for StatefulReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl AsyncSeek for StatefulReader {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        self.project().inner.poll_seek(cx, pos)
    }
}
