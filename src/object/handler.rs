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

use std::io;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::AsyncRead;
use futures::AsyncSeek;

use crate::raw::*;

/// ObjectHandler is the handler to read and seek a file.
pub struct ObjectHandler(BytesHandler);

impl ObjectHandler {
    pub(crate) fn new(bh: BytesHandler) -> Self {
        Self(bh)
    }
}

impl AsyncRead for ObjectHandler {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.0.as_mut()).poll_read(cx, buf)
    }
}

impl AsyncSeek for ObjectHandler {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        Pin::new(self.0.as_mut()).poll_seek(cx, pos)
    }
}

/// BlockingObjectHandler is the handler to read and seek a file.
pub struct BlockingObjectHandler(BlockingBytesHandler);

impl BlockingObjectHandler {
    pub(crate) fn new(bh: BlockingBytesHandler) -> Self {
        Self(bh)
    }
}

impl std::io::Read for BlockingObjectHandler {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl std::io::Seek for BlockingObjectHandler {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}
