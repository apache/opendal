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

use std::io;
use std::io::BufRead;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use futures::AsyncBufReadExt;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;

use crate::raw::*;
use crate::*;

/// StdReader is the adapter of [`Read`], [`Seek`] and [`BufRead`] for [`BlockingReader`][crate::BlockingReader].
///
/// Users can use this adapter in cases where they need to use [`Read`] or [`BufRead`] trait.
///
/// StdReader also implements [`Send`] and [`Sync`].
pub struct StdReader {
    handle: tokio::runtime::Handle,
    r: FuturesAsyncReader,
}

impl StdReader {
    /// NOTE: don't allow users to create StdReader directly.
    #[inline]
    pub(super) fn new(handle: tokio::runtime::Handle, r: FuturesAsyncReader) -> Self {
        Self { handle, r }
    }
}

impl BufRead for StdReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.handle.block_on(self.r.fill_buf())
    }

    fn consume(&mut self, amt: usize) {
        self.r.consume(amt);
    }
}

impl Read for StdReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.handle.block_on(self.r.read(buf))
    }
}

impl Seek for StdReader {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.handle.block_on(self.r.seek(pos))
    }
}
