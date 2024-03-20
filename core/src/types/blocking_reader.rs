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
use std::io::SeekFrom;
use std::ops::{Range, RangeBounds};
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::Stream;
use tokio::io::ReadBuf;

use crate::raw::oio::BlockingRead;
use crate::raw::*;
use crate::*;

/// BlockingReader is designed to read data from given path in an blocking
/// manner.
pub struct BlockingReader {
    pub(crate) inner: oio::BlockingReader,
}

impl BlockingReader {
    /// Create a new blocking reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) fn create(acc: FusedAccessor, path: &str, op: OpRead) -> crate::Result<Self> {
        let (_, r) = acc.blocking_read(path, op)?;

        Ok(BlockingReader { inner: r })
    }

    /// Create a new reader from an `oio::BlockingReader`.
    pub(crate) fn new(r: oio::BlockingReader) -> Self {
        BlockingReader { inner: r }
    }

    /// Read given range bytes of data from reader.
    pub fn read_at(&self, buf: &mut impl BufMut, offset: u64) -> crate::Result<usize> {
        let bs = self.inner.read_at(offset, buf.remaining_mut())?;
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }

    /// Read given range bytes of data from reader.
    pub fn read_range(&self, buf: &mut impl BufMut, range: Range<u64>) -> crate::Result<usize> {
        if range.is_empty() {
            return Ok(0);
        }
        let (mut offset, mut size) = (range.start, range.end - range.start);

        let mut read = 0;

        loop {
            let bs = self.inner.read_at(offset, size as usize)?;
            let n = bs.remaining();
            read += n;
            buf.put(bs);
            if n == 0 {
                return Ok(read);
            }

            offset += n as u64;

            debug_assert!(
                size >= n as u64,
                "read should not return more bytes than expected"
            );
            size -= n as u64;
            if size == 0 {
                return Ok(read);
            }
        }
    }

    pub fn read_to_end(&self, buf: &mut impl BufMut) -> crate::Result<usize> {
        self.read_to_end_at(buf, 0)
    }

    pub fn read_to_end_at(&self, buf: &mut impl BufMut, mut offset: u64) -> crate::Result<usize> {
        let mut size = 0;
        loop {
            // TODO: io size should be tuned based on storage
            let bs = self.inner.read_at(offset, 4 * 1024 * 1024)?;
            let n = bs.remaining();
            size += n;

            buf.put(bs);
            if n == 0 {
                return Ok(size);
            }

            offset += n as u64;
        }
    }
}

// impl io::Read for BlockingReader {
//     #[inline]
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         let bs = self.inner.read(buf.len()).map_err(format_std_io_error)?;
//         buf[..bs.len()].copy_from_slice(&bs);
//         Ok(bs.len())
//     }
// }
//
// impl io::Seek for BlockingReader {
//     #[inline]
//     fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
//         self.inner.seek(pos).map_err(format_std_io_error)
//     }
// }
//
// impl Iterator for BlockingReader {
//     type Item = io::Result<Bytes>;
//
//     #[inline]
//     fn next(&mut self) -> Option<Self::Item> {
//         match self
//             .inner
//             .read(4 * 1024 * 1024)
//             .map_err(format_std_io_error)
//         {
//             Ok(bs) if bs.is_empty() => None,
//             Ok(bs) => Some(Ok(bs)),
//             Err(err) => Some(Err(err)),
//         }
//     }
// }
