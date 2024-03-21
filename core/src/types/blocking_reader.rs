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

use std::collections::Bound;
use std::ops::RangeBounds;

use bytes::Buf;
use bytes::BufMut;

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

    /// Read from underlying storage and write data into the specified buffer, starting at
    /// the given offset and up to the limit.
    ///
    /// A return value of `n` signifies that `n` bytes of data have been read into `buf`.
    /// If `n < limit`, it indicates that the reader has reached EOF (End of File).
    #[inline]
    pub fn read(&self, buf: &mut impl BufMut, offset: u64, limit: usize) -> Result<usize> {
        let bs = self.inner.read_at(offset, limit)?;
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }

    /// Read given range bytes of data from reader.
    pub fn read_range(&self, buf: &mut impl BufMut, range: impl RangeBounds<u64>) -> Result<usize> {
        let start = match range.start_bound().cloned() {
            Bound::Included(start) => start,
            Bound::Excluded(start) => start + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound().cloned() {
            Bound::Included(end) => Some(end + 1),
            Bound::Excluded(end) => Some(end),
            Bound::Unbounded => None,
        };

        // If range is empty, return Ok(0) directly.
        if let Some(end) = end {
            if end <= start {
                return Ok(0);
            }
        }

        let mut offset = start;
        let mut size = end.map(|end| end - start);

        let mut read = 0;
        loop {
            let bs = self
                .inner
                // TODO: use service preferred io size instead.
                .read_at(offset, size.unwrap_or(4 * 1024 * 1024) as usize)?;
            let n = bs.remaining();
            read += n;
            buf.put(bs);
            if n == 0 {
                return Ok(read);
            }

            offset += n as u64;

            size = size.map(|v| v - n as u64);
            if size == Some(0) {
                return Ok(read);
            }
        }
    }

    /// Read all data from reader.
    ///
    /// This API is exactly the same with `BlockingReader::read_range(buf, ..)`.
    #[inline]
    pub fn read_to_end(&self, buf: &mut impl BufMut) -> Result<usize> {
        self.read_range(buf, ..)
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
