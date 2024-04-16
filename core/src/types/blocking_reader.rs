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
use std::ops::Range;
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

    /// Read give range from reader into [`Buffer`].
    ///
    /// This operation is zero-copy, which means it keeps the [`Bytes`] returned by underlying
    /// storage services without any extra copy or intensive memory allocations.
    ///
    /// # Notes
    ///
    /// - Buffer length smaller than range means we have reached the end of file.
    pub fn read(&self, range: impl RangeBounds<u64>) -> Result<Buffer> {
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
                return Ok(Buffer::new());
            }
        }

        let mut bufs = Vec::new();
        let mut offset = start;

        loop {
            // TODO: use service preferred io size instead.
            let limit = end.map(|end| end - offset).unwrap_or(4 * 1024 * 1024) as usize;
            let bs = self.inner.read_at(offset, limit)?;
            let n = bs.remaining();
            bufs.push(bs);
            if n < limit {
                return Ok(bufs.into_iter().flatten().collect());
            }

            offset += n as u64;
            if Some(offset) == end {
                return Ok(bufs.into_iter().flatten().collect());
            }
        }
    }

    ///
    /// This operation will copy and write bytes into given [`BufMut`]. Allocation happens while
    /// [`BufMut`] doesn't have enough space.
    ///
    /// # Notes
    ///
    /// - Returning length smaller than range means we have reached the end of file.
    pub fn read_into(&self, buf: &mut impl BufMut, range: impl RangeBounds<u64>) -> Result<usize> {
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
        let mut read = 0;

        loop {
            // TODO: use service preferred io size instead.
            let limit = end.map(|end| end - offset).unwrap_or(4 * 1024 * 1024) as usize;
            let bs = self.inner.read_at(offset, limit)?;
            let n = bs.remaining();
            buf.put(bs);
            read += n as u64;
            if n < limit {
                return Ok(read as _);
            }

            offset += n as u64;
            if Some(offset) == end {
                return Ok(read as _);
            }
        }
    }

    /// Convert reader into [`StdReader`] which implements [`futures::AsyncRead`],
    /// [`futures::AsyncSeek`] and [`futures::AsyncBufRead`].
    #[inline]
    pub fn into_std_read(self, range: Range<u64>) -> StdReader {
        // TODO: the capacity should be decided by services.
        StdReader::new(self.inner, range)
    }

    /// Convert reader into [`StdBytesIterator`] which implements [`Iterator`].
    #[inline]
    pub fn into_bytes_iterator(self, range: Range<u64>) -> StdBytesIterator {
        StdBytesIterator::new(self.inner, range)
    }
}

pub mod into_std_read {
    use std::io;
    use std::io::BufRead;
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::ops::Range;

    use bytes::Buf;

    use crate::raw::*;
    use crate::*;

    /// StdReader is the adapter of [`Read`], [`Seek`] and [`BufRead`] for [`BlockingReader`][crate::BlockingReader].
    ///
    /// Users can use this adapter in cases where they need to use [`Read`] or [`BufRead`] trait.
    ///
    /// StdReader also implements [`Send`] and [`Sync`].
    pub struct StdReader {
        inner: oio::BlockingReader,
        offset: u64,
        size: u64,
        cap: usize,

        cur: u64,
        buf: Buffer,
    }

    impl StdReader {
        /// NOTE: don't allow users to create StdReader directly.
        #[inline]
        pub(super) fn new(r: oio::BlockingReader, range: Range<u64>) -> Self {
            StdReader {
                inner: r,
                offset: range.start,
                size: range.end - range.start,
                // TODO: should use services preferred io size.
                cap: 4 * 1024 * 1024,

                cur: 0,
                buf: Buffer::new(),
            }
        }

        /// Set the capacity of this reader to control the IO size.
        pub fn with_capacity(mut self, cap: usize) -> Self {
            self.cap = cap;
            self
        }
    }

    impl BufRead for StdReader {
        fn fill_buf(&mut self) -> io::Result<&[u8]> {
            if self.buf.has_remaining() {
                return Ok(self.buf.chunk());
            }

            // Make sure cur didn't exceed size.
            if self.cur >= self.size {
                return Ok(&[]);
            }

            let next_offset = self.offset + self.cur;
            let next_size = (self.size - self.cur).min(self.cap as u64) as usize;
            self.buf = self
                .inner
                .read_at(next_offset, next_size)
                .map_err(format_std_io_error)?;
            Ok(self.buf.chunk())
        }

        fn consume(&mut self, amt: usize) {
            self.buf.advance(amt);
            self.cur += amt as u64;
        }
    }

    impl Read for StdReader {
        #[inline]
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let bs = self.fill_buf()?;
            let n = bs.len().min(buf.len());
            buf[..n].copy_from_slice(&bs[..n]);
            self.consume(n);
            Ok(n)
        }
    }

    impl Seek for StdReader {
        #[inline]
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            let new_pos = match pos {
                SeekFrom::Start(pos) => pos as i64,
                SeekFrom::End(pos) => self.size as i64 + pos,
                SeekFrom::Current(pos) => self.cur as i64 + pos,
            };

            if new_pos < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid seek to a negative position",
                ));
            }

            let new_pos = new_pos as u64;

            if (self.cur..self.cur + self.buf.remaining() as u64).contains(&new_pos) {
                let cnt = new_pos - self.cur;
                self.buf.advance(cnt as _);
            } else {
                self.buf = Buffer::new()
            }

            self.cur = new_pos;
            Ok(self.cur)
        }
    }
}

pub mod into_std_iterator {
    use std::io;

    use bytes::Buf;
    use bytes::Bytes;

    use crate::raw::*;

    /// StdIterator is the adapter of [`Iterator`] for [`BlockingReader`][crate::BlockingReader].
    ///
    /// Users can use this adapter in cases where they need to use [`Iterator`] trait.
    ///
    /// StdIterator also implements [`Send`] and [`Sync`].
    pub struct StdBytesIterator {
        inner: oio::BlockingReader,
        offset: u64,
        size: u64,
        cap: usize,

        cur: u64,
    }

    impl StdBytesIterator {
        /// NOTE: don't allow users to create StdIterator directly.
        #[inline]
        pub(crate) fn new(r: oio::BlockingReader, range: std::ops::Range<u64>) -> Self {
            StdBytesIterator {
                inner: r,
                offset: range.start,
                size: range.end - range.start,
                // TODO: should use services preferred io size.
                cap: 4 * 1024 * 1024,
                cur: 0,
            }
        }

        /// Set the capacity of this reader to control the IO size.
        pub fn with_capacity(mut self, cap: usize) -> Self {
            self.cap = cap;
            self
        }
    }

    impl Iterator for StdBytesIterator {
        type Item = io::Result<Bytes>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.cur >= self.size {
                return None;
            }

            let next_offset = self.offset + self.cur;
            let next_size = (self.size - self.cur).min(self.cap as u64) as usize;
            match self.inner.read_at(next_offset, next_size) {
                Ok(buf) if !buf.has_remaining() => None,
                Ok(mut buf) => {
                    self.cur += buf.remaining() as u64;
                    Some(Ok(buf.copy_to_bytes(buf.remaining())))
                }
                Err(err) => Some(Err(format_std_io_error(err))),
            }
        }
    }
}
