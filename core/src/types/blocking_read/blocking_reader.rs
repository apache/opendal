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
use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;

use crate::raw::*;
use crate::*;

/// BlockingReader is designed to read data from given path in an blocking
/// manner.
pub struct BlockingReader {
    acc: Accessor,
    path: Arc<String>,

    pub(crate) inner: oio::BlockingReader,

    /// Total size of the reader.
    size: Arc<AtomicContentLength>,
}

impl BlockingReader {
    /// Create a new blocking reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) fn create(acc: Accessor, path: Arc<String>, op: OpRead) -> crate::Result<Self> {
        let (_, r) = acc.blocking_read(&path, op)?;

        Ok(BlockingReader {
            acc,
            path,
            inner: r,
            size: Arc::new(AtomicContentLength::new()),
        })
    }

    /// Parse users input range bounds into valid `Range<u64>`.
    ///
    /// To avoid duplicated stat call, we will cache the size of the reader.
    fn parse_range(&self, range: impl RangeBounds<u64>) -> Result<Range<u64>> {
        let start = match range.start_bound() {
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v + 1,
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(v) => v + 1,
            Bound::Excluded(v) => *v,
            Bound::Unbounded => match self.size.load() {
                Some(v) => v,
                None => {
                    let size = self
                        .acc
                        .blocking_stat(&self.path, OpStat::new())?
                        .into_metadata()
                        .content_length();
                    self.size.store(size);
                    size
                }
            },
        };

        Ok(start..end)
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
        let range = self.parse_range(range)?;
        let start = range.start;
        let end = range.end;

        let mut bufs = Vec::new();
        let mut offset = start;

        loop {
            // TODO: use service preferred io size instead.
            let size = (end - offset) as usize;
            let bs = self.inner.read()?;
            let n = bs.remaining();
            bufs.push(bs);
            if n < size {
                return Ok(bufs.into_iter().flatten().collect());
            }

            offset += n as u64;
            if offset == end {
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
        let range = self.parse_range(range)?;
        let start = range.start;
        let end = range.end;

        let mut offset = start;
        let mut read = 0;

        loop {
            // TODO: use service preferred io size instead.
            let size = (end - offset) as usize;
            let bs = self.inner.read()?;
            let n = bs.remaining();
            buf.put(bs);
            read += n as u64;
            offset += n as u64;
            if offset == end {
                return Ok(read as _);
            }
        }
    }

    /// Convert reader into [`StdReader`] which implements [`futures::AsyncRead`],
    /// [`futures::AsyncSeek`] and [`futures::AsyncBufRead`].
    #[inline]
    pub fn into_std_read(self, range: impl RangeBounds<u64>) -> Result<StdReader> {
        let range = self.parse_range(range)?;
        Ok(StdReader::new(self.inner, range))
    }

    /// Convert reader into [`StdBytesIterator`] which implements [`Iterator`].
    #[inline]
    pub fn into_bytes_iterator(self, range: impl RangeBounds<u64>) -> Result<StdBytesIterator> {
        let range = self.parse_range(range)?;
        Ok(StdBytesIterator::new(self.inner, range))
    }
}
