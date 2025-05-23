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

use std::ops::RangeBounds;

use bytes::BufMut;

use crate::Reader as AsyncReader;
use crate::*;

use super::BufferIterator;
use super::StdBytesIterator;
use super::StdReader;

/// BlockingReader is designed to read data from given path in an blocking
/// manner.
#[derive(Clone)]
pub struct Reader {
    handle: tokio::runtime::Handle,
    inner: AsyncReader,
}

impl Reader {
    /// Create a new blocking reader.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) fn new(handle: tokio::runtime::Handle, inner: AsyncReader) -> Self {
        Reader { handle, inner }
    }

    /// Read give range from reader into [`Buffer`].
    ///
    /// This operation is zero-copy, which means it keeps the [`bytes::Bytes`] returned by underlying
    /// storage services without any extra copy or intensive memory allocations.
    ///
    /// # Notes
    ///
    /// - Buffer length smaller than range means we have reached the end of file.
    pub fn read(&self, range: impl RangeBounds<u64>) -> Result<Buffer> {
        self.handle.block_on(self.inner.read(range))
    }

    ///
    /// This operation will copy and write bytes into given [`BufMut`]. Allocation happens while
    /// [`BufMut`] doesn't have enough space.
    ///
    /// # Notes
    ///
    /// - Returning length smaller than range means we have reached the end of file.
    pub fn read_into(&self, buf: &mut impl BufMut, range: impl RangeBounds<u64>) -> Result<usize> {
        self.handle.block_on(self.inner.read_into(buf, range))
    }

    /// Create a buffer iterator to read specific range from given reader.
    pub fn into_iterator(self, range: impl RangeBounds<u64>) -> Result<BufferIterator> {
        let iter = self.handle.block_on(self.inner.into_stream(range))?;

        Ok(BufferIterator::new(self.handle.clone(), iter))
    }

    /// Convert reader into [`StdReader`] which implements [`futures::AsyncRead`],
    /// [`futures::AsyncSeek`] and [`futures::AsyncBufRead`].
    #[inline]
    pub fn into_std_read(self, range: impl RangeBounds<u64>) -> Result<StdReader> {
        let r = self
            .handle
            .block_on(self.inner.into_futures_async_read(range))?;

        Ok(StdReader::new(self.handle, r))
    }

    /// Convert reader into [`StdBytesIterator`] which implements [`Iterator`].
    #[inline]
    pub fn into_bytes_iterator(self, range: impl RangeBounds<u64>) -> Result<StdBytesIterator> {
        let iter = self.handle.block_on(self.inner.into_bytes_stream(range))?;
        Ok(StdBytesIterator::new(self.handle, iter))
    }
}
