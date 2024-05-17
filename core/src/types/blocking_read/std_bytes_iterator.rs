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
use std::io;
use std::ops::RangeBounds;

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
    end: u64,
    cap: usize,

    cur: u64,
}

impl StdBytesIterator {
    /// NOTE: don't allow users to create StdIterator directly.
    #[inline]
    pub(crate) fn new(r: oio::BlockingReader, range: impl RangeBounds<u64>) -> Self {
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

        StdBytesIterator {
            inner: r,
            offset: start,
            end: end.unwrap_or(u64::MAX),
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
        if self.offset + self.cur >= self.end {
            return None;
        }

        let next_offset = self.offset + self.cur;
        let next_size = (self.end - self.offset).min(self.cap as u64) as usize;
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
