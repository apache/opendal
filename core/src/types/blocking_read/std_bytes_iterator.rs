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
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::raw::*;
use crate::Buffer;
use crate::BufferIterator;
use crate::*;

/// StdIterator is the adapter of [`Iterator`] for [`BlockingReader`][crate::BlockingReader].
///
/// Users can use this adapter in cases where they need to use [`Iterator`] trait.
///
/// StdIterator also implements [`Send`] and [`Sync`].
pub struct StdBytesIterator {
    iter: BufferIterator,
    buf: Buffer,
}

impl StdBytesIterator {
    /// NOTE: don't allow users to create StdIterator directly.
    #[inline]
    pub(crate) fn new(ctx: Arc<ReadContext>, range: Range<u64>) -> Self {
        let iter = BufferIterator::new(ctx, range);

        StdBytesIterator {
            iter,
            buf: Buffer::new(),
        }
    }
}

impl Iterator for StdBytesIterator {
    type Item = io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Consume current buffer
            if let Some(bs) = Iterator::next(&mut self.buf) {
                return Some(Ok(bs));
            }

            self.buf = match self.iter.next() {
                Some(Ok(buf)) => buf,
                Some(Err(err)) => return Some(Err(format_std_io_error(err))),
                None => return None,
            };
        }
    }
}
