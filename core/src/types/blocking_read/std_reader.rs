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
use std::ops::Range;
use std::sync::Arc;

use bytes::Buf;

use crate::raw::*;
use crate::*;

/// StdReader is the adapter of [`Read`], [`Seek`] and [`BufRead`] for [`BlockingReader`][crate::BlockingReader].
///
/// Users can use this adapter in cases where they need to use [`Read`] or [`BufRead`] trait.
///
/// StdReader also implements [`Send`] and [`Sync`].
pub struct StdReader {
    ctx: Arc<ReadContext>,

    iter: BufferIterator,
    buf: Buffer,
    start: u64,
    end: u64,
    pos: u64,
}

impl StdReader {
    /// NOTE: don't allow users to create StdReader directly.
    #[inline]
    pub(super) fn new(ctx: Arc<ReadContext>, range: Range<u64>) -> Self {
        let (start, end) = (range.start, range.end);
        let iter = BufferIterator::new(ctx.clone(), range);

        Self {
            ctx,
            iter,
            buf: Buffer::new(),
            start,
            end,
            pos: 0,
        }
    }
}

impl BufRead for StdReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        loop {
            if self.buf.has_remaining() {
                return Ok(self.buf.chunk());
            }

            self.buf = match self.iter.next().transpose().map_err(format_std_io_error)? {
                Some(buf) => buf,
                None => return Ok(&[]),
            };
        }
    }

    fn consume(&mut self, amt: usize) {
        self.buf.advance(amt);
        // Make sure buf has been dropped before starting new request.
        // Otherwise, we will hold those bytes in memory until next
        // buffer reaching.
        if self.buf.is_empty() {
            self.buf = Buffer::new();
        }
        self.pos += amt as u64;
    }
}

impl Read for StdReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.buf.remaining() > 0 {
                let size = self.buf.remaining().min(buf.len());
                self.buf.copy_to_slice(&mut buf[..size]);
                self.pos += size as u64;
                return Ok(size);
            }

            self.buf = match self.iter.next() {
                Some(Ok(buf)) => buf,
                Some(Err(err)) => return Err(format_std_io_error(err)),
                None => return Ok(0),
            };
        }
    }
}

impl Seek for StdReader {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(pos) => pos as i64,
            SeekFrom::End(pos) => self.end as i64 - self.start as i64 + pos,
            SeekFrom::Current(pos) => self.pos as i64 + pos,
        };

        // Check if new_pos is negative.
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid seek to a negative position",
            ));
        }

        let new_pos = new_pos as u64;

        if (self.pos..self.pos + self.buf.remaining() as u64).contains(&new_pos) {
            let cnt = new_pos - self.pos;
            self.buf.advance(cnt as _);
        } else {
            self.buf = Buffer::new();
            self.iter = BufferIterator::new(self.ctx.clone(), new_pos + self.start..self.end);
        }

        self.pos = new_pos;
        Ok(self.pos)
    }
}
