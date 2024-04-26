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
