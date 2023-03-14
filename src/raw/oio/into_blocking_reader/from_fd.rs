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

use std::cmp;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use bytes::Bytes;

use crate::raw::*;
use crate::*;

/// Convert given fd into [`oio::BlockingRead`].
pub fn from_fd<R>(fd: R, start: u64, end: u64) -> FdReader<R>
where
    R: Read + Seek + Send + Sync,
{
    FdReader {
        inner: fd,
        start,
        end,
        offset: 0,
    }
}

/// FdReader is a wrapper of input fd to implement [`oio::Read`].
pub struct FdReader<R: Read + Seek + Send + Sync> {
    inner: R,

    start: u64,
    end: u64,
    offset: u64,
}

impl<R> FdReader<R>
where
    R: Read + Seek + Send + Sync,
{
    #[inline]
    pub(crate) fn current_size(&self) -> i64 {
        debug_assert!(self.offset >= self.start, "offset must in range");
        self.end as i64 - self.offset as i64
    }
}

impl<R> oio::BlockingRead for FdReader<R>
where
    R: Read + Seek + Send + Sync + 'static,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.current_size() <= 0 {
            return Ok(0);
        }

        let max = cmp::min(buf.len() as u64, self.current_size() as u64) as usize;
        // TODO: we can use pread instead.
        let n = self.inner.read(&mut buf[..max]).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read data from FdReader")
                .with_context("source", "FdReader")
                .set_source(err)
        })?;
        self.offset += n as u64;
        Ok(n)
    }

    /// TODO: maybe we don't need to do seek really, just call pread instead.
    ///
    /// We need to wait for tokio's pread support.
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let (base, offset) = match pos {
            SeekFrom::Start(n) => (self.start as i64, n as i64),
            SeekFrom::End(n) => (self.end as i64, n),
            SeekFrom::Current(n) => (self.offset as i64, n),
        };

        match base.checked_add(offset) {
            Some(n) if n < 0 => Err(Error::new(
                ErrorKind::Unexpected,
                "invalid seek to a negative or overflowing position",
            )),
            Some(n) => {
                let cur = self.inner.seek(SeekFrom::Start(n as u64)).map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "seek data from FdReader")
                        .with_context("source", "FdReader")
                        .set_source(err)
                })?;

                self.offset = cur;
                Ok(self.offset - self.start)
            }
            None => Err(Error::new(
                ErrorKind::Unexpected,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support iterating",
        )))
    }
}
