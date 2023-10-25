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
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::raw::*;
use crate::*;

/// FileReader implements [`oio::Read`] via `AsyncRead + AsyncSeek`.
pub struct FileReader<R> {
    inner: R,

    start: u64,
    end: Option<u64>,

    offset: u64,
}

impl<R> FileReader<R> {
    /// Create a new FileReader.
    ///
    /// # Notes
    ///
    /// It's required that input reader's cursor is at the input `start` of the file.
    pub fn new(fd: R, start: u64, end: Option<u64>) -> FileReader<R> {
        FileReader {
            inner: fd,
            start,
            end,

            offset: start,
        }
    }

    fn calculate_position(&self, pos: SeekFrom) -> Result<SeekFrom> {
        match pos {
            SeekFrom::Start(n) => {
                if n < self.start {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }

                Ok(SeekFrom::Start(self.start + n))
            }
            SeekFrom::End(n) => {
                let end = if let Some(end) = self.end {
                    end as i64 + n
                } else {
                    n
                };

                if self.start as i64 + end < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }

                Ok(SeekFrom::End(end))
            }
            SeekFrom::Current(n) => {
                if self.offset as i64 + n < self.start as i64 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }

                Ok(SeekFrom::Current(n))
            }
        }
    }
}

impl<R> oio::Read for FileReader<R>
where
    R: AsyncRead + AsyncSeek + Unpin + Send + Sync,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let size = if let Some(end) = self.end {
            if self.offset >= end {
                return Poll::Ready(Ok(0));
            }
            cmp::min(buf.len(), (end - self.offset) as usize)
        } else {
            buf.len()
        };

        let n =
            ready!(Pin::new(&mut self.inner).poll_read(cx, &mut buf[..size])).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read data from FileReader").set_source(err)
            })?;
        self.offset += n as u64;
        Poll::Ready(Ok(n))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let pos = self.calculate_position(pos)?;

        let cur = ready!(Pin::new(&mut self.inner).poll_seek(cx, pos)).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "seek data from FileReader").set_source(err)
        })?;

        self.offset = cur;
        Poll::Ready(Ok(self.offset - self.start))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        let _ = cx;

        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support next",
        ))))
    }
}

impl<R> oio::BlockingRead for FileReader<R>
where
    R: Read + Seek + Send + Sync + 'static,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let size = if let Some(end) = self.end {
            if self.offset >= end {
                return Ok(0);
            }
            cmp::min(buf.len(), (end - self.offset) as usize)
        } else {
            buf.len()
        };

        let n = self.inner.read(&mut buf[..size]).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read data from FileReader").set_source(err)
        })?;
        self.offset += n as u64;
        Ok(n)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let pos = self.calculate_position(pos)?;

        let cur = self.inner.seek(pos).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "seek data from FileReader").set_source(err)
        })?;

        self.offset = cur;
        Ok(self.offset - self.start)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support iterating",
        )))
    }
}
