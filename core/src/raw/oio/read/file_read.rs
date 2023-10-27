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

use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::Future;

use crate::raw::*;
use crate::*;

/// FileReader that implement range read and streamable read on seekable reader.
///
/// `oio::Reader` requires the underlying reader to handle range correctly and have streamable support.
/// But some services like `fs`, `hdfs` only have seek support. FileReader implements range and stream
/// support based on `seek`. We will maintain the correct range for give file and implement streamable
/// operations based on [`oio::AdaptiveBuf`].
pub struct FileReader<A: Accessor, R> {
    acc: Arc<A>,
    path: Arc<String>,
    op: OpRead,

    offset: Option<u64>,
    size: Option<u64>,
    cur: u64,

    buf: oio::AdaptiveBuf,
    state: State<R>,
}

enum State<R> {
    Idle,
    Send(BoxFuture<'static, Result<(RpRead, R)>>),
    Read(R),
}

/// Safety: State will only be accessed under &mut.
unsafe impl<R> Sync for State<R> {}

impl<A, R> FileReader<A, R>
where
    A: Accessor,
{
    /// Create a new FileReader.
    ///
    /// # Notes
    ///
    /// It's required that input reader's cursor is at the input `start` of the file.
    pub fn new(acc: Arc<A>, path: &str, op: OpRead) -> FileReader<A, R> {
        FileReader {
            acc,
            path: Arc::new(path.to_string()),
            op,

            offset: None,
            size: None,
            cur: 0,
            buf: oio::AdaptiveBuf::default(),
            state: State::<R>::Idle,
        }
    }

    /// Calculate the actual position that we should seek to.
    fn calculate_position(
        offset: Option<u64>,
        size: Option<u64>,
        cur: u64,
        pos: SeekFrom,
    ) -> Result<SeekFrom> {
        let offset = offset.expect("offset should be set for calculate_position");

        match pos {
            SeekFrom::Start(n) => {
                // It's valid for user to seek outsides end of the file.
                Ok(SeekFrom::Start(offset + n))
            }
            SeekFrom::End(n) => {
                if let Some(size) = size {
                    if size as i64 + n < 0 {
                        return Err(Error::new(
                            ErrorKind::InvalidInput,
                            "seek to a negative position is invalid",
                        )
                        .with_context("position", format!("{pos:?}")));
                    }
                    // size is known, we can convert SeekFrom::End into SeekFrom::Start.
                    Ok(SeekFrom::Start(offset + (size as i64 + n) as u64))
                } else {
                    // size unknown means we can forward seek end to underlying reader directly.
                    Ok(SeekFrom::End(n))
                }
            }
            SeekFrom::Current(n) => {
                if cur as i64 + n < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }
                Ok(SeekFrom::Start(offset + (cur as i64 + n) as u64))
            }
        }
    }
}

impl<A, R> FileReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn read_future(&self) -> BoxFuture<'static, Result<(RpRead, R)>> {
        let acc = self.acc.clone();
        let path = self.path.clone();

        // FileReader doesn't support range, we will always use full range to open a file.
        let op = self.op.clone().with_range(BytesRange::from(..));

        Box::pin(async move { acc.read(&path, op).await })
    }

    /// calculate_offset will make sure that the offset has been set.
    fn poll_offset(
        cx: &mut Context<'_>,
        r: &mut R,
        range: BytesRange,
    ) -> Poll<Result<(Option<u64>, Option<u64>)>> {
        let (offset, size) = match (range.offset(), range.size()) {
            (None, None) => (0, None),
            (None, Some(size)) => {
                let start = ready!(r.poll_seek(cx, SeekFrom::End(-(size as i64))))?;
                (start, Some(size))
            }
            (Some(offset), None) => {
                let start = ready!(r.poll_seek(cx, SeekFrom::Start(offset)))?;
                (start, None)
            }
            (Some(offset), Some(size)) => {
                let start = ready!(r.poll_seek(cx, SeekFrom::Start(offset)))?;
                (start, Some(size))
            }
        };

        Poll::Ready(Ok((Some(offset), size)))
    }
}

impl<A, R> FileReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    /// calculate_offset will make sure that the offset has been set.
    fn calculate_offset(r: &mut R, range: BytesRange) -> Result<(Option<u64>, Option<u64>)> {
        let (offset, size) = match (range.offset(), range.size()) {
            (None, None) => (0, None),
            (None, Some(size)) => {
                let start = r.seek(SeekFrom::End(-(size as i64)))?;
                (start, Some(size))
            }
            (Some(offset), None) => {
                let start = r.seek(SeekFrom::Start(offset))?;
                (start, None)
            }
            (Some(offset), Some(size)) => {
                let start = r.seek(SeekFrom::Start(offset))?;
                (start, Some(size))
            }
        };

        Ok((Some(offset), size))
    }
}

impl<A, R> oio::Read for FileReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        match &mut self.state {
            State::Idle => {
                self.state = State::Send(self.read_future());
                self.poll_read(cx, buf)
            }
            State::Send(fut) => {
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If send future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;
                self.state = State::Read(r);
                self.poll_read(cx, buf)
            }
            State::Read(r) => {
                // We should know where to start read the data.
                if self.offset.is_none() {
                    (self.offset, self.size) = ready!(Self::poll_offset(cx, r, self.op.range()))?;
                }

                let size = if let Some(size) = self.size {
                    // Sanity check.
                    if self.cur >= size {
                        return Poll::Ready(Ok(0));
                    }
                    cmp::min(buf.len(), (size - self.cur) as usize)
                } else {
                    buf.len()
                };

                match ready!(r.poll_read(cx, &mut buf[..size])) {
                    Ok(0) => Poll::Ready(Ok(0)),
                    Ok(n) => {
                        self.cur += n as u64;
                        Poll::Ready(Ok(n))
                    }
                    // We don't need to reset state here since it's ok to poll the same reader.
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
        }
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        match &mut self.state {
            State::Idle => {
                self.state = State::Send(self.read_future());
                self.poll_seek(cx, pos)
            }
            State::Send(fut) => {
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If send future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;
                self.state = State::Read(r);
                self.poll_seek(cx, pos)
            }
            State::Read(r) => {
                // We should know where to start read the data.
                if self.offset.is_none() {
                    (self.offset, self.size) = ready!(Self::poll_offset(cx, r, self.op.range()))?;
                }

                let pos = Self::calculate_position(self.offset, self.size, self.cur, pos)?;
                let cur = ready!(r.poll_seek(cx, pos))?;
                if cur < self.offset.unwrap() {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}"))));
                }
                self.cur = cur - self.offset.unwrap();
                Poll::Ready(Ok(self.cur))
            }
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match &mut self.state {
            State::Idle => {
                self.state = State::Send(self.read_future());
                self.poll_next(cx)
            }
            State::Send(fut) => {
                let (_, r) = ready!(Pin::new(fut).poll(cx)).map_err(|err| {
                    // If send future returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.state = State::Idle;
                    err
                })?;
                self.state = State::Read(r);
                self.poll_next(cx)
            }
            State::Read(r) => {
                // We should know where to start read the data.
                if self.offset.is_none() {
                    (self.offset, self.size) = ready!(Self::poll_offset(cx, r, self.op.range()))?;
                }

                self.buf.reserve();

                let mut buf = self.buf.initialized_mut();
                let buf = buf.initialized_mut();

                let size = if let Some(size) = self.size {
                    // Sanity check.
                    if self.cur >= size {
                        return Poll::Ready(None);
                    }
                    cmp::min(buf.len(), (size - self.cur) as usize)
                } else {
                    buf.len()
                };

                match ready!(r.poll_read(cx, &mut buf[..size])) {
                    Ok(0) => Poll::Ready(None),
                    Ok(n) => {
                        self.cur += n as u64;
                        self.buf.record(n);
                        Poll::Ready(Some(Ok(self.buf.split(n))))
                    }
                    // We don't need to reset state here since it's ok to poll the same reader.
                    Err(err) => Poll::Ready(Some(Err(err))),
                }
            }
        }
    }
}

impl<A, R> oio::BlockingRead for FileReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.state {
            State::Idle => {
                // FileReader doesn't support range, we will always use full range to open a file.
                let op = self.op.clone().with_range(BytesRange::from(..));

                let (_, r) = self.acc.blocking_read(&self.path, op)?;
                self.state = State::Read(r);
                self.read(buf)
            }

            State::Read(r) => {
                // We should know where to start read the data.
                if self.offset.is_none() {
                    (self.offset, self.size) = Self::calculate_offset(r, self.op.range())?;
                }

                let size = if let Some(size) = self.size {
                    // Sanity check.
                    if self.cur >= size {
                        return Ok(0);
                    }
                    cmp::min(buf.len(), (size - self.cur) as usize)
                } else {
                    buf.len()
                };

                match r.read(&mut buf[..size]) {
                    Ok(0) => Ok(0),
                    Ok(n) => {
                        self.cur += n as u64;
                        Ok(n)
                    }
                    // We don't need to reset state here since it's ok to poll the same reader.
                    Err(err) => Err(err),
                }
            }
            State::Send(_) => {
                unreachable!(
                    "It's invalid to go into State::Send for BlockingRead, please report this bug"
                )
            }
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match &mut self.state {
            State::Idle => {
                // FileReader doesn't support range, we will always use full range to open a file.
                let op = self.op.clone().with_range(BytesRange::from(..));

                let (_, r) = self.acc.blocking_read(&self.path, op)?;
                self.state = State::Read(r);
                self.seek(pos)
            }
            State::Read(r) => {
                // We should know where to start read the data.
                if self.offset.is_none() {
                    (self.offset, self.size) = Self::calculate_offset(r, self.op.range())?;
                }

                let pos = Self::calculate_position(self.offset, self.size, self.cur, pos)?;
                let cur = r.seek(pos)?;
                if cur < self.offset.unwrap() {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }

                self.cur = cur - self.offset.unwrap();
                Ok(self.cur)
            }
            State::Send(_) => {
                unreachable!(
                    "It's invalid to go into State::Send for BlockingRead, please report this bug"
                )
            }
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match &mut self.state {
            State::Idle => {
                // FileReader doesn't support range, we will always use full range to open a file.
                let op = self.op.clone().with_range(BytesRange::from(..));

                let r = match self.acc.blocking_read(&self.path, op) {
                    Ok((_, r)) => r,
                    Err(err) => return Some(Err(err)),
                };
                self.state = State::Read(r);
                self.next()
            }

            State::Read(r) => {
                // We should know where to start read the data.
                if self.offset.is_none() {
                    (self.offset, self.size) = match Self::calculate_offset(r, self.op.range()) {
                        Ok(v) => v,
                        Err(err) => return Some(Err(err)),
                    }
                }

                self.buf.reserve();

                let mut buf = self.buf.initialized_mut();
                let buf = buf.initialized_mut();

                let size = if let Some(size) = self.size {
                    // Sanity check.
                    if self.cur >= size {
                        return None;
                    }
                    cmp::min(buf.len(), (size - self.cur) as usize)
                } else {
                    buf.len()
                };

                match r.read(&mut buf[..size]) {
                    Ok(0) => None,
                    Ok(n) => {
                        self.cur += n as u64;
                        self.buf.record(n);
                        Some(Ok(self.buf.split(n)))
                    }
                    // We don't need to reset state here since it's ok to poll the same reader.
                    Err(err) => Some(Err(err)),
                }
            }
            State::Send(_) => {
                unreachable!(
                    "It's invalid to go into State::Send for BlockingRead, please report this bug"
                )
            }
        }
    }
}
