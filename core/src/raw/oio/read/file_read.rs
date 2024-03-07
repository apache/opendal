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

    reader: Option<R>,
    buf: oio::AdaptiveBuf,
    /// Do we need to reset our cursor?
    seek_dirty: bool,
}

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
            reader: None,
            seek_dirty: false,
        }
    }
}

impl<A, R> FileReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    /// calculate_offset will make sure that the offset has been set.
    async fn offset(r: &mut R, range: BytesRange) -> Result<(Option<u64>, Option<u64>)> {
        let (offset, size) = match (range.offset(), range.size()) {
            (None, None) => (0, None),
            (None, Some(size)) => {
                let start = r.seek(SeekFrom::End(-(size as i64))).await?;
                (start, Some(size))
            }
            (Some(offset), None) => {
                let start = r.seek(SeekFrom::Start(offset)).await?;
                (start, None)
            }
            (Some(offset), Some(size)) => {
                let start = r.seek(SeekFrom::Start(offset)).await?;
                (start, Some(size))
            }
        };

        Ok((Some(offset), size))
    }

    async fn seek_inner(
        r: &mut R,
        offset: Option<u64>,
        size: Option<u64>,
        cur: u64,
        pos: SeekFrom,
    ) -> Result<u64> {
        let offset = offset.expect("offset should be set for calculate_position");

        match pos {
            SeekFrom::Start(n) => {
                // It's valid for user to seek outsides end of the file.
                r.seek(SeekFrom::Start(offset + n)).await
            }
            SeekFrom::End(n) => {
                let size =
                    size.expect("size should be set for calculate_position when seek with end");
                if size as i64 + n < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }
                // size is known, we can convert SeekFrom::End into SeekFrom::Start.
                let pos = SeekFrom::Start(offset + (size as i64 + n) as u64);
                r.seek(pos).await
            }
            SeekFrom::Current(n) => {
                if cur as i64 + n < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }
                let pos = SeekFrom::Start(offset + (cur as i64 + n) as u64);
                r.seek(pos).await
            }
        }
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

    fn blocking_seek_inner(
        r: &mut R,
        offset: Option<u64>,
        size: Option<u64>,
        cur: u64,
        pos: SeekFrom,
    ) -> Result<u64> {
        let offset = offset.expect("offset should be set for calculate_position");

        match pos {
            SeekFrom::Start(n) => {
                // It's valid for user to seek outsides end of the file.
                r.seek(SeekFrom::Start(offset + n))
            }
            SeekFrom::End(n) => {
                let size =
                    size.expect("size should be set for calculate_position when seek with end");
                if size as i64 + n < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }
                // size is known, we can convert SeekFrom::End into SeekFrom::Start.
                let pos = SeekFrom::Start(offset + (size as i64 + n) as u64);
                r.seek(pos)
            }
            SeekFrom::Current(n) => {
                if cur as i64 + n < 0 {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "seek to a negative position is invalid",
                    )
                    .with_context("position", format!("{pos:?}")));
                }
                let pos = SeekFrom::Start(offset + (cur as i64 + n) as u64);
                r.seek(pos)
            }
        }
    }
}

impl<A, R> oio::Read for FileReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.reader.is_none() {
            // FileReader doesn't support range, we will always use full range to open a file.
            let op = self.op.clone().with_range(BytesRange::from(..));
            let (_, r) = self.acc.read(&self.path, op).await?;
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");

        // We should know where to start read the data.
        if self.offset.is_none() {
            (self.offset, self.size) = Self::offset(r, self.op.range()).await?;
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

        match r.read(&mut buf[..size]).await {
            Ok(0) => Ok(0),
            Ok(n) => {
                self.cur += n as u64;
                Ok(n)
            }
            // We don't need to reset state here since it's ok to poll the same reader.
            Err(err) => Err(err),
        }
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if self.reader.is_none() {
            // FileReader doesn't support range, we will always use full range to open a file.
            let op = self.op.clone().with_range(BytesRange::from(..));
            let (_, r) = self.acc.read(&self.path, op).await?;
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");

        // We should know where to start read the data.
        if self.offset.is_none() {
            (self.offset, self.size) = Self::offset(r, self.op.range()).await?;
        }

        // Fetch size when seek end.
        let current_offset = self.offset.unwrap() + self.cur;
        if matches!(pos, SeekFrom::End(_)) && self.size.is_none() {
            let size = r.seek(SeekFrom::End(0)).await?;
            self.size = Some(size - self.offset.unwrap());
            self.seek_dirty = true;
        }
        if self.seek_dirty {
            // Reset cursor.
            r.seek(SeekFrom::Start(current_offset)).await?;
            self.seek_dirty = false;
        }

        let pos = Self::seek_inner(r, self.offset, self.size, self.cur, pos).await?;
        self.cur = pos - self.offset.unwrap();
        Ok(self.cur)
    }

    // async fn next(&mut self) -> Option<Result<Bytes>> {
    //     let cur = self.cur;
    //     let size = self.size;
    //
    //     if self.reader.is_none() {
    //         // FileReader doesn't support range, we will always use full range to open a file.
    //         let op = self.op.clone().with_range(BytesRange::from(..));
    //         let (_, r) = match self.acc.read(&self.path, op).await {
    //             Ok(v) => v,
    //             Err(err) => return Some(Err(err)),
    //         };
    //         self.reader = Some(r);
    //     }
    //
    //     let r = self.reader.as_mut().expect("reader must be valid");
    //
    //     // We should know where to start read the data.
    //     if self.offset.is_none() {
    //         (self.offset, self.size) = match Self::offset(r, self.op.range()).await {
    //             Ok((offset, size)) => (offset, size),
    //             Err(err) => return Some(Err(err)),
    //         }
    //     }
    //
    //     self.buf.reserve();
    //
    //     let mut buf = self.buf.initialized_mut();
    //     let buf = buf.initialized_mut();
    //
    //     let size = if let Some(size) = size {
    //         // Sanity check.
    //         if cur >= size {
    //             return None;
    //         }
    //         cmp::min(buf.len(), (size - cur) as usize)
    //     } else {
    //         buf.len()
    //     };
    //
    //     match r.read(&mut buf[..size]).await {
    //         Ok(0) => None,
    //         Ok(n) => {
    //             self.cur += n as u64;
    //             self.buf.record(n);
    //             Some(Ok(self.buf.split(n)))
    //         }
    //         // We don't need to reset state here since it's ok to poll the same reader.
    //         Err(err) => Some(Err(err)),
    //     }
    // }

    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        todo!()
    }
}

impl<A, R> oio::BlockingRead for FileReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.reader.is_none() {
            // FileReader doesn't support range, we will always use full range to open a file.
            let op = self.op.clone().with_range(BytesRange::from(..));
            let (_, r) = self.acc.blocking_read(&self.path, op)?;
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");

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

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if self.reader.is_none() {
            // FileReader doesn't support range, we will always use full range to open a file.
            let op = self.op.clone().with_range(BytesRange::from(..));
            let (_, r) = self.acc.blocking_read(&self.path, op)?;
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");

        // We should know where to start read the data.
        if self.offset.is_none() {
            (self.offset, self.size) = Self::calculate_offset(r, self.op.range())?;
        }
        // Fetch size when seek end.
        let current_offset = self.offset.unwrap() + self.cur;
        if matches!(pos, SeekFrom::End(_)) && self.size.is_none() {
            let size = r.seek(SeekFrom::End(0))?;
            self.size = Some(size - self.offset.unwrap());
            self.seek_dirty = true;
        }
        if self.seek_dirty {
            // Reset cursor.
            r.seek(SeekFrom::Start(current_offset))?;
            self.seek_dirty = false;
        }

        let pos = Self::blocking_seek_inner(r, self.offset, self.size, self.cur, pos)?;
        self.cur = pos - self.offset.unwrap();
        Ok(self.cur)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        if self.reader.is_none() {
            // FileReader doesn't support range, we will always use full range to open a file.
            let op = self.op.clone().with_range(BytesRange::from(..));
            let (_, r) = match self.acc.blocking_read(&self.path, op) {
                Ok(v) => v,
                Err(err) => return Some(Err(err)),
            };
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");

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
}
