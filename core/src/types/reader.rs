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
use std::io::SeekFrom;
use std::ops::{Bound, RangeBounds};

use std::task::ready;
use std::task::Context;
use std::task::Poll;

use bytes::{Buf, BufMut};
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

/// Reader is designed to read data from given path in an asynchronous
/// manner.
///
/// # Usage
///
/// [`Reader`] provides multiple ways to read data from given reader. Please note that it's
/// undefined behavior to use `Reader` in different ways.
///
/// ## Direct
///
/// [`Reader`] provides public API including [`Reader::read`], [`Reader:read_range`], and [`Reader::read_to_end`]. You can use those APIs directly without extra copy.
///
/// # TODO
///
/// Implement `into_async_read` and `into_stream`.
pub struct Reader {
    acc: FusedAccessor,
    path: String,
    op: OpRead,

    inner: oio::Reader,
}

impl Reader {
    /// Create a new reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) async fn create(acc: FusedAccessor, path: &str, op: OpRead) -> Result<Self> {
        let (_, r) = acc.read(path, op.clone()).await?;

        Ok(Reader {
            acc,
            path: path.to_string(),
            op,
            inner: r,
        })
    }

    /// Read from underlying storage and write data into the specified buffer, starting at
    /// the given offset and up to the limit.
    ///
    /// A return value of `n` signifies that `n` bytes of data have been read into `buf`.
    /// If `n < limit`, it indicates that the reader has reached EOF (End of File).
    #[inline]
    pub async fn read(&self, buf: &mut impl BufMut, offset: u64, limit: usize) -> Result<usize> {
        let bs = self.inner.read_at_dyn(offset, limit).await?;
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }

    /// Read given range bytes of data from reader.
    pub async fn read_range(
        &self,
        buf: &mut impl BufMut,
        range: impl RangeBounds<u64>,
    ) -> Result<usize> {
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

        // If range is empty, return Ok(0) directly.
        if let Some(end) = end {
            if end <= start {
                return Ok(0);
            }
        }

        let mut offset = start;
        let mut size = match end {
            Some(end) => Some(end - start),
            None => None,
        };

        let mut read = 0;
        loop {
            let bs = self
                .inner
                // TODO: use service preferred io size instead.
                .read_at_dyn(offset, size.unwrap_or(4 * 1024 * 1024) as usize)
                .await?;
            let n = bs.remaining();
            read += n;
            buf.put(bs);
            if n == 0 {
                return Ok(read);
            }

            offset += n as u64;

            size = size.map(|v| v - n as u64);
            if size == Some(0) {
                return Ok(read);
            }
        }
    }

    /// Read all data from reader.
    ///
    /// This API is exactly the same with `Reader::read_range(buf, ..)`.
    #[inline]
    pub async fn read_to_end(&self, buf: &mut impl BufMut) -> Result<usize> {
        self.read_range(buf, ..).await
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use crate::services;
    use crate::Operator;

    fn gen_random_bytes() -> Vec<u8> {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        content
    }

    #[tokio::test]
    async fn test_reader_async_read() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader(path).await.unwrap();
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .expect("read to end must succeed");

        assert_eq!(buf, content);
    }

    #[tokio::test]
    async fn test_reader_async_seek() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader(path).await.unwrap();
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .expect("read to end must succeed");
        assert_eq!(buf, content);
    }
}
