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

use std::sync::Arc;

use futures::Future;

use crate::raw::*;
use crate::*;

const DEFAULT_POSITION_READ_MAX_BUF_SIZE: usize = 2 * 1024 * 1024;

/// PositionRead is used to implement [`oio::Read`] based on positioned reads.
///
/// Services that implement [`PositionRead`] must support position-independent
/// reads. `size` is the maximum number of bytes to read, and implementations may
/// return fewer bytes. Returning an empty buffer means EOF.
pub trait PositionRead: Send + Sync + Unpin + 'static {
    /// Read up to `size` bytes from `offset`.
    fn read_at(&self, offset: u64, size: usize)
    -> impl Future<Output = Result<Buffer>> + MaybeSend;
}

/// PositionReader implements [`oio::Read`] based on [`PositionRead`].
pub struct PositionReader<R: PositionRead> {
    inner: Arc<R>,
    max_buf_size: usize,
}

impl<R: PositionRead> PositionReader<R> {
    /// Create a new [`PositionReader`].
    pub fn new(inner: R) -> Self {
        Self {
            inner: Arc::new(inner),
            max_buf_size: DEFAULT_POSITION_READ_MAX_BUF_SIZE,
        }
    }

    /// Set the maximum buffer size used by [`PositionReader`].
    pub fn with_max_buf_size(mut self, buf_size: usize) -> Self {
        assert!(
            buf_size > 0,
            "position read max buffer size must not be zero"
        );

        self.max_buf_size = buf_size;
        self
    }

    /// Consume the reader and return the inner [`PositionRead`].
    ///
    /// # Panics
    ///
    /// Panics if there are active streams that still share the inner reader.
    pub fn into_inner(self) -> R {
        Arc::into_inner(self.inner).expect("position reader must not be shared")
    }
}

impl<R: PositionRead> oio::Read for PositionReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let stream = PositionReadStream::new(self.inner.clone(), range, self.max_buf_size);
        Ok((
            RpRead::default(),
            Box::new(stream) as Box<dyn oio::ReadStreamDyn>,
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let size = range
            .size()
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "read requires a bounded range"))?;

        let mut offset = range.offset();
        let mut remaining = size;
        let mut bufs = Vec::new();

        while remaining > 0 {
            let read_size = remaining.min(self.max_buf_size as u64) as usize;
            let buf = self.inner.read_at(offset, read_size).await?;
            check_position_read_size(read_size, buf.len())?;
            if buf.is_empty() {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range exceeds content length",
                )
                .with_context("offset", offset)
                .with_context("remaining", remaining));
            }

            let n = buf.len() as u64;
            offset += n;
            remaining -= n;
            bufs.push(buf);
        }

        Ok((RpRead::default(), bufs.into_iter().flatten().collect()))
    }
}

struct PositionReadStream<R: PositionRead> {
    inner: Arc<R>,
    offset: u64,
    remaining: Option<u64>,
    max_buf_size: usize,
    done: bool,
}

impl<R: PositionRead> PositionReadStream<R> {
    fn new(inner: Arc<R>, range: BytesRange, max_buf_size: usize) -> Self {
        Self {
            inner,
            offset: range.offset(),
            remaining: range.size(),
            max_buf_size,
            done: false,
        }
    }
}

impl<R: PositionRead> oio::ReadStream for PositionReadStream<R> {
    async fn read(&mut self) -> Result<Buffer> {
        if self.done || self.remaining == Some(0) {
            return Ok(Buffer::new());
        }

        let read_size = self
            .remaining
            .map(|remaining| remaining.min(self.max_buf_size as u64) as usize)
            .unwrap_or(self.max_buf_size);

        let buf = self.inner.read_at(self.offset, read_size).await?;
        check_position_read_size(read_size, buf.len())?;
        if buf.is_empty() {
            self.done = true;
            if let Some(remaining) = self.remaining {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range exceeds content length",
                )
                .with_context("offset", self.offset)
                .with_context("remaining", remaining));
            }
            return Ok(Buffer::new());
        }

        let n = buf.len() as u64;
        self.offset += n;
        if let Some(remaining) = &mut self.remaining {
            *remaining -= n;
        }

        Ok(buf)
    }
}

fn check_position_read_size(expected: usize, actual: usize) -> Result<()> {
    if actual > expected {
        return Err(
            Error::new(ErrorKind::Unexpected, "reader got unexpected data size")
                .with_context("expect", expected)
                .with_context("actual", actual),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;

    use super::*;
    use crate::raw::oio::Read;
    use crate::raw::oio::ReadStream;

    struct TestPositionRead {
        content: Bytes,
        max_read: usize,
        calls: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    impl TestPositionRead {
        fn new(content: &'static [u8], max_read: usize) -> Self {
            Self {
                content: Bytes::from_static(content),
                max_read,
                calls: Arc::default(),
            }
        }
    }

    impl PositionRead for TestPositionRead {
        async fn read_at(&self, offset: u64, size: usize) -> Result<Buffer> {
            self.calls.lock().unwrap().push((offset, size));

            let offset = offset as usize;
            if offset >= self.content.len() {
                return Ok(Buffer::new());
            }

            let end = offset + size.min(self.max_read).min(self.content.len() - offset);
            Ok(Buffer::from(self.content.slice(offset..end)))
        }
    }

    #[tokio::test]
    async fn test_position_reader_read_handles_partial_reads() -> Result<()> {
        let inner = TestPositionRead::new(b"0123456789", 2);
        let calls = inner.calls.clone();
        let reader = PositionReader::new(inner).with_max_buf_size(4);

        let (_, buf) = reader.read(BytesRange::from(2..8)).await?;

        assert_eq!(buf.to_vec(), b"234567");
        assert_eq!(calls.lock().unwrap().as_slice(), &[(2, 4), (4, 4), (6, 2)]);

        Ok(())
    }

    #[tokio::test]
    async fn test_position_reader_read_reports_early_eof() -> Result<()> {
        let reader =
            PositionReader::new(TestPositionRead::new(b"0123456789", 4)).with_max_buf_size(4);

        let err = reader.read(BytesRange::from(8..12)).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
        Ok(())
    }

    #[tokio::test]
    async fn test_position_reader_open_stops_at_eof() -> Result<()> {
        let reader =
            PositionReader::new(TestPositionRead::new(b"0123456789", 2)).with_max_buf_size(4);
        let (_, mut stream) = reader.open(BytesRange::from(8..)).await?;

        let buf = stream.read_all().await?;

        assert_eq!(buf.to_vec(), b"89");
        Ok(())
    }
}
