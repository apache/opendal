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

use std::future::Future;
use std::io::SeekFrom;
use std::sync::Arc;

use bytes::Bytes;
use futures::AsyncSeek;

use crate::raw::*;
use crate::*;

/// RangeReader that can do seek on non-seekable reader.
///
/// `oio::Reader` requires the underlying reader to be seekable, but some services like s3, gcs
/// doesn't support seek natively. RangeReader implement seek by read_with_range. We will start
/// a new read request with the correct range when seek is called.
///
/// The `seek` operation on `RangeReader` is zero cost and purely in-memory. But calling `seek`
/// while there is a pending read request will cancel the request and start a new one. This could
/// add extra cost to the read operation.
pub struct RangeReader<A: Accessor, R> {
    acc: Arc<A>,
    path: Arc<String>,
    op: OpRead,

    offset: Option<u64>,
    size: Option<u64>,
    cur: u64,
    reader: Option<R>,
}

impl<A, R> RangeReader<A, R>
where
    A: Accessor,
{
    /// Create a new [`oio::Reader`] by range support.
    ///
    /// # Input
    ///
    /// The input is an Accessor will may return a non-seekable reader.
    ///
    /// # Output
    ///
    /// The output is a reader that can be seek by range.
    ///
    /// # Notes
    ///
    /// This operation is not zero cost. If the accessor already returns a
    /// seekable reader, please don't use this.
    pub fn new(acc: Arc<A>, path: &str, op: OpRead) -> RangeReader<A, R> {
        // Normalize range like `..` into `0..` to make sure offset is valid.
        let (offset, size) = match (op.range().offset(), op.range().size()) {
            (None, None) => (Some(0), None),
            v => v,
        };

        RangeReader {
            acc,
            path: Arc::new(path.to_string()),
            op,

            offset,
            size,
            cur: 0,
            reader: None,
        }
    }

    /// Ensure current reader's offset is valid via total_size.
    fn ensure_offset(&mut self, total_size: u64) -> Result<()> {
        (self.offset, self.size) = match (self.offset, self.size) {
            (None, Some(size)) => {
                if size > total_size {
                    // If returns an error, we should reset
                    // state to Idle so that we can retry it.
                    self.reader = None;
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "read to a negative or overflowing position is invalid",
                    ));
                }

                (Some(total_size - size), Some(size))
            }
            (Some(offset), None) => {
                // It's valid for reader to seek to a position that out of the content length.
                // We should return `Ok(0)` instead of an error at this case to align fs behavior.
                let size = total_size.checked_sub(offset).unwrap_or_default();

                (Some(offset), Some(size))
            }
            (Some(offset), Some(size)) => (Some(offset), Some(size)),
            (None, None) => {
                unreachable!("fill_range should not reach this case after normalization")
            }
        };

        Ok(())
    }

    /// Ensure size will use the information returned by RpRead to calculate the correct size for reader.
    ///
    /// - If `RpRead` returns `range`, we can calculate the correct size by `range.size()`.
    /// - If `RpRead` returns `size`, we can use it's as the returning body's size.
    fn ensure_size(&mut self, total_size: Option<u64>, content_size: Option<u64>) {
        if let Some(total_size) = total_size {
            // It's valid for reader to seek to a position that out of the content length.
            // We should return `Ok(0)` instead of an error at this case to align fs behavior.
            let size = total_size
                .checked_sub(self.offset.expect("reader offset must be valid"))
                .unwrap_or_default();

            // Ensure size when:
            //
            // - reader's size is unknown.
            // - reader's size is larger than file's size.
            if self.size.is_none() || Some(size) < self.size {
                self.size = Some(size);
                return;
            }
        }

        if let Some(content_size) = content_size {
            if content_size == 0 {
                // Skip size set if content size is 0 since it could be invalid.
                //
                // For example, users seek to `u64::MAX` and calling read.
                return;
            }

            let calculated_size = content_size + self.cur;

            // Ensure size when:
            //
            // - reader's size is unknown.
            // - reader's size is larger than file's size.
            if self.size.is_none() || Some(calculated_size) < self.size {
                self.size = Some(calculated_size);
            }
        }
    }

    /// Calculate the current range, maybe sent as next read request.
    ///
    /// # Panics
    ///
    /// Offset must be normalized before calling this function.
    ///
    /// - `..` should be transformed into `0..`
    /// - `..size` should be transformed into `(total-size)..total`.
    fn calculate_range(&self) -> BytesRange {
        let offset = self
            .offset
            .expect("offset must be set before calculating range");

        BytesRange::new(Some(offset + self.cur), self.size.map(|v| v - self.cur))
    }
}

impl<A, R> RangeReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    async fn read_future(&self) -> Result<(RpRead, R)> {
        let mut op = self.op.clone();
        // cur != 0 means we have read some data out, we should convert
        // the op into deterministic to avoid ETag changes.
        if self.cur != 0 {
            op = op.into_deterministic();
        }
        // Alter OpRead with correct calculated range.
        op = op.with_range(self.calculate_range());

        self.acc.read(&self.path, op).await
    }

    async fn stat_future(&self) -> Result<RpStat> {
        // Handle if-match and if-none-match correctly.
        let mut args = OpStat::default();
        // TODO: stat should support range to check if ETag matches.
        if self.op.range().is_full() {
            if let Some(v) = self.op.if_match() {
                args = args.with_if_match(v);
            }
            if let Some(v) = self.op.if_none_match() {
                args = args.with_if_none_match(v);
            }
        }

        self.acc.stat(&self.path, args).await
    }
}

impl<A, R> RangeReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read_action(&self) -> Result<(RpRead, R)> {
        let acc = self.acc.clone();
        let path = self.path.clone();

        let mut op = self.op.clone();
        // cur != 0 means we have read some data out, we should convert
        // the op into deterministic to avoid ETag changes.
        if self.cur != 0 {
            op = op.into_deterministic();
        }
        // Alter OpRead with correct calculated range.
        op = op.with_range(self.calculate_range());

        acc.blocking_read(&path, op)
    }

    fn stat_action(&self) -> Result<RpStat> {
        let acc = self.acc.clone();
        let path = self.path.clone();

        // Handle if-match and if-none-match correctly.
        let mut args = OpStat::default();
        // TODO: stat should support range to check if ETag matches.
        if self.op.range().is_full() {
            if let Some(v) = self.op.if_match() {
                args = args.with_if_match(v);
            }
            if let Some(v) = self.op.if_none_match() {
                args = args.with_if_none_match(v);
            }
        }

        acc.blocking_stat(&path, args)
    }
}

impl<A, R> oio::Read for RangeReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Sanity check for normal cases.
        if buf.is_empty() || self.cur >= self.size.unwrap_or(u64::MAX) {
            return Ok(0);
        }

        if self.offset.is_none() {
            let rp = self.stat_future().await?;
            let length = rp.into_metadata().content_length();
            self.ensure_offset(length)?;
        }
        if self.reader.is_none() {
            let (rp, r) = self.read_future().await?;

            self.ensure_size(rp.range().unwrap_or_default().size(), rp.size());
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");
        match r.read(buf).await {
            Ok(0) => {
                // Reset state to Idle after all data has been consumed.
                self.reader = None;
                Ok(0)
            }
            Ok(n) => {
                self.cur += n as u64;
                Ok(n)
            }
            Err(e) => {
                self.reader = None;
                Err(e)
            }
        }
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        // There is an optimization here that we can calculate if users trying to seek
        // the same position, for example, `reader.seek(SeekFrom::Current(0))`.
        // In this case, we can just return current position without dropping reader.
        if pos == SeekFrom::Current(0) || pos == SeekFrom::Start(self.cur) {
            return Ok(self.cur);
        }

        // We are seeking to other places, let's drop existing reader.
        self.reader = None;

        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::Current(n) => (self.cur as i64, n),
            SeekFrom::End(n) => {
                if let Some(size) = self.size {
                    (size as i64, n)
                } else {
                    let rp = self.stat_future().await?;
                    let length = rp.into_metadata().content_length();
                    self.ensure_offset(length)?;

                    (length as i64, n)
                }
            }
        };

        let seek_pos = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ))
            }
        };

        self.cur = seek_pos;
        Ok(self.cur)
    }

    async fn next(&mut self) -> Option<Result<Bytes>> {
        // Sanity check for normal cases.
        if self.cur >= self.size.unwrap_or(u64::MAX) {
            return None;
        }

        if self.offset.is_none() {
            let rp = match self.stat_future().await {
                Ok(v) => v,
                Err(err) => return Some(Err(err)),
            };
            let length = rp.into_metadata().content_length();
            if let Err(err) = self.ensure_offset(length) {
                return Some(Err(err));
            }
        }
        if self.reader.is_none() {
            let (rp, r) = match self.read_future().await {
                Ok((rp, r)) => (rp, r),
                Err(err) => return Some(Err(err)),
            };

            self.ensure_size(rp.range().unwrap_or_default().size(), rp.size());
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");
        match r.next().await {
            Some(Ok(bs)) => {
                self.cur += bs.len() as u64;
                Some(Ok(bs))
            }
            Some(Err(err)) => {
                self.reader = None;
                Some(Err(err))
            }
            None => {
                self.reader = None;
                None
            }
        }
    }
}

impl<A, R> oio::BlockingRead for RangeReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Sanity check for normal cases.
        if buf.is_empty() || self.cur >= self.size.unwrap_or(u64::MAX) {
            return Ok(0);
        }

        if self.offset.is_none() {
            let rp = self.stat_action()?;
            let length = rp.into_metadata().content_length();
            self.ensure_offset(length)?;
        }
        if self.reader.is_none() {
            let (rp, r) = self.read_action()?;

            self.ensure_size(rp.range().unwrap_or_default().size(), rp.size());
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");
        match r.read(buf) {
            Ok(0) => {
                // Reset state to Idle after all data has been consumed.
                self.reader = None;
                Ok(0)
            }
            Ok(n) => {
                self.cur += n as u64;
                Ok(n)
            }
            Err(e) => {
                self.reader = None;
                Err(e)
            }
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        // There is an optimization here that we can calculate if users trying to seek
        // the same position, for example, `reader.seek(SeekFrom::Current(0))`.
        // In this case, we can just return current position without dropping reader.
        if pos == SeekFrom::Current(0) || pos == SeekFrom::Start(self.cur) {
            return Ok(self.cur);
        }

        // We are seeking to other places, let's drop existing reader.
        self.reader = None;

        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::Current(n) => (self.cur as i64, n),
            SeekFrom::End(n) => {
                if let Some(size) = self.size {
                    (size as i64, n)
                } else {
                    let rp = self.stat_action()?;
                    let length = rp.into_metadata().content_length();
                    self.ensure_offset(length)?;

                    (length as i64, n)
                }
            }
        };

        let seek_pos = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ))
            }
        };

        self.cur = seek_pos;
        Ok(self.cur)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        // Sanity check for normal cases.
        if self.cur >= self.size.unwrap_or(u64::MAX) {
            return None;
        }

        if self.offset.is_none() {
            let rp = match self.stat_action() {
                Ok(rp) => rp,
                Err(err) => return Some(Err(err)),
            };
            let length = rp.into_metadata().content_length();
            if let Err(err) = self.ensure_offset(length) {
                return Some(Err(err));
            }
        }
        if self.reader.is_none() {
            let (rp, r) = match self.read_action() {
                Ok((rp, r)) => (rp, r),
                Err(err) => return Some(Err(err)),
            };

            self.ensure_size(rp.range().unwrap_or_default().size(), rp.size());
            self.reader = Some(r);
        }

        let r = self.reader.as_mut().expect("reader must be valid");
        match r.next() {
            Some(Ok(bs)) => {
                self.cur += bs.len() as u64;
                Some(Ok(bs))
            }
            Some(Err(err)) => {
                self.reader = None;
                Some(Err(err))
            }
            None => {
                self.reader = None;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::AsyncRead;
    use futures::AsyncReadExt;
    use futures::AsyncSeekExt;
    use rand::prelude::*;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;

    // Generate bytes between [4MiB, 16MiB)
    fn gen_bytes() -> (Bytes, usize) {
        let mut rng = thread_rng();

        let size = rng.gen_range(4 * 1024 * 1024..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        (Bytes::from(content), size)
    }

    #[derive(Debug, Clone, Default)]
    struct MockReadService {
        data: Bytes,
    }

    impl MockReadService {
        fn new(data: Bytes) -> Self {
            Self { data }
        }
    }

    #[async_trait]
    impl Accessor for MockReadService {
        type Reader = MockReader;
        type Writer = ();
        type Lister = ();
        type BlockingReader = ();
        type BlockingWriter = ();
        type BlockingLister = ();

        fn info(&self) -> AccessorInfo {
            let mut am = AccessorInfo::default();
            am.set_native_capability(Capability {
                read: true,
                ..Default::default()
            });

            am
        }

        async fn read(&self, _: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
            let bs = args.range().apply_on_bytes(self.data.clone());

            Ok((
                RpRead::new(),
                MockReader {
                    inner: futures::io::Cursor::new(bs.into()),
                },
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockReader {
        inner: futures::io::Cursor<Vec<u8>>,
    }

    impl oio::Read for MockReader {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            self.inner.read(buf).await.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read data from mock").set_source(err)
            })
        }

        async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
            let _ = pos;

            Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support seeking",
            ))
        }

        async fn next(&mut self) -> Option<Result<Bytes>> {
            let mut bs = vec![0; 4 * 1024];
            let n = self.inner.read(&mut bs).await.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read data from mock").set_source(err)
            })?;
            if n == 0 {
                None
            } else {
                Some(Ok(Bytes::from(bs[..n].to_vec())))
            }
        }
    }

    #[tokio::test]
    async fn test_read_all() -> anyhow::Result<()> {
        let (bs, _) = gen_bytes();
        let acc = Arc::new(MockReadService::new(bs.clone()));

        let mut r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(..)),
        )) as oio::Reader;

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(bs.len(), buf.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&buf)),
            "read content"
        );

        let n = r.seek(SeekFrom::Start(0)).await?;
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(bs.len(), buf.len(), "read twice size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&buf)),
            "read twice content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_read_part() -> anyhow::Result<()> {
        let (bs, _) = gen_bytes();
        let acc = Arc::new(MockReadService::new(bs.clone()));

        let mut r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(4096..4096 + 4096)),
        )) as oio::Reader;

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(4096, buf.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096..4096 + 4096])),
            format!("{:x}", Sha256::digest(&buf)),
            "read content"
        );

        let n = r.seek(SeekFrom::Start(0)).await?;
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        r.read_to_end(&mut buf).await?;
        assert_eq!(4096, buf.len(), "read twice size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096..4096 + 4096])),
            format!("{:x}", Sha256::digest(&buf)),
            "read twice content"
        );

        let n = r.seek(SeekFrom::Start(1024)).await?;
        assert_eq!(1024, n, "seek to 1024");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf).await?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 1024..4096 + 2048])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek 1024"
        );

        let n = r.seek(SeekFrom::Current(1024)).await?;
        assert_eq!(3072, n, "seek to 3072");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf).await?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 3072..4096 + 3072 + 1024])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek to 3072"
        );

        Ok(())
    }
}
