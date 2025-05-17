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

use std::ops::Range;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::BufMut;
use futures::stream;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::*;

/// Reader is designed to read data from given path in an asynchronous
/// manner.
///
/// # Usage
///
/// [`Reader`] provides multiple ways to read data from given reader.
///
/// `Reader` implements `Clone` so you can clone it and store in place where ever you want.
///
/// ## Direct
///
/// [`Reader`] provides public API including [`Reader::read`]. You can use those APIs directly without extra copy.
///
/// ```
/// use opendal::Operator;
/// use opendal::Result;
///
/// async fn test(op: Operator) -> Result<()> {
///     let r = op.reader("path/to/file").await?;
///     let bs = r.read(0..1024).await?;
///     Ok(())
/// }
/// ```
///
/// ## Read like `Stream`
///
/// ```
/// use anyhow::Result;
/// use bytes::Bytes;
/// use futures::TryStreamExt;
/// use opendal::Operator;
///
/// async fn test(op: Operator) -> Result<()> {
///     let s = op
///         .reader("path/to/file")
///         .await?
///         .into_bytes_stream(1024..2048)
///         .await?;
///     let bs: Vec<Bytes> = s.try_collect().await?;
///     Ok(())
/// }
/// ```
///
/// ## Read like `AsyncRead` and `AsyncBufRead`
///
/// ```
/// use anyhow::Result;
/// use bytes::Bytes;
/// use futures::AsyncReadExt;
/// use opendal::Operator;
///
/// async fn test(op: Operator) -> Result<()> {
///     let mut r = op
///         .reader("path/to/file")
///         .await?
///         .into_futures_async_read(1024..2048)
///         .await?;
///     let mut bs = vec![];
///     let n = r.read_to_end(&mut bs).await?;
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Reader {
    ctx: Arc<ReadContext>,
}

impl Reader {
    /// Create a new reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementation for users.
    ///
    /// We don't want to expose those details to users so keep this function
    /// in crate only.
    pub(crate) fn new(ctx: ReadContext) -> Self {
        Reader { ctx: Arc::new(ctx) }
    }

    /// Read give range from reader into [`Buffer`].
    ///
    /// This operation is zero-copy, which means it keeps the [`bytes::Bytes`] returned by underlying
    /// storage services without any extra copy or intensive memory allocations.
    pub async fn read(&self, range: impl RangeBounds<u64>) -> Result<Buffer> {
        let bufs: Vec<_> = self.clone().into_stream(range).await?.try_collect().await?;
        Ok(bufs.into_iter().flatten().collect())
    }

    /// Read all data from reader into given [`BufMut`].
    ///
    /// This operation will copy and write bytes into given [`BufMut`]. Allocation happens while
    /// [`BufMut`] doesn't have enough space.
    pub async fn read_into(
        &self,
        buf: &mut impl BufMut,
        range: impl RangeBounds<u64>,
    ) -> Result<usize> {
        let mut stream = self.clone().into_stream(range).await?;

        let mut read = 0;
        loop {
            let Some(bs) = stream.try_next().await? else {
                return Ok(read);
            };
            read += bs.len();
            buf.put(bs);
        }
    }

    /// Fetch specific ranges from reader.
    ///
    /// This operation try to merge given ranges into a list of
    /// non-overlapping ranges. Users may also specify a `gap` to merge
    /// close ranges.
    ///
    /// The returning `Buffer` may share the same underlying memory without
    /// any extra copy.
    pub async fn fetch(&self, ranges: Vec<Range<u64>>) -> Result<Vec<Buffer>> {
        let merged_ranges = self.merge_ranges(ranges.clone());

        let merged_bufs: Vec<_> =
            stream::iter(merged_ranges.clone().into_iter().map(|v| self.read(v)))
                .buffered(self.ctx.options().concurrent())
                .try_collect()
                .await?;

        let mut bufs = Vec::with_capacity(ranges.len());
        for range in ranges {
            let idx = merged_ranges.partition_point(|v| v.start <= range.start) - 1;
            let start = range.start - merged_ranges[idx].start;
            let end = range.end - merged_ranges[idx].start;
            bufs.push(merged_bufs[idx].slice(start as usize..end as usize));
        }

        Ok(bufs)
    }

    /// Merge given ranges into a list of non-overlapping ranges.
    fn merge_ranges(&self, mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        let gap = self.ctx.options().gap().unwrap_or(1024 * 1024) as u64;
        // We don't care about the order of range with same start, they
        // will be merged in the next step.
        ranges.sort_unstable_by(|a, b| a.start.cmp(&b.start));

        // We know that this vector will have at most element
        let mut merged = Vec::with_capacity(ranges.len());
        let mut cur = ranges[0].clone();

        for range in ranges.into_iter().skip(1) {
            if range.start <= cur.end + gap {
                // There is an overlap or the gap is small enough to merge
                cur.end = cur.end.max(range.end);
            } else {
                // No overlap and the gap is too large, push the current range to the list and start a new one
                merged.push(cur);
                cur = range;
            }
        }

        // Push the last range
        merged.push(cur);

        merged
    }

    /// Create a buffer stream to read specific range from given reader.
    ///
    /// # Notes
    ///
    /// BufferStream is a zero-cost abstraction. It doesn't involve extra copy of data.
    /// It will return underlying [`Buffer`] directly.
    ///
    /// The [`Buffer`] this stream yields can be seen as an iterator of [`Bytes`].
    ///
    /// # Inputs
    ///
    /// - `range`: The range of data to read. range like `..` it will read all data from reader.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    ///
    /// ```
    /// use std::io;
    ///
    /// use futures::TryStreamExt;
    /// use opendal::{Buffer, Operator};
    /// use opendal::Result;
    /// use bytes::Bytes;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut s = op
    ///         .reader("hello.txt")
    ///         .await?
    ///         .into_stream(1024..2048)
    ///         .await?;
    ///
    ///     let bs: Vec<Buffer> = s.try_collect().await?;
    ///     // We can use those buffer as bytes if we want.
    ///     let bytes_vec: Vec<Bytes> = bs.clone().into_iter().flatten().collect();
    ///     // Or we can merge them into a single [`Buffer`] and later use it as [`bytes::Buf`].
    ///     let new_buffer: Buffer = bs.into_iter().flatten().collect::<Buffer>();
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Concurrent Read
    ///
    /// The following example reads data in 256B chunks with 8 concurrent.
    ///
    /// ```
    /// use std::io;
    /// use bytes::Bytes;
    ///
    /// use futures::TryStreamExt;
    /// use opendal::{Buffer, Operator};
    /// use opendal::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let s = op
    ///         .reader_with("hello.txt")
    ///         .concurrent(8)
    ///         .chunk(256)
    ///         .await?
    ///         .into_stream(1024..2048)
    ///         .await?;
    ///
    ///     // Every buffer except the last one in the stream will be 256B.
    ///     let bs: Vec<Buffer> = s.try_collect().await?;
    ///     // We can use those buffer as bytes if we want.
    ///     let bytes_vec: Vec<Bytes> = bs.clone().into_iter().flatten().collect();
    ///     // Or we can merge them into a single [`Buffer`] and later use it as [`bytes::Buf`].
    ///     let new_buffer: Buffer = bs.into_iter().flatten().collect::<Buffer>();
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn into_stream(self, range: impl RangeBounds<u64>) -> Result<BufferStream> {
        BufferStream::create(self.ctx, range).await
    }

    /// Convert reader into [`FuturesAsyncReader`] which implements [`futures::AsyncRead`],
    /// [`futures::AsyncSeek`] and [`futures::AsyncBufRead`].
    ///
    /// # Notes
    ///
    /// FuturesAsyncReader is not a zero-cost abstraction. The underlying reader
    /// returns an owned [`Buffer`], which involves an extra copy operation.
    ///
    /// # Inputs
    ///
    /// - `range`: The range of data to read. range like `..` it will read all data from reader.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    ///
    /// ```
    /// use std::io;
    ///
    /// use futures::io::AsyncReadExt;
    /// use opendal::Operator;
    /// use opendal::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut r = op
    ///         .reader("hello.txt")
    ///         .await?
    ///         .into_futures_async_read(1024..2048)
    ///         .await?;
    ///     let mut bs = Vec::new();
    ///     r.read_to_end(&mut bs).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Concurrent Read
    ///
    /// The following example reads data in 256B chunks with 8 concurrent.
    ///
    /// ```
    /// use std::io;
    ///
    /// use futures::io::AsyncReadExt;
    /// use opendal::Operator;
    /// use opendal::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut r = op
    ///         .reader_with("hello.txt")
    ///         .concurrent(8)
    ///         .chunk(256)
    ///         .await?
    ///         .into_futures_async_read(1024..2048)
    ///         .await?;
    ///     let mut bs = Vec::new();
    ///     r.read_to_end(&mut bs).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn into_futures_async_read(
        self,
        range: impl RangeBounds<u64>,
    ) -> Result<FuturesAsyncReader> {
        let range = self.ctx.parse_into_range(range).await?;
        Ok(FuturesAsyncReader::new(self.ctx, range))
    }

    /// Convert reader into [`FuturesBytesStream`] which implements [`futures::Stream`].
    ///
    /// # Inputs
    ///
    /// - `range`: The range of data to read. range like `..` it will read all data from reader.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    ///
    /// ```
    /// use std::io;
    ///
    /// use bytes::Bytes;
    /// use futures::TryStreamExt;
    /// use opendal::Operator;
    /// use opendal::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut s = op
    ///         .reader("hello.txt")
    ///         .await?
    ///         .into_bytes_stream(1024..2048)
    ///         .await?;
    ///     let bs: Vec<Bytes> = s.try_collect().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Concurrent Read
    ///
    /// The following example reads data in 256B chunks with 8 concurrent.
    ///
    /// ```
    /// use std::io;
    ///
    /// use bytes::Bytes;
    /// use futures::TryStreamExt;
    /// use opendal::Operator;
    /// use opendal::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut s = op
    ///         .reader_with("hello.txt")
    ///         .concurrent(8)
    ///         .chunk(256)
    ///         .await?
    ///         .into_bytes_stream(1024..2048)
    ///         .await?;
    ///     let bs: Vec<Bytes> = s.try_collect().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[inline]
    pub async fn into_bytes_stream(
        self,
        range: impl RangeBounds<u64>,
    ) -> Result<FuturesBytesStream> {
        FuturesBytesStream::new(self.ctx, range).await
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rand::rngs::ThreadRng;
    use rand::Rng;
    use rand::RngCore;

    use super::*;
    use crate::raw::*;
    use crate::services;
    use crate::Operator;

    #[tokio::test]
    async fn test_trait() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        op.write(
            "test",
            Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]),
        )
        .await?;

        let acc = op.into_inner();
        let ctx = ReadContext::new(acc, "test".to_string(), OpRead::new(), OpReader::new());

        let _: Box<dyn Unpin + MaybeSend + Sync + 'static> = Box::new(Reader::new(ctx));

        Ok(())
    }

    fn gen_random_bytes() -> Vec<u8> {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        content
    }

    fn gen_fixed_bytes(size: usize) -> Vec<u8> {
        let mut rng = ThreadRng::default();
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        content
    }

    #[tokio::test]
    async fn test_reader_read() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader(path).await.unwrap();
        let buf = reader.read(..).await.expect("read to end must succeed");

        assert_eq!(buf.to_bytes(), content);
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_read_with_chunk() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader_with(path).chunk(16).await.unwrap();
        let buf = reader.read(..).await.expect("read to end must succeed");

        assert_eq!(buf.to_bytes(), content);
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_read_with_concurrent() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op
            .reader_with(path)
            .chunk(128)
            .concurrent(16)
            .await
            .unwrap();
        let buf = reader.read(..).await.expect("read to end must succeed");

        assert_eq!(buf.to_bytes(), content);
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_read_into() -> Result<()> {
        let op = Operator::via_iter(Scheme::Memory, [])?;
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader(path).await.unwrap();
        let mut buf = Vec::new();
        reader
            .read_into(&mut buf, ..)
            .await
            .expect("read to end must succeed");

        assert_eq!(buf, content);
        Ok(())
    }

    #[tokio::test]
    async fn test_merge_ranges() -> Result<()> {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_random_bytes();
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader_with(path).gap(1).await.unwrap();

        let ranges = vec![0..10, 10..20, 21..30, 40..50, 40..60, 45..59];
        let merged = reader.merge_ranges(ranges.clone());
        assert_eq!(merged, vec![0..30, 40..60]);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch() -> Result<()> {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let path = "test_file";

        let content = gen_fixed_bytes(1024);
        op.write(path, content.clone())
            .await
            .expect("write must succeed");

        let reader = op.reader_with(path).gap(1).await.unwrap();

        let ranges = vec![
            0..10,
            40..50,
            45..59,
            10..20,
            21..30,
            40..50,
            40..60,
            45..59,
        ];
        let merged = reader
            .fetch(ranges.clone())
            .await
            .expect("fetch must succeed");

        for (i, range) in ranges.iter().enumerate() {
            assert_eq!(
                merged[i].to_bytes(),
                content[range.start as usize..range.end as usize]
            );
        }
        Ok(())
    }
}
