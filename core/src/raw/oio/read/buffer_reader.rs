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

use bytes::BufMut;
use bytes::Bytes;
use tokio::io::ReadBuf;

use std::cmp::min;
use std::io::SeekFrom;

use std::task::ready;
use std::task::Context;
use std::task::Poll;

use crate::raw::*;
use crate::*;

/// [BufferReader] allows the underlying reader to fetch data at the buffer's size
/// and is used to amortize the IO's overhead.
pub struct BufferReader<R> {
    r: R,
    cur: u64,

    buf: Vec<u8>,
    filled: usize,
    pos: usize,
}

impl<R> BufferReader<R> {
    /// Create a new [`oio::Reader`] with a buffer.
    pub fn new(r: R, cap: usize) -> BufferReader<R> {
        BufferReader {
            r,
            cur: 0,

            buf: Vec::with_capacity(cap),
            filled: 0,
            pos: 0,
        }
    }

    /// Invalidates all data in the internal buffer.
    #[inline]
    fn discard_buffer(&mut self) {
        self.buf.clear();
        self.pos = 0;
        self.filled = 0;
    }

    /// Returns the capacity of the internal buffer.
    fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

impl<R> BufferReader<R>
where
    R: oio::Read,
{
    fn poll_fill_buf(&mut self, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        // If we've reached the end of our internal buffer then we need to fetch
        // some more data from the underlying reader.
        // Branch using `>=` instead of the more correct `==`
        // to tell the compiler that the pos..cap slice is always valid.
        if self.pos >= self.filled {
            debug_assert!(self.pos == self.filled);

            let cap = self.capacity();
            self.buf.clear();
            let dst = self.buf.spare_capacity_mut();
            let mut buf = ReadBuf::uninit(dst);
            unsafe { buf.assume_init(cap) };

            let n = ready!(self.r.poll_read(cx, buf.initialized_mut()))?;
            unsafe { self.buf.set_len(n) }

            self.pos = 0;
            self.filled = n;
        }

        Poll::Ready(Ok(&self.buf[self.pos..self.filled]))
    }

    fn consume(&mut self, amt: usize) {
        let new_pos = min(self.pos + amt, self.filled);
        let amt = new_pos - self.pos;

        self.pos = new_pos;
        self.cur += amt as u64;
    }

    fn seek_relative(&mut self, offset: i64) -> Option<u64> {
        let pos = self.pos as u64;

        if let (Some(new_pos), Some(new_cur)) = (
            pos.checked_add_signed(offset),
            self.cur.checked_add_signed(offset),
        ) {
            if new_pos <= self.filled as u64 {
                self.cur = new_cur;
                self.pos = new_pos as usize;
                return Some(self.cur);
            }
        }

        None
    }

    fn poll_inner_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let cur = ready!(self.r.poll_seek(cx, pos))?;
        self.discard_buffer();
        self.cur = cur;

        Poll::Ready(Ok(cur))
    }
}

impl<R> oio::Read for BufferReader<R>
where
    R: oio::Read,
{
    fn poll_read(&mut self, cx: &mut Context<'_>, mut dst: &mut [u8]) -> Poll<Result<usize>> {
        // Sanity check for normal cases.
        if dst.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.filled && dst.len() >= self.capacity() {
            let res = ready!(self.r.poll_read(cx, dst));
            self.discard_buffer();
            return Poll::Ready(res);
        }

        let rem = ready!(self.poll_fill_buf(cx))?;
        let amt = min(rem.len(), dst.len());
        dst.put(&rem[..amt]);
        self.consume(amt);
        Poll::Ready(Ok(amt))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let offset = match pos {
            SeekFrom::Start(new_pos) => {
                // TODO(weny): Check the overflowing.
                match (new_pos as i64).checked_sub(self.cur as i64) {
                    Some(n) => n,
                    _ => return self.poll_inner_seek(cx, pos),
                }
            }
            SeekFrom::Current(offset) => offset,
            _ => return self.poll_inner_seek(cx, pos),
        };

        match self.seek_relative(offset) {
            Some(cur) => Poll::Ready(Ok(cur)),
            None => self.poll_inner_seek(cx, pos),
        }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match ready!(self.poll_fill_buf(cx)) {
            Ok(bytes) => {
                if bytes.is_empty() {
                    return Poll::Ready(None);
                }

                let bytes = Bytes::copy_from_slice(bytes);
                self.consume(bytes.len());
                Poll::Ready(Some(Ok(bytes)))
            }
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;
    use std::pin::Pin;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::AsyncRead;
    use futures::AsyncReadExt;
    use futures::AsyncSeekExt;
    use rand::prelude::*;
    use sha2::Digest;
    use sha2::Sha256;

    use crate::raw::oio::RangeReader;

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
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Lister = ();
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
        fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
            Pin::new(&mut self.inner).poll_read(cx, buf).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "read data from mock").set_source(err)
            })
        }

        fn poll_seek(&mut self, cx: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
            let (_, _) = (cx, pos);

            Poll::Ready(Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support seeking",
            )))
        }

        fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
            let mut bs = vec![0; 4 * 1024];
            let n = ready!(Pin::new(&mut self.inner)
                .poll_read(cx, &mut bs)
                .map_err(
                    |err| Error::new(ErrorKind::Unexpected, "read data from mock").set_source(err)
                )?);
            if n == 0 {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(Ok(Bytes::from(bs[..n].to_vec()))))
            }
        }
    }

    #[tokio::test]
    async fn test_read_from_buf() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(&b"Hello, World!"[..]);

        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(acc, "x", OpRead::default())) as oio::Reader;

        let buf_cap = 10;
        let mut r = Box::new(BufferReader::new(r, buf_cap)) as oio::Reader;
        let mut dst = [0u8; 5];

        let nread = r.read(&mut dst).await?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b"Hello");

        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst).await?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b", Wor");

        let mut dst = [0u8; 3];
        let nread = r.read(&mut dst).await?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b"ld!");

        Ok(())
    }

    #[tokio::test]
    async fn test_seek() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(&b"Hello, World!"[..]);
        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(acc, "x", OpRead::default())) as oio::Reader;

        let buf_cap = 10;
        let mut r = Box::new(BufferReader::new(r, buf_cap)) as oio::Reader;

        // The underlying reader buffers the b"Hello, Wor".
        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst).await?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b"Hello");

        let pos = r.seek(SeekFrom::Start(7)).await?;
        assert_eq!(pos, 7);
        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst).await?;
        assert_eq!(&dst[..nread], &bs[7..10]);
        assert_eq!(nread, 3);

        // Should perform a relative seek.
        let pos = r.seek(SeekFrom::Start(0)).await?;
        assert_eq!(pos, 0);
        let mut dst = [0u8; 9];
        let nread = r.read(&mut dst).await?;
        assert_eq!(&dst[..nread], &bs[0..9]);
        assert_eq!(nread, 9);

        // Should perform a non-relative seek.
        let pos = r.seek(SeekFrom::Start(11)).await?;
        assert_eq!(pos, 11);
        let mut dst = [0u8; 9];
        let nread = r.read(&mut dst).await?;
        assert_eq!(&dst[..nread], &bs[11..13]);
        assert_eq!(nread, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_all() -> anyhow::Result<()> {
        let (bs, _) = gen_bytes();
        let acc = Arc::new(MockReadService::new(bs.clone()));

        let r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(..)),
        )) as oio::Reader;

        let mut r = Box::new(BufferReader::new(r, 4096 * 1024)) as oio::Reader;

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

        let r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(4096..4096 + 4096)),
        )) as oio::Reader;
        let mut r = Box::new(BufferReader::new(r, 4096 * 1024)) as oio::Reader;

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
