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

use std::cmp::min;
use std::io::SeekFrom;

use bytes::BufMut;
use bytes::Bytes;
use tokio::io::ReadBuf;

use super::BlockingRead;
use crate::raw::*;
use crate::*;

/// [BufferReader] allows the underlying reader to fetch data at the buffer's size
/// and is used to amortize the IO's overhead.
pub struct BufferReader<R> {
    r: R,
    cur: u64,

    /// TODO: maybe we can use chunked bytes here?
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

    fn unconsumed_buffer_len(&self) -> i64 {
        (self.filled as i64) - (self.pos as i64)
    }
}

impl<R> BufferReader<R>
where
    R: oio::Read,
{
    async fn fill_buf(&mut self) -> Result<&[u8]> {
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

            let bs = self.r.read(cap).await?;
            buf.put_slice(&bs);
            unsafe { self.buf.set_len(bs.len()) }

            self.pos = 0;
            self.filled = bs.len();
        }

        Ok(&self.buf[self.pos..self.filled])
    }

    async fn inner_seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let cur = self.r.seek(pos).await?;
        self.discard_buffer();
        self.cur = cur;

        Ok(cur)
    }
}

impl<R> oio::Read for BufferReader<R>
where
    R: oio::Read,
{
    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(new_pos) => {
                // TODO(weny): Check the overflowing.
                let Some(offset) = (new_pos as i64).checked_sub(self.cur as i64) else {
                    return self.inner_seek(pos).await;
                };

                match self.seek_relative(offset) {
                    Some(cur) => Ok(cur),
                    None => self.inner_seek(pos).await,
                }
            }
            SeekFrom::Current(offset) => match self.seek_relative(offset) {
                Some(cur) => Ok(cur),
                None => {
                    self.inner_seek(SeekFrom::Current(offset - self.unconsumed_buffer_len()))
                        .await
                }
            },
            SeekFrom::End(_) => self.inner_seek(pos).await,
        }
    }

    async fn read(&mut self, size: usize) -> Result<Bytes> {
        if size == 0 {
            return Ok(Bytes::new());
        }

        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.filled && size >= self.capacity() {
            let res = self.r.read(size).await;
            self.discard_buffer();
            return match res {
                Ok(bs) => {
                    self.cur += bs.len() as u64;
                    Ok(bs)
                }
                Err(err) => Err(err),
            };
        }

        let bytes = self.fill_buf().await?;

        if bytes.is_empty() {
            return Ok(Bytes::new());
        }
        let size = min(bytes.len(), size);
        let bytes = Bytes::copy_from_slice(&bytes[..size]);
        self.consume(bytes.len());
        Ok(bytes)
    }
}

impl<R> BufferReader<R>
where
    R: BlockingRead,
{
    fn blocking_fill_buf(&mut self) -> Result<&[u8]> {
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

            let n = self.r.read(buf.initialized_mut())?;
            unsafe { self.buf.set_len(n) }

            self.pos = 0;
            self.filled = n;
        }

        Ok(&self.buf[self.pos..self.filled])
    }

    fn blocking_inner_seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let cur = self.r.seek(pos)?;
        self.discard_buffer();
        self.cur = cur;

        Ok(cur)
    }
}

impl<R> BlockingRead for BufferReader<R>
where
    R: BlockingRead,
{
    fn read(&mut self, mut dst: &mut [u8]) -> Result<usize> {
        // Sanity check for normal cases.
        if dst.is_empty() {
            return Ok(0);
        }

        // If we don't have any buffered data and we're doing a massive read
        // (larger than our internal buffer), bypass our internal buffer
        // entirely.
        if self.pos == self.filled && dst.len() >= self.capacity() {
            let res = self.r.read(dst);
            self.discard_buffer();
            return match res {
                Ok(nread) => {
                    self.cur += nread as u64;
                    Ok(nread)
                }
                Err(err) => Err(err),
            };
        }

        let rem = self.blocking_fill_buf()?;
        let amt = min(rem.len(), dst.len());
        dst.put(&rem[..amt]);
        self.consume(amt);
        Ok(amt)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match pos {
            SeekFrom::Start(new_pos) => {
                // TODO(weny): Check the overflowing.
                let Some(offset) = (new_pos as i64).checked_sub(self.cur as i64) else {
                    return self.blocking_inner_seek(pos);
                };

                match self.seek_relative(offset) {
                    Some(cur) => Ok(cur),
                    None => self.blocking_inner_seek(pos),
                }
            }
            SeekFrom::Current(offset) => match self.seek_relative(offset) {
                Some(cur) => Ok(cur),
                None => self
                    .blocking_inner_seek(SeekFrom::Current(offset - self.unconsumed_buffer_len())),
            },
            SeekFrom::End(_) => self.blocking_inner_seek(pos),
        }
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        match self.blocking_fill_buf() {
            Ok(bytes) => {
                if bytes.is_empty() {
                    return None;
                }

                let bytes = Bytes::copy_from_slice(bytes);
                self.consume(bytes.len());
                Some(Ok(bytes))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use rand::prelude::*;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::raw::oio::RangeReader;

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
        type BlockingReader = MockReader;
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
                    inner: oio::Cursor::from(bs),
                },
            ))
        }

        fn blocking_read(&self, _: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
            let bs = args.range().apply_on_bytes(self.data.clone());

            Ok((
                RpRead::new(),
                MockReader {
                    inner: oio::Cursor::from(bs),
                },
            ))
        }
    }

    struct MockReader {
        inner: oio::Cursor,
    }

    impl oio::Read for MockReader {
        async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
            let _ = pos;

            Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support seeking",
            ))
        }

        async fn read(&mut self, size: usize) -> Result<Bytes> {
            oio::Read::read(&mut self.inner, size).await
        }
    }

    impl BlockingRead for MockReader {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            self.inner.read(buf)
        }

        fn seek(&mut self, _pos: SeekFrom) -> Result<u64> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support seeking",
            ))
        }

        fn next(&mut self) -> Option<Result<Bytes>> {
            self.inner.next()
        }
    }

    #[tokio::test]
    async fn test_read_from_buf() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(&b"Hello, World!"[..]);

        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(acc, "x", OpRead::default())) as oio::Reader;

        let buf_cap = 10;
        let r = Box::new(BufferReader::new(r, buf_cap)) as oio::Reader;
        let mut r = Reader::new(r);

        let bs = r.read(5).await?;
        assert_eq!(bs.len(), 5);
        assert_eq!(bs.as_ref(), b"Hello");

        let bs = r.read(5).await?;
        assert_eq!(bs.len(), 5);
        assert_eq!(bs.as_ref(), b", Wor");

        let bs = r.read(3).await?;
        assert_eq!(bs.len(), 3);
        assert_eq!(bs.as_ref(), b"ld!");

        Ok(())
    }

    #[tokio::test]
    async fn test_seek() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(&b"Hello, World!"[..]);
        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(acc, "x", OpRead::default())) as oio::Reader;

        let buf_cap = 10;
        let r = Box::new(BufferReader::new(r, buf_cap)) as oio::Reader;
        let mut r = Reader::new(r);

        // The underlying reader buffers the b"Hello, Wor".
        let buf = r.read(5).await?;
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_ref(), b"Hello");

        let pos = r.seek(SeekFrom::Start(7)).await?;
        assert_eq!(pos, 7);
        let buf = r.read(5).await?;
        assert_eq!(&buf, &bs[7..10]);
        assert_eq!(buf.len(), 3);

        // Should perform a relative seek.
        let pos = r.seek(SeekFrom::Start(0)).await?;
        assert_eq!(pos, 0);
        let buf = r.read(9).await?;
        assert_eq!(&buf, &bs[0..9]);
        assert_eq!(buf.len(), 9);

        // Should perform a non-relative seek.
        let pos = r.seek(SeekFrom::Start(11)).await?;
        assert_eq!(pos, 11);
        let buf = r.read(9).await?;
        assert_eq!(&buf, &bs[11..13]);
        assert_eq!(buf.len(), 2);

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

        let r = Box::new(BufferReader::new(r, 4096 * 1024)) as oio::Reader;
        let mut r = Reader::new(r);

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
    async fn test_bypass_seek_relative() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(
            &b"Hello, World! I'm going to tests a seek relative related bug!"[..],
        );
        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(..)),
        )) as oio::Reader;
        let r = Box::new(BufferReader::new(r, 10)) as oio::Reader;
        let mut r = Reader::new(r);

        let mut cur = 0;
        for _ in 0..3 {
            let bs = r.read(5).await?;
            assert_eq!(bs.len(), 5);
            cur += 5;
        }

        let ret_cur = r.seek(SeekFrom::Current(-15)).await?;
        assert_eq!(cur - 15, ret_cur);

        Ok(())
    }

    #[tokio::test]
    async fn test_bypass_read_and_seek_relative() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(
            &b"Hello, World! I'm going to tests a seek relative related bug!"[..],
        );
        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(..)),
        )) as oio::Reader;
        let r = Box::new(BufferReader::new(r, 5)) as oio::Reader;
        let mut r = Reader::new(r);

        let mut cur = 0;
        for _ in 0..3 {
            let bs = r.read(6).await?;
            assert_eq!(bs.len(), 6);
            cur += 6;
        }

        let ret_cur = r.seek(SeekFrom::Current(6)).await?;
        assert_eq!(cur + 6, ret_cur);

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
        let r = Box::new(BufferReader::new(r, 4096 * 1024)) as oio::Reader;
        let mut r = Reader::new(r);

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

        let buf = r.read_exact(1024).await?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 1024..4096 + 2048])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek 1024"
        );

        let n = r.seek(SeekFrom::Current(1024)).await?;
        assert_eq!(3072, n, "seek to 3072");

        let buf = r.read_exact(1024).await?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 3072..4096 + 3072 + 1024])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek to 3072"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking_read_from_buf() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(&b"Hello, World!"[..]);
        let r = Box::new(oio::Cursor::from(bs.clone())) as oio::BlockingReader;
        let buf_cap = 10;
        let r = Box::new(BufferReader::new(r, buf_cap)) as oio::BlockingReader;
        let mut r = BlockingReader::new(r);

        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst)?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b"Hello");

        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst)?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b", Wor");

        let mut dst = [0u8; 3];
        let nread = r.read(&mut dst)?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b"ld!");

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking_seek() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(&b"Hello, World!"[..]);
        let r = Box::new(oio::Cursor::from(bs.clone())) as oio::BlockingReader;
        let buf_cap = 10;
        let r = Box::new(BufferReader::new(r, buf_cap)) as oio::BlockingReader;
        let mut r = BlockingReader::new(r);

        // The underlying reader buffers the b"Hello, Wor".
        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst)?;
        assert_eq!(nread, dst.len());
        assert_eq!(&dst, b"Hello");

        let pos = r.seek(SeekFrom::Start(7))?;
        assert_eq!(pos, 7);
        let mut dst = [0u8; 5];
        let nread = r.read(&mut dst)?;
        assert_eq!(&dst[..nread], &bs[7..10]);
        assert_eq!(nread, 3);

        // Should perform a relative seek.
        let pos = r.seek(SeekFrom::Start(0))?;
        assert_eq!(pos, 0);
        let mut dst = [0u8; 9];
        let nread = r.read(&mut dst)?;
        assert_eq!(&dst[..nread], &bs[0..9]);
        assert_eq!(nread, 9);

        // Should perform a non-relative seek.
        let pos = r.seek(SeekFrom::Start(11))?;
        assert_eq!(pos, 11);
        let mut dst = [0u8; 9];
        let nread = r.read(&mut dst)?;
        assert_eq!(&dst[..nread], &bs[11..13]);
        assert_eq!(nread, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking_read_all() -> anyhow::Result<()> {
        let (bs, _) = gen_bytes();
        let r = Box::new(oio::Cursor::from(bs.clone())) as oio::BlockingReader;
        let r = Box::new(BufferReader::new(r, 4096 * 1024)) as oio::BlockingReader;
        let mut r = BlockingReader::new(r);

        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        assert_eq!(bs.len(), buf.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&buf)),
            "read content"
        );

        let n = r.seek(SeekFrom::Start(0))?;
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        r.read_to_end(&mut buf)?;
        assert_eq!(bs.len(), buf.len(), "read twice size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&buf)),
            "read twice content"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking_bypass_seek_relative() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(
            &b"Hello, World! I'm going to tests a seek relative related bug!"[..],
        );
        let r = Box::new(oio::Cursor::from(bs.clone())) as oio::BlockingReader;
        let r = Box::new(BufferReader::new(r, 10)) as oio::BlockingReader;
        let mut r = BlockingReader::new(r);

        let mut cur = 0;
        for _ in 0..3 {
            let mut dst = [0u8; 5];
            let nread = r.read(&mut dst)?;
            assert_eq!(nread, 5);
            cur += 5;
        }

        let ret_cur = r.seek(SeekFrom::Current(-15))?;
        assert_eq!(cur - 15, ret_cur);

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking_bypass_read_and_seek_relative() -> anyhow::Result<()> {
        let bs = Bytes::copy_from_slice(
            &b"Hello, World! I'm going to tests a seek relative related bug!"[..],
        );
        let r = Box::new(oio::Cursor::from(bs.clone())) as oio::BlockingReader;
        let r = Box::new(BufferReader::new(r, 5)) as oio::BlockingReader;
        let mut r = BlockingReader::new(r);

        let mut cur = 0;
        for _ in 0..3 {
            let mut dst = [0u8; 6];
            let nread = r.read(&mut dst)?;
            assert_eq!(nread, 6);
            cur += 6;
        }

        let ret_cur = r.seek(SeekFrom::Current(6))?;
        assert_eq!(cur + 6, ret_cur);

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking_read_part() -> anyhow::Result<()> {
        use std::io::Read;

        let (bs, _) = gen_bytes();
        let acc = Arc::new(MockReadService::new(bs.clone()));
        let r = Box::new(RangeReader::new(
            acc,
            "x",
            OpRead::default().with_range(BytesRange::from(4096..4096 + 4096)),
        )) as oio::BlockingReader;
        let r = Box::new(BufferReader::new(r, 4096 * 1024)) as oio::BlockingReader;
        let mut r = BlockingReader::new(r);

        let mut buf = Vec::new();
        BlockingRead::read_to_end(&mut r, &mut buf)?;
        assert_eq!(4096, buf.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096..4096 + 4096])),
            format!("{:x}", Sha256::digest(&buf)),
            "read content"
        );

        let n = r.seek(SeekFrom::Start(0))?;
        assert_eq!(n, 0, "seek position must be 0");

        let mut buf = Vec::new();
        BlockingRead::read_to_end(&mut r, &mut buf)?;
        assert_eq!(4096, buf.len(), "read twice size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096..4096 + 4096])),
            format!("{:x}", Sha256::digest(&buf)),
            "read twice content"
        );

        let n = r.seek(SeekFrom::Start(1024))?;
        assert_eq!(1024, n, "seek to 1024");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf)?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 1024..4096 + 2048])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek 1024"
        );

        let n = r.seek(SeekFrom::Current(1024))?;
        assert_eq!(3072, n, "seek to 3072");

        let mut buf = vec![0; 1024];
        r.read_exact(&mut buf)?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[4096 + 3072..4096 + 3072 + 1024])),
            format!("{:x}", Sha256::digest(&buf)),
            "read after seek to 3072"
        );

        Ok(())
    }
}
