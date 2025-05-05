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

use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::IoSlice;
use std::io::{self, BufRead, Read, Seek, SeekFrom};
use std::mem;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures::Stream;

use crate::*;

/// Buffer is a wrapper of contiguous `Bytes` and non-contiguous `[Bytes]`.
///
/// We designed buffer to allow underlying storage to return non-contiguous bytes. For example,
/// http based storage like s3 could generate non-contiguous bytes by stream.
///
/// ## Features
///
/// - [`Buffer`] can be used as [`Buf`], [`Iterator`], [`Stream`] directly.
/// - [`Buffer`] is cheap to clone like [`Bytes`], only update reference count, no allocation.
/// - [`Buffer`] is vectorized write friendly, you can convert it to [`IoSlice`] for vectored write.
///
/// ## Examples
///
/// ### As `Buf`
///
/// `Buffer` implements `Buf` trait:
///
/// ```rust
/// use bytes::Buf;
/// use opendal::Buffer;
/// use serde_json;
///
/// fn test(mut buf: Buffer) -> Vec<String> {
///     serde_json::from_reader(buf.reader()).unwrap()
/// }
/// ```
///
/// ### As Bytes `Iterator`
///
/// `Buffer` implements `Iterator<Item=Bytes>` trait:
///
/// ```rust
/// use bytes::Bytes;
/// use opendal::Buffer;
///
/// fn test(mut buf: Buffer) -> Vec<Bytes> {
///     buf.into_iter().collect()
/// }
/// ```
///
/// ### As Bytes `Stream`
///
/// `Buffer` implements `Stream<Item=Result<Bytes, Infallible>>` trait:
///
/// ```rust
/// use bytes::Bytes;
/// use futures::TryStreamExt;
/// use opendal::Buffer;
///
/// async fn test(mut buf: Buffer) -> Vec<Bytes> {
///     buf.into_iter().try_collect().await.unwrap()
/// }
/// ```
///
/// ### As one contiguous Bytes
///
/// `Buffer` can make contiguous by transform into `Bytes` or `Vec<u8>`.
/// Please keep in mind that this operation involves new allocation and bytes copy, and we can't
/// reuse the same memory region anymore.
///
/// ```rust
/// use bytes::Bytes;
/// use opendal::Buffer;
///
/// fn test_to_vec(buf: Buffer) -> Vec<u8> {
///     buf.to_vec()
/// }
///
/// fn test_to_bytes(buf: Buffer) -> Bytes {
///     buf.to_bytes()
/// }
/// ```
#[derive(Clone)]
pub struct Buffer(Inner);

#[derive(Clone)]
enum Inner {
    Contiguous(Bytes),
    NonContiguous {
        parts: Arc<[Bytes]>,
        size: usize,
        idx: usize,
        offset: usize,
    },
}

impl Debug for Buffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut b = f.debug_struct("Buffer");

        match &self.0 {
            Inner::Contiguous(bs) => {
                b.field("type", &"contiguous");
                b.field("size", &bs.len());
            }
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                b.field("type", &"non_contiguous");
                b.field("parts", &parts);
                b.field("size", &size);
                b.field("idx", &idx);
                b.field("offset", &offset);
            }
        }
        b.finish_non_exhaustive()
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Buffer {
    /// Create a new empty buffer.
    ///
    /// This operation is const and no allocation will be performed.
    #[inline]
    pub const fn new() -> Self {
        Self(Inner::Contiguous(Bytes::new()))
    }

    /// Get the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.0 {
            Inner::Contiguous(b) => b.remaining(),
            Inner::NonContiguous { size, .. } => *size,
        }
    }

    /// Check if buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of [`Bytes`] in [`Buffer`].
    ///
    /// For contiguous buffer, it's always 1. For non-contiguous buffer, it's number of bytes
    /// available for use.
    pub fn count(&self) -> usize {
        match &self.0 {
            Inner::Contiguous(_) => 1,
            Inner::NonContiguous {
                parts,
                idx,
                size,
                offset,
            } => {
                parts
                    .iter()
                    .skip(*idx)
                    .fold((0, size + offset), |(count, size), bytes| {
                        if size == 0 {
                            (count, 0)
                        } else {
                            (count + 1, size.saturating_sub(bytes.len()))
                        }
                    })
                    .0
            }
        }
    }

    /// Get current [`Bytes`].
    pub fn current(&self) -> Bytes {
        match &self.0 {
            Inner::Contiguous(inner) => inner.clone(),
            Inner::NonContiguous {
                parts,
                idx,
                offset,
                size,
            } => {
                let chunk = &parts[*idx];
                let n = (chunk.len() - *offset).min(*size);
                chunk.slice(*offset..*offset + n)
            }
        }
    }

    /// Shortens the buffer, keeping the first `len` bytes and dropping the rest.
    ///
    /// If `len` is greater than the buffer’s current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match &mut self.0 {
            Inner::Contiguous(bs) => bs.truncate(len),
            Inner::NonContiguous { size, .. } => {
                *size = (*size).min(len);
            }
        }
    }

    /// Returns a slice of self for the provided range.
    ///
    /// This will increment the reference count for the underlying memory and return a new Buffer handle set to the slice.
    ///
    /// This operation is O(1).
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let len = self.len();

        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("out of range"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        assert!(
            begin <= end,
            "range start must not be greater than end: {:?} <= {:?}",
            begin,
            end,
        );
        assert!(
            end <= len,
            "range end out of bounds: {:?} <= {:?}",
            end,
            len,
        );

        if end == begin {
            return Buffer::new();
        }

        let mut ret = self.clone();
        ret.truncate(end);
        ret.advance(begin);
        ret
    }

    /// Combine all bytes together into one single [`Bytes`].
    ///
    /// This operation is zero copy if the underlying bytes are contiguous.
    /// Otherwise, it will copy all bytes into one single [`Bytes`].
    /// Please use API from [`Buf`], [`Iterator`] or [`Stream`] whenever possible.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        match &self.0 {
            Inner::Contiguous(bytes) => bytes.clone(),
            Inner::NonContiguous {
                parts,
                size,
                idx: _,
                offset,
            } => {
                if parts.len() == 1 {
                    parts[0].slice(*offset..(*offset + *size))
                } else {
                    let mut ret = BytesMut::with_capacity(self.len());
                    ret.put(self.clone());
                    ret.freeze()
                }
            }
        }
    }

    /// Combine all bytes together into one single [`Vec<u8>`].
    ///
    /// This operation is not zero copy, it will copy all bytes into one single [`Vec<u8>`].
    /// Please use API from [`Buf`], [`Iterator`] or [`Stream`] whenever possible.
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut ret = Vec::with_capacity(self.len());
        ret.put(self.clone());
        ret
    }

    /// Convert buffer into a slice of [`IoSlice`] for vectored write.
    #[inline]
    pub fn to_io_slice(&self) -> Vec<IoSlice<'_>> {
        match &self.0 {
            Inner::Contiguous(bs) => vec![IoSlice::new(bs.chunk())],
            Inner::NonContiguous {
                parts, idx, offset, ..
            } => {
                let mut ret = Vec::with_capacity(parts.len() - *idx);
                let mut new_offset = *offset;
                for part in parts.iter().skip(*idx) {
                    ret.push(IoSlice::new(&part[new_offset..]));
                    new_offset = 0;
                }
                ret
            }
        }
    }
}

impl From<Vec<u8>> for Buffer {
    #[inline]
    fn from(bs: Vec<u8>) -> Self {
        Self(Inner::Contiguous(bs.into()))
    }
}

impl From<Bytes> for Buffer {
    #[inline]
    fn from(bs: Bytes) -> Self {
        Self(Inner::Contiguous(bs))
    }
}

impl From<String> for Buffer {
    #[inline]
    fn from(s: String) -> Self {
        Self(Inner::Contiguous(Bytes::from(s)))
    }
}

impl From<&'static [u8]> for Buffer {
    #[inline]
    fn from(s: &'static [u8]) -> Self {
        Self(Inner::Contiguous(Bytes::from_static(s)))
    }
}

impl From<&'static str> for Buffer {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self(Inner::Contiguous(Bytes::from_static(s.as_bytes())))
    }
}

impl FromIterator<u8> for Buffer {
    #[inline]
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self(Inner::Contiguous(Bytes::from_iter(iter)))
    }
}

impl From<VecDeque<Bytes>> for Buffer {
    #[inline]
    fn from(bs: VecDeque<Bytes>) -> Self {
        let size = bs.iter().map(Bytes::len).sum();
        Self(Inner::NonContiguous {
            parts: Vec::from(bs).into(),
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl From<Vec<Bytes>> for Buffer {
    #[inline]
    fn from(bs: Vec<Bytes>) -> Self {
        let size = bs.iter().map(Bytes::len).sum();
        Self(Inner::NonContiguous {
            parts: bs.into(),
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl From<Arc<[Bytes]>> for Buffer {
    #[inline]
    fn from(bs: Arc<[Bytes]>) -> Self {
        let size = bs.iter().map(Bytes::len).sum();
        Self(Inner::NonContiguous {
            parts: bs,
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl FromIterator<Bytes> for Buffer {
    #[inline]
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        let mut size = 0;
        let bs = iter.into_iter().inspect(|v| size += v.len());
        // This operation only needs one allocation from iterator to `Arc<[Bytes]>` instead
        // of iterator -> `Vec<Bytes>` -> `Arc<[Bytes]>`.
        let parts = Arc::from_iter(bs);
        Self(Inner::NonContiguous {
            parts,
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl Buf for Buffer {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        match &self.0 {
            Inner::Contiguous(b) => b.chunk(),
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                if *size == 0 {
                    return &[];
                }

                let chunk = &parts[*idx];
                let n = (chunk.len() - *offset).min(*size);
                &parts[*idx][*offset..*offset + n]
            }
        }
    }

    #[inline]
    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        match &self.0 {
            Inner::Contiguous(b) => {
                if dst.is_empty() {
                    return 0;
                }

                dst[0] = IoSlice::new(b.chunk());
                1
            }
            Inner::NonContiguous {
                parts, idx, offset, ..
            } => {
                if dst.is_empty() {
                    return 0;
                }

                let mut new_offset = *offset;
                parts
                    .iter()
                    .skip(*idx)
                    .zip(dst.iter_mut())
                    .map(|(part, dst)| {
                        *dst = IoSlice::new(&part[new_offset..]);
                        new_offset = 0;
                    })
                    .count()
            }
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        match &mut self.0 {
            Inner::Contiguous(b) => b.advance(cnt),
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                assert!(
                    cnt <= *size,
                    "cannot advance past {cnt} bytes, only {size} bytes left"
                );

                let mut new_idx = *idx;
                let mut new_offset = *offset;
                let mut remaining_cnt = cnt;
                while remaining_cnt > 0 {
                    let part_len = parts[new_idx].len();
                    let remaining_in_part = part_len - new_offset;

                    if remaining_cnt < remaining_in_part {
                        new_offset += remaining_cnt;
                        break;
                    }

                    remaining_cnt -= remaining_in_part;
                    new_idx += 1;
                    new_offset = 0;
                }

                *idx = new_idx;
                *offset = new_offset;
                *size -= cnt;
            }
        }
    }
}

impl Iterator for Buffer {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Inner::Contiguous(bs) => {
                if bs.is_empty() {
                    None
                } else {
                    Some(mem::take(bs))
                }
            }
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                if *size == 0 {
                    return None;
                }

                let chunk = &parts[*idx];
                let n = (chunk.len() - *offset).min(*size);
                let buf = chunk.slice(*offset..*offset + n);
                *size -= n;
                *offset += n;

                if *offset == chunk.len() {
                    *idx += 1;
                    *offset = 0;
                }

                Some(buf)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            Inner::Contiguous(bs) => {
                if bs.is_empty() {
                    (0, Some(0))
                } else {
                    (1, Some(1))
                }
            }
            Inner::NonContiguous { parts, idx, .. } => {
                let remaining = parts.len().saturating_sub(*idx);
                (remaining, Some(remaining))
            }
        }
    }
}

impl Stream for Buffer {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().next().map(Ok))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        Iterator::size_hint(self)
    }
}

impl Read for Buffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let chunk = self.chunk();
        let len = chunk.len().min(buf.len());
        buf[..len].copy_from_slice(&chunk[..len]);
        self.advance(len);
        Ok(len)
    }
}

impl Seek for Buffer {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let len = self.len() as u64;
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset < 0 {
                    len.checked_sub(offset.unsigned_abs())
                        .ok_or(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "invalid seek to a negative position",
                        ))?
                } else {
                    len.checked_add(offset as u64).ok_or(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "seek out of bounds",
                    ))?
                }
            }
            SeekFrom::Current(offset) => {
                let current_pos = (len - self.remaining() as u64) as i64;
                let new_pos = current_pos.checked_add(offset).ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "seek out of bounds",
                ))?;
                if new_pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid seek to a negative position",
                    ));
                }
                new_pos as u64
            }
        };

        if new_pos > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek out of bounds",
            ));
        }

        self.advance((new_pos - (len - self.remaining() as u64)) as usize);
        Ok(new_pos)
    }
}

impl BufRead for Buffer {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        let chunk = match &self.0 {
            Inner::Contiguous(b) => b.chunk(),
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                if *size == 0 {
                    return Ok(&[]);
                }

                let chunk = &parts[*idx];
                let n = (chunk.len() - *offset).min(*size);
                &parts[*idx][*offset..*offset + n]
            }
        };
        Ok(chunk)
    }

    fn consume(&mut self, amt: usize) {
        self.advance(amt);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rand::prelude::*;
    use std::io::{BufRead, Read, Seek, SeekFrom};

    use super::*;

    const EMPTY_SLICE: &[u8] = &[];

    #[test]
    fn test_contiguous_buffer() {
        let mut buf = Buffer::new();

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
        assert_eq!(buf.next(), None);
    }

    #[test]
    fn test_empty_non_contiguous_buffer() {
        let mut buf = Buffer::from(vec![Bytes::new()]);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
        assert_eq!(buf.next(), None);
    }

    #[test]
    fn test_non_contiguous_buffer_with_empty_chunks() {
        let mut buf = Buffer::from(vec![Bytes::from("a")]);

        assert_eq!(buf.remaining(), 1);
        assert_eq!(buf.chunk(), b"a");

        buf.advance(1);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
    }

    #[test]
    fn test_non_contiguous_buffer_with_next() {
        let mut buf = Buffer::from(vec![Bytes::from("a")]);

        assert_eq!(buf.remaining(), 1);
        assert_eq!(buf.chunk(), b"a");

        let bs = buf.next();

        assert_eq!(bs, Some(Bytes::from("a")));
        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
    }

    #[test]
    fn test_buffer_advance() {
        let mut buf = Buffer::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);

        assert_eq!(buf.remaining(), 3);
        assert_eq!(buf.chunk(), b"a");

        buf.advance(1);

        assert_eq!(buf.remaining(), 2);
        assert_eq!(buf.chunk(), b"b");

        buf.advance(1);

        assert_eq!(buf.remaining(), 1);
        assert_eq!(buf.chunk(), b"c");

        buf.advance(1);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);

        buf.advance(0);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
    }

    #[test]
    fn test_buffer_truncate() {
        let mut buf = Buffer::from(vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);

        assert_eq!(buf.remaining(), 3);
        assert_eq!(buf.chunk(), b"a");

        buf.truncate(100);

        assert_eq!(buf.remaining(), 3);
        assert_eq!(buf.chunk(), b"a");

        buf.truncate(2);

        assert_eq!(buf.remaining(), 2);
        assert_eq!(buf.chunk(), b"a");

        buf.truncate(0);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
    }

    /// This setup will return
    ///
    /// - A buffer
    /// - Total size of this buffer.
    /// - Total content of this buffer.
    fn setup_buffer() -> (Buffer, usize, Bytes) {
        let mut rng = thread_rng();

        let bs = (0..100)
            .map(|_| {
                let len = rng.gen_range(1..100);
                let mut buf = vec![0; len];
                rng.fill(&mut buf[..]);
                Bytes::from(buf)
            })
            .collect::<Vec<_>>();

        let total_size = bs.iter().map(|b| b.len()).sum::<usize>();
        let total_content = bs.iter().flatten().copied().collect::<Bytes>();
        let buf = Buffer::from(bs);

        (buf, total_size, total_content)
    }

    #[test]
    fn fuzz_buffer_advance() {
        let mut rng = thread_rng();

        let (mut buf, total_size, total_content) = setup_buffer();
        assert_eq!(buf.remaining(), total_size);
        assert_eq!(buf.to_bytes(), total_content);

        let mut cur = 0;
        // Loop at most 10000 times.
        let mut times = 10000;
        while !buf.is_empty() && times > 0 {
            times -= 1;

            let cnt = rng.gen_range(0..total_size - cur);
            cur += cnt;
            buf.advance(cnt);

            assert_eq!(buf.remaining(), total_size - cur);
            assert_eq!(buf.to_bytes(), total_content.slice(cur..));
        }
    }

    #[test]
    fn fuzz_buffer_iter() {
        let mut rng = thread_rng();

        let (mut buf, total_size, total_content) = setup_buffer();
        assert_eq!(buf.remaining(), total_size);
        assert_eq!(buf.to_bytes(), total_content);

        let mut cur = 0;
        while buf.is_empty() {
            let cnt = rng.gen_range(0..total_size - cur);
            cur += cnt;
            buf.advance(cnt);

            // Before next
            assert_eq!(buf.remaining(), total_size - cur);
            assert_eq!(buf.to_bytes(), total_content.slice(cur..));

            if let Some(bs) = buf.next() {
                assert_eq!(bs, total_content.slice(cur..cur + bs.len()));
                cur += bs.len();
            }

            // After next
            assert_eq!(buf.remaining(), total_size - cur);
            assert_eq!(buf.to_bytes(), total_content.slice(cur..));
        }
    }

    #[test]
    fn fuzz_buffer_truncate() {
        let mut rng = thread_rng();

        let (mut buf, total_size, total_content) = setup_buffer();
        assert_eq!(buf.remaining(), total_size);
        assert_eq!(buf.to_bytes(), total_content);

        let mut cur = 0;
        while buf.is_empty() {
            let cnt = rng.gen_range(0..total_size - cur);
            cur += cnt;
            buf.advance(cnt);

            // Before truncate
            assert_eq!(buf.remaining(), total_size - cur);
            assert_eq!(buf.to_bytes(), total_content.slice(cur..));

            let truncate_size = rng.gen_range(0..total_size - cur);
            buf.truncate(truncate_size);

            // After truncate
            assert_eq!(buf.remaining(), truncate_size);
            assert_eq!(
                buf.to_bytes(),
                total_content.slice(cur..cur + truncate_size)
            );

            // Try next after truncate
            if let Some(bs) = buf.next() {
                assert_eq!(bs, total_content.slice(cur..cur + bs.len()));
                cur += bs.len();
            }

            // After next
            assert_eq!(buf.remaining(), total_size - cur);
            assert_eq!(buf.to_bytes(), total_content.slice(cur..));
        }
    }

    #[test]
    fn test_read_trait() {
        let mut buffer = Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]);
        let mut output = vec![0; 5];
        let size = buffer.read(&mut output).unwrap();
        assert_eq!(size, 5);
        assert_eq!(&output, b"Hello");
    }

    #[test]
    fn test_seek_trait() {
        let mut buffer = Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]);
        buffer.seek(SeekFrom::Start(5)).unwrap();
        let mut output = vec![0; 5];
        buffer.read_exact(&mut output).unwrap();
        assert_eq!(&output, b"World");
    }

    #[test]
    fn test_bufread_trait() {
        let mut buffer = Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]);
        let mut output = String::new();
        buffer.read_to_string(&mut output).unwrap();
        assert_eq!(output, "HelloWorld");

        let mut buffer = Buffer::from(vec![Bytes::from("Hello"), Bytes::from("World")]);
        let buf = buffer.fill_buf().unwrap();
        assert_eq!(buf, b"Hello");
        buffer.consume(5);
        let buf = buffer.fill_buf().unwrap();
        assert_eq!(buf, b"World");
    }

    #[test]
    fn test_read_partial() {
        let mut buffer = Buffer::from(vec![Bytes::from("Partial"), Bytes::from("Read")]);
        let mut output = vec![0; 4];
        let size = buffer.read(&mut output).unwrap();
        assert_eq!(size, 4);
        assert_eq!(&output, b"Part");

        let size = buffer.read(&mut output).unwrap();
        assert_eq!(size, 3);
        assert_eq!(&output[..3], b"ial");
    }

    #[test]
    fn test_seek_and_read() {
        let mut buffer = Buffer::from(vec![Bytes::from("SeekAndRead")]);
        buffer.seek(SeekFrom::Start(4)).unwrap();
        let mut output = vec![0; 3];
        buffer.read_exact(&mut output).unwrap();
        assert_eq!(&output, b"And");
    }

    #[test]
    fn test_bufread_consume() {
        let mut buffer = Buffer::from(vec![Bytes::from("ConsumeTest")]);
        let buf = buffer.fill_buf().unwrap();
        assert_eq!(buf, b"ConsumeTest");
        buffer.consume(7);
        let buf = buffer.fill_buf().unwrap();
        assert_eq!(buf, b"Test");
    }

    #[test]
    fn test_empty_buffer() {
        let mut buffer = Buffer::new();
        let mut output = vec![0; 5];
        let size = buffer.read(&mut output).unwrap();
        assert_eq!(size, 0);
        assert_eq!(&output, &[0; 5]);
    }

    #[test]
    fn test_seek_out_of_bounds() {
        let mut buffer = Buffer::from(vec![Bytes::from("OutOfBounds")]);
        let result = buffer.seek(SeekFrom::Start(100));
        assert!(result.is_err());
    }
}
