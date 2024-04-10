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
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Buf;
use bytes::Bytes;
use futures::Stream;

/// Buffer is a wrapper of contiguous `Bytes` and non contiguous `[Bytes]`.
///
/// We designed buffer to allow underlying storage to return non-contiguous bytes. For example,
/// http based storage like s3 could generate non-contiguous bytes by stream.
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

impl Buffer {
    /// Create a new empty buffer.
    ///
    /// This operation is const and no allocation will be performed.
    #[inline]
    pub const fn new() -> Self {
        Self(Inner::Contiguous(Bytes::new()))
    }

    /// Clone internal bytes to a new `Bytes`.
    ///
    /// # Notes
    ///
    /// This operation is not efficient and should be used with caution.
    /// Please never use this method in read/write related APIs.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        let mut bs = self.clone();
        bs.copy_to_bytes(bs.remaining())
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
}

impl From<Vec<u8>> for Buffer {
    fn from(bs: Vec<u8>) -> Self {
        Self(Inner::Contiguous(bs.into()))
    }
}

impl From<Bytes> for Buffer {
    fn from(bs: Bytes) -> Self {
        Self(Inner::Contiguous(bs))
    }
}

impl From<String> for Buffer {
    fn from(s: String) -> Self {
        Self(Inner::Contiguous(Bytes::from(s)))
    }
}

impl From<&'static [u8]> for Buffer {
    fn from(s: &'static [u8]) -> Self {
        Self(Inner::Contiguous(Bytes::from_static(s)))
    }
}

impl From<&'static str> for Buffer {
    fn from(s: &'static str) -> Self {
        Self(Inner::Contiguous(Bytes::from_static(s.as_bytes())))
    }
}

impl FromIterator<u8> for Buffer {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Self(Inner::Contiguous(Bytes::from_iter(iter)))
    }
}

impl From<VecDeque<Bytes>> for Buffer {
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rand::prelude::*;

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
}
