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

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::Read;
use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::Bytes;
use bytes::BytesMut;

use crate::raw::*;
use crate::*;

/// Cursor is the cursor for [`Bytes`] that implements [`oio::Read`]
#[derive(Default)]
pub struct Cursor {
    inner: Bytes,
    pos: u64,
}

impl Cursor {
    /// Create a new empty cursor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the remaining slice is empty.
    pub fn is_empty(&self) -> bool {
        self.pos as usize >= self.inner.len()
    }

    /// Returns the remaining slice.
    pub fn remaining_slice(&self) -> &[u8] {
        let len = self.pos.min(self.inner.len() as u64) as usize;
        &self.inner.as_ref()[len..]
    }

    /// Return the length of remaining slice.
    pub fn len(&self) -> usize {
        self.inner.len() - self.pos as usize
    }
}

impl From<Bytes> for Cursor {
    fn from(v: Bytes) -> Self {
        Cursor { inner: v, pos: 0 }
    }
}

impl From<Vec<u8>> for Cursor {
    fn from(v: Vec<u8>) -> Self {
        Cursor {
            inner: Bytes::from(v),
            pos: 0,
        }
    }
}

impl oio::Read for Cursor {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let n = Read::read(&mut self.remaining_slice(), buf).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read data from Cursor")
                .with_context("source", "Cursor")
                .set_source(err)
        })?;
        self.pos += n as u64;
        Poll::Ready(Ok(n))
    }

    fn poll_seek(&mut self, _: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::End(n) => (self.inner.len() as i64, n),
            SeekFrom::Current(n) => (self.pos as i64, n),
        };

        let n = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                )))
            }
        };
        self.pos = n;
        Poll::Ready(Ok(n))
    }

    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.is_empty() {
            Poll::Ready(None)
        } else {
            // The clone here is required as we don't want to change it.
            let bs = self.inner.clone().split_off(self.pos as usize);
            self.pos += bs.len() as u64;
            Poll::Ready(Some(Ok(bs)))
        }
    }
}

impl oio::BlockingRead for Cursor {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = Read::read(&mut self.remaining_slice(), buf).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read data from Cursor")
                .with_context("source", "Cursor")
                .set_source(err)
        })?;
        self.pos += n as u64;
        Ok(n)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::End(n) => (self.inner.len() as i64, n),
            SeekFrom::Current(n) => (self.pos as i64, n),
        };

        let n = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ))
            }
        };
        self.pos = n;
        Ok(n)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        if self.is_empty() {
            None
        } else {
            // The clone here is required as we don't want to change it.
            let bs = self.inner.clone().split_off(self.pos as usize);
            self.pos += bs.len() as u64;
            Some(Ok(bs))
        }
    }
}

impl oio::Stream for Cursor {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.is_empty() {
            return Poll::Ready(None);
        }

        let bs = self.inner.clone();
        self.pos += bs.len() as u64;
        Poll::Ready(Some(Ok(bs)))
    }

    fn poll_reset(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        self.pos = 0;
        Poll::Ready(Ok(()))
    }
}

/// ChunkedCursor is used represents a non-contiguous bytes in memory.
///
/// This is useful when we buffer users' random writes without copy. ChunkedCursor implements
/// [`oio::Stream`] so it can be used in [`oio::Write::pipe`] directly.
///
/// # TODO
///
/// we can do some compaction during runtime. For example, merge 4K data
/// into the same bytes instead.
#[derive(Clone)]
pub struct ChunkedCursor {
    inner: VecDeque<Bytes>,
    idx: usize,
}

impl Default for ChunkedCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedCursor {
    /// Create a new chunked cursor.
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
            idx: 0,
        }
    }

    /// Returns `true` if current cursor is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.len() <= self.idx
    }

    /// Return current bytes size of cursor.
    pub fn len(&self) -> usize {
        self.inner.iter().skip(self.idx).map(|v| v.len()).sum()
    }

    /// Clear the entire cursor.
    pub fn clear(&mut self) {
        self.idx = 0;
        self.inner.clear();
    }

    /// Push a new bytes into vector cursor.
    pub fn push(&mut self, bs: Bytes) {
        self.inner.push_back(bs);
    }

    /// split_off will split the cursor into two cursors at given size.
    ///
    /// After split, `self` will contains the `0..at` part and the returned cursor contains
    /// `at..` parts.
    ///
    /// # Panics
    ///
    /// - Panics if `at > len`
    /// - Panics if `idx != 0`, the cursor must be reset before split.
    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len(),
            "split_off at must smaller than current size"
        );
        assert_eq!(self.idx, 0, "split_off must reset cursor first");

        let mut chunks = VecDeque::new();
        let mut size = self.len() - at;

        while let Some(mut bs) = self.inner.pop_back() {
            match size.cmp(&bs.len()) {
                Ordering::Less => {
                    let remaining = bs.split_off(bs.len() - size);
                    chunks.push_front(remaining);
                    self.inner.push_back(bs);
                    break;
                }
                Ordering::Equal => {
                    chunks.push_front(bs);
                    break;
                }
                Ordering::Greater => {
                    size -= bs.len();
                    chunks.push_front(bs);
                }
            }
        }

        Self {
            inner: chunks,
            idx: 0,
        }
    }

    /// split_to will split the cursor into two cursors at given size.
    ///
    /// After split, `self` will contains the `at..` part and the returned cursor contains
    /// `0..at` parts.
    ///
    /// # Panics
    ///
    /// - Panics if `at > len`
    /// - Panics if `idx != 0`, the cursor must be reset before split.
    pub fn split_to(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len(),
            "split_to at must smaller than current size"
        );
        assert_eq!(self.idx, 0, "split_to must reset cursor first");

        let mut chunks = VecDeque::new();
        let mut size = at;

        while let Some(mut bs) = self.inner.pop_front() {
            match size.cmp(&bs.len()) {
                Ordering::Less => {
                    let remaining = bs.split_off(size);
                    chunks.push_back(bs);
                    self.inner.push_front(remaining);
                    break;
                }
                Ordering::Equal => {
                    chunks.push_back(bs);
                    break;
                }
                Ordering::Greater => {
                    size -= bs.len();
                    chunks.push_back(bs);
                }
            }
        }

        Self {
            inner: chunks,
            idx: 0,
        }
    }

    #[cfg(test)]
    fn concat(&self) -> Bytes {
        let mut bs = BytesMut::new();
        for v in self.inner.iter().skip(self.idx) {
            bs.extend_from_slice(v);
        }
        bs.freeze()
    }
}

impl oio::Stream for ChunkedCursor {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.is_empty() {
            return Poll::Ready(None);
        }

        let bs = self.inner[self.idx].clone();
        self.idx += 1;
        Poll::Ready(Some(Ok(bs)))
    }

    fn poll_reset(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        self.idx = 0;
        Poll::Ready(Ok(()))
    }
}

/// VectorCursor is the cursor for [`Vec<Bytes>`] that implements [`oio::Stream`]
pub struct VectorCursor {
    inner: VecDeque<Bytes>,
    size: usize,
}

impl Default for VectorCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorCursor {
    /// Create a new vector cursor.
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
            size: 0,
        }
    }

    /// Returns `true` if current vector is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Return current bytes size of current vector.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Push a new bytes into vector cursor.
    pub fn push(&mut self, bs: Bytes) {
        self.size += bs.len();
        self.inner.push_back(bs);
    }

    /// Pop a bytes from vector cursor.
    pub fn pop(&mut self) {
        let bs = self.inner.pop_back();
        self.size -= bs.expect("pop bytes must exist").len()
    }

    /// Clear the entire vector.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = 0;
    }

    /// Peak will read and copy exactly n bytes from current cursor
    /// without change it's content.
    ///
    /// This function is useful if you want to read a fixed size
    /// content to make sure it aligned.
    ///
    /// # Panics
    ///
    /// Panics if n is larger than current size.
    ///
    /// # TODO
    ///
    /// Optimize to avoid data copy.
    pub fn peak_exact(&self, n: usize) -> Bytes {
        assert!(n <= self.size, "peak size must smaller than current size");

        // Avoid data copy if n is smaller than first chunk.
        if self.inner[0].len() >= n {
            return self.inner[0].slice(..n);
        }

        let mut bs = BytesMut::with_capacity(n);
        let mut n = n;
        for b in &self.inner {
            if n == 0 {
                break;
            }
            let len = b.len().min(n);
            bs.extend_from_slice(&b[..len]);
            n -= len;
        }
        bs.freeze()
    }

    /// peak_at_least will read and copy at least n bytes from current
    /// cursor without change it's content.
    ///
    /// This function is useful if you only want to make sure the
    /// returning bytes is larger.
    ///
    /// # Panics
    ///
    /// Panics if n is larger than current size.
    ///
    /// # TODO
    ///
    /// Optimize to avoid data copy.
    pub fn peak_at_least(&self, n: usize) -> Bytes {
        assert!(n <= self.size, "peak size must smaller than current size");

        // Avoid data copy if n is smaller than first chunk.
        if self.inner[0].len() >= n {
            return self.inner[0].clone();
        }

        let mut bs = BytesMut::with_capacity(n);
        let mut n = n;
        for b in &self.inner {
            if n == 0 {
                break;
            }
            let len = b.len().min(n);
            bs.extend_from_slice(&b[..len]);
            n -= len;
        }
        bs.freeze()
    }

    /// peak all will read and copy all bytes from current cursor
    /// without change it's content.
    ///
    /// TODO: we need to find a way to avoid copy all content here.
    pub fn peak_all(&self) -> Bytes {
        // Avoid data copy if we only have one bytes.
        if self.inner.len() == 1 {
            return self.inner[0].clone();
        }

        let mut bs = BytesMut::with_capacity(self.len());
        for b in &self.inner {
            bs.extend_from_slice(b);
        }
        bs.freeze()
    }

    /// Take will consume n bytes from current cursor.
    ///
    /// # Panics
    ///
    /// Panics if n is larger than current size.
    pub fn take(&mut self, n: usize) {
        assert!(n <= self.size, "take size must smamller than current size");

        // Update current size
        self.size -= n;

        let mut n = n;
        while n > 0 {
            assert!(!self.inner.is_empty(), "inner must not be empty");

            if self.inner[0].len() <= n {
                n -= self.inner[0].len();
                self.inner.pop_front();
            } else {
                self.inner[0].advance(n);
                n = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use rand::RngCore;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::raw::oio::StreamExt;

    #[test]
    fn test_vector_cursor() {
        let mut vc = VectorCursor::new();

        vc.push(Bytes::from("hello"));
        vc.push(Bytes::from("world"));

        assert_eq!(vc.peak_exact(1), Bytes::from("h"));
        assert_eq!(vc.peak_exact(1), Bytes::from("h"));
        assert_eq!(vc.peak_exact(4), Bytes::from("hell"));
        assert_eq!(vc.peak_exact(10), Bytes::from("helloworld"));

        vc.take(1);
        assert_eq!(vc.peak_exact(1), Bytes::from("e"));
        vc.take(1);
        assert_eq!(vc.peak_exact(1), Bytes::from("l"));
        vc.take(5);
        assert_eq!(vc.peak_exact(1), Bytes::from("r"));
    }

    #[tokio::test]
    async fn test_chunked_cursor() -> Result<()> {
        let mut c = ChunkedCursor::new();

        c.push(Bytes::from("hello"));
        assert_eq!(c.len(), 5);
        assert!(!c.is_empty());

        c.push(Bytes::from("world"));
        assert_eq!(c.len(), 10);
        assert!(!c.is_empty());

        let bs = c.next().await.unwrap().unwrap();
        assert_eq!(bs, Bytes::from("hello"));
        assert_eq!(c.len(), 5);
        assert!(!c.is_empty());

        let bs = c.next().await.unwrap().unwrap();
        assert_eq!(bs, Bytes::from("world"));
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());

        c.reset().await?;
        assert_eq!(c.len(), 10);
        assert!(!c.is_empty());

        c.clear();
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());

        Ok(())
    }

    #[test]
    fn test_chunked_cursor_split_to() {
        let mut base = ChunkedCursor::new();
        base.push(Bytes::from("Hello"));
        base.push(Bytes::from("Wor"));
        base.push(Bytes::from("ld"));

        // Case 1: split less than first chunk
        let mut c1 = base.clone();
        let c2 = c1.split_to(3);

        assert_eq!(c1.len(), 7);
        assert_eq!(
            &c1.inner,
            &[Bytes::from("lo"), Bytes::from("Wor"), Bytes::from("ld")]
        );

        assert_eq!(c2.len(), 3);
        assert_eq!(&c2.inner, &[Bytes::from("Hel")]);

        // Case 2: split larger than first chunk
        let mut c1 = base.clone();
        let c2 = c1.split_to(6);

        assert_eq!(c1.len(), 4);
        assert_eq!(&c1.inner, &[Bytes::from("or"), Bytes::from("ld")]);

        assert_eq!(c2.len(), 6);
        assert_eq!(&c2.inner, &[Bytes::from("Hello"), Bytes::from("W")]);

        // Case 3: split at chunk edge
        let mut c1 = base.clone();
        let c2 = c1.split_to(8);

        assert_eq!(c1.len(), 2);
        assert_eq!(&c1.inner, &[Bytes::from("ld")]);

        assert_eq!(c2.len(), 8);
        assert_eq!(&c2.inner, &[Bytes::from("Hello"), Bytes::from("Wor")]);
    }

    #[test]
    fn test_chunked_cursor_split_off() {
        let mut base = ChunkedCursor::new();
        base.push(Bytes::from("Hello"));
        base.push(Bytes::from("Wor"));
        base.push(Bytes::from("ld"));

        // Case 1: split less than first chunk
        let mut c1 = base.clone();
        let c2 = c1.split_off(3);

        assert_eq!(c1.len(), 3);
        assert_eq!(&c1.inner, &[Bytes::from("Hel")]);

        assert_eq!(c2.len(), 7);
        assert_eq!(
            &c2.inner,
            &[Bytes::from("lo"), Bytes::from("Wor"), Bytes::from("ld")]
        );

        // Case 2: split larger than first chunk
        let mut c1 = base.clone();
        let c2 = c1.split_off(6);

        assert_eq!(c1.len(), 6);
        assert_eq!(&c1.inner, &[Bytes::from("Hello"), Bytes::from("W")]);

        assert_eq!(c2.len(), 4);
        assert_eq!(&c2.inner, &[Bytes::from("or"), Bytes::from("ld")]);

        // Case 3: split at chunk edge
        let mut c1 = base.clone();
        let c2 = c1.split_off(8);

        assert_eq!(c1.len(), 8);
        assert_eq!(&c1.inner, &[Bytes::from("Hello"), Bytes::from("Wor")]);

        assert_eq!(c2.len(), 2);
        assert_eq!(&c2.inner, &[Bytes::from("ld")]);
    }

    #[test]
    fn test_fuzz_chunked_cursor_split_to() {
        let mut rng = thread_rng();
        let mut expected = vec![];
        let mut total_size = 0;

        let mut cursor = ChunkedCursor::new();

        // Build Cursor
        let count = rng.gen_range(1..1000);
        for _ in 0..count {
            let size = rng.gen_range(1..100);
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);
            total_size += size;

            expected.extend_from_slice(&content);
            cursor.push(Bytes::from(content));
        }

        // Test Cursor
        for _ in 0..count {
            let mut cursor = cursor.clone();

            let at = rng.gen_range(0..total_size);
            let to = cursor.split_to(at);

            assert_eq!(cursor.len(), total_size - at);
            assert_eq!(
                format!("{:x}", Sha256::digest(&cursor.concat())),
                format!("{:x}", Sha256::digest(&expected[at..])),
            );

            assert_eq!(to.len(), at);
            assert_eq!(
                format!("{:x}", Sha256::digest(&to.concat())),
                format!("{:x}", Sha256::digest(&expected[0..at])),
            );
        }
    }

    #[test]
    fn test_fuzz_chunked_cursor_split_off() {
        let mut rng = thread_rng();
        let mut expected = vec![];
        let mut total_size = 0;

        let mut cursor = ChunkedCursor::new();

        // Build Cursor
        let count = rng.gen_range(1..1000);
        for _ in 0..count {
            let size = rng.gen_range(1..100);
            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);
            total_size += size;

            expected.extend_from_slice(&content);
            cursor.push(Bytes::from(content));
        }

        // Test Cursor
        for _ in 0..count {
            let mut cursor = cursor.clone();

            let at = rng.gen_range(0..total_size);
            let off = cursor.split_off(at);

            assert_eq!(cursor.len(), at);
            assert_eq!(
                format!("{:x}", Sha256::digest(&cursor.concat())),
                format!("{:x}", Sha256::digest(&expected[..at])),
            );

            assert_eq!(off.len(), total_size - at);
            assert_eq!(
                format!("{:x}", Sha256::digest(&off.concat())),
                format!("{:x}", Sha256::digest(&expected[at..])),
            );
        }
    }
}
