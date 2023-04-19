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
pub struct Cursor {
    inner: Bytes,
    pos: u64,
}

impl Cursor {
    /// Returns `true` if the remaining slice is empty.
    pub fn is_empty(&self) -> bool {
        self.pos as usize >= self.inner.len()
    }

    /// Returns the remaining slice.
    pub fn remaining_slice(&self) -> &[u8] {
        let len = self.pos.min(self.inner.len() as u64) as usize;
        &self.inner.as_ref()[len..]
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
                    ErrorKind::Unexpected,
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
                    ErrorKind::Unexpected,
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

/// VectorCursor is the cursor for [`Vec<Bytes>`] that implements [`oio::Read`]
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
    use super::*;

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
}
