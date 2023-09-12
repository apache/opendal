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

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::IoSlice;
use std::task::{Context, Poll};

use crate::raw::*;
use crate::*;

/// ChunkedBytes is used represents a non-contiguous bytes in memory.
#[derive(Clone)]
pub struct ChunkedBytes {
    inner: VecDeque<Bytes>,
    size: usize,
}

impl Default for ChunkedBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedBytes {
    /// Create a new chunked cursor.
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
            size: 0,
        }
    }

    /// Returns `true` if current cursor is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Return current bytes size of cursor.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Clear the entire cursor.
    pub fn clear(&mut self) {
        self.size = 0;
        self.inner.clear();
    }

    /// Push a new bytes into vector cursor.
    pub fn push(&mut self, bs: Bytes) {
        self.size += bs.len();
        self.inner.push_back(bs);
    }
}

impl oio::WriteBuf for ChunkedBytes {
    fn remaining(&self) -> usize {
        self.size
    }

    fn advance(&mut self, mut cnt: usize) {
        debug_assert!(
            cnt <= self.size,
            "cnt size {} is larger than bytes size {}",
            cnt,
            self.size
        );

        self.size -= cnt;
        loop {
            if cnt == 0 {
                break;
            }

            let bs = self.inner.front_mut().unwrap();
            if cnt >= bs.len() {
                cnt -= bs.len();
                self.inner.pop_front();
            } else {
                bs.advance(cnt);
                cnt = 0;
            }
        }
    }

    fn chunk(&self) -> &[u8] {
        match self.inner.front() {
            Some(v) => v,
            None => &[],
        }
    }

    fn vectored_chunk(&self) -> Vec<IoSlice> {
        self.inner.iter().map(|v| IoSlice::new(v)).collect()
    }

    fn bytes(&self, size: usize) -> Bytes {
        debug_assert!(
            size <= self.size,
            "input size {} is larger than bytes size {}",
            size,
            self.size
        );

        if size == 0 {
            return Bytes::new();
        }

        if let Some(bs) = self.inner.front() {
            if size <= bs.len() {
                return bs.slice(..size);
            }
        }

        let mut remaining = size;
        let mut buf = BytesMut::with_capacity(size);
        for bs in self.inner.iter() {
            if remaining == 0 {
                break;
            }

            if remaining <= bs.len() {
                buf.extend_from_slice(&bs[..remaining]);
                break;
            }

            buf.extend_from_slice(bs);
            remaining -= bs.len();
        }

        buf.freeze()
    }

    fn vectored_bytes(&self, size: usize) -> Vec<Bytes> {
        debug_assert!(
            size <= self.size,
            "input size {} is larger than bytes size {}",
            size,
            self.size
        );

        let mut remaining = size;
        let mut buf = vec![];
        for bs in self.inner.iter() {
            if remaining == 0 {
                break;
            }

            if remaining <= bs.len() {
                buf.push(bs.slice(..remaining));
                break;
            }

            buf.push(bs.clone());
            remaining -= bs.len();
        }

        buf
    }
}

impl oio::Stream for ChunkedBytes {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.inner.pop_front() {
            Some(bs) => {
                self.size -= bs.len();
                Poll::Ready(Some(Ok(bs)))
            }
            None => Poll::Ready(None),
        }
    }

    fn poll_reset(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "ChunkedBytes does not support reset",
        )))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::raw::oio::{StreamExt, WriteBuf};

    #[test]
    fn test_chunked_bytes_write_buf() -> Result<()> {
        let mut c = ChunkedBytes::new();

        c.push(Bytes::from("hello"));
        assert_eq!(c.len(), 5);
        assert!(!c.is_empty());

        c.push(Bytes::from("world"));
        assert_eq!(c.len(), 10);
        assert!(!c.is_empty());

        // Test chunk
        let bs = c.chunk();
        assert_eq!(bs, "hello".as_bytes());
        assert_eq!(c.len(), 10);
        assert!(!c.is_empty());

        // The second chunk should return the same content.
        let bs = c.chunk();
        assert_eq!(bs, "hello".as_bytes());
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());

        // Test vectored chunk
        let bs = c.vectored_chunk();
        assert_eq!(
            bs.iter().map(|v| v.as_ref()).collect::<Vec<_>>(),
            vec!["hello".as_bytes(), "world".as_bytes()]
        );
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());

        // Test bytes
        let bs = c.bytes(4);
        assert_eq!(bs, Bytes::from("hell"));
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());

        // Test bytes again
        let bs = c.bytes(6);
        assert_eq!(bs, Bytes::from("hellow"));
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());

        // Test vectored bytes
        let bs = c.vectored_bytes(4);
        assert_eq!(bs, vec![Bytes::from("hell")]);
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());

        // Test vectored bytes again
        let bs = c.vectored_bytes(6);
        assert_eq!(bs, vec![Bytes::from("hello"), Bytes::from("w")]);
        assert_eq!(c.remaining(), 10);
        assert!(!c.is_empty());

        // Test Advance.
        c.advance(4);

        // Test chunk
        let bs = c.chunk();
        assert_eq!(bs, "o".as_bytes());
        assert_eq!(c.len(), 6);
        assert!(!c.is_empty());

        c.clear();
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_bytes_stream() -> Result<()> {
        let mut c = ChunkedBytes::new();

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

        c.clear();
        assert_eq!(c.len(), 0);
        assert!(c.is_empty());

        Ok(())
    }
}
