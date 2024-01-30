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
use std::collections::VecDeque;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use bytes::BytesMut;
use futures::Stream;

use crate::raw::*;
use crate::*;

// TODO: 64KiB is picked based on experiences, should be configurable
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// ChunkedBytes is used represents a non-contiguous bytes in memory.
#[derive(Clone)]
pub struct ChunkedBytes {
    chunk_size: usize,

    frozen: VecDeque<Bytes>,
    active: BytesMut,
    size: usize,
}

impl Default for ChunkedBytes {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkedBytes {
    /// Create a new chunked bytes.
    pub fn new() -> Self {
        Self {
            frozen: VecDeque::new(),
            active: BytesMut::new(),
            size: 0,

            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Create a new chunked cursor with given chunk size.
    pub fn with_chunk_size(chunk_size: usize) -> Self {
        Self {
            frozen: VecDeque::new(),
            active: BytesMut::new(),
            size: 0,

            chunk_size,
        }
    }

    /// Build a chunked bytes from a vector of bytes.
    ///
    /// This function is guaranteed to run in O(1) time and to not re-allocate the Vec’s buffer
    /// or allocate any additional memory.
    ///
    /// Reference: <https://doc.rust-lang.org/stable/std/collections/struct.VecDeque.html#impl-From%3CVec%3CT,+A%3E%3E-for-VecDeque%3CT,+A%3E>
    pub fn from_vec(bs: Vec<Bytes>) -> Self {
        Self {
            size: bs.iter().map(|v| v.len()).sum(),
            frozen: bs.into(),
            active: BytesMut::new(),

            chunk_size: DEFAULT_CHUNK_SIZE,
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
        self.frozen.clear();
        self.active.clear();
    }

    /// Push a new bytes into ChunkedBytes.
    pub fn push(&mut self, mut bs: Bytes) {
        self.size += bs.len();

        // Optimization: if active is empty, we can push to frozen directly if possible.
        if self.active.is_empty() {
            let aligned_size = bs.len() - bs.len() % self.chunk_size;
            if aligned_size > 0 {
                self.frozen.push_back(bs.split_to(aligned_size));
            }
            if !bs.is_empty() {
                self.active.extend_from_slice(&bs);
            }
            return;
        }

        // Try to fill bytes into active first.
        let remaining = self.chunk_size.saturating_sub(self.active.len());
        if remaining > 0 {
            let len = min(remaining, bs.len());
            self.active.extend_from_slice(&bs.split_to(len));
        }

        // If active is full, freeze it and push it into frozen.
        if self.active.len() == self.chunk_size {
            self.frozen.push_back(self.active.split().freeze());
        }

        // Split remaining bytes into chunks.
        let aligned_size = bs.len() - bs.len() % self.chunk_size;
        if aligned_size > 0 {
            self.frozen.push_back(bs.split_to(aligned_size));
        }

        // Append to active if there are remaining bytes.
        if !bs.is_empty() {
            self.active.extend_from_slice(&bs);
        }
    }

    /// Push a new &[u8] into ChunkedBytes.
    pub fn extend_from_slice(&mut self, bs: &[u8]) {
        self.size += bs.len();

        let mut remaining = bs;

        while !remaining.is_empty() {
            let available = self.chunk_size.saturating_sub(self.active.len());

            // available == 0 means self.active.len() >= CHUNK_SIZE
            if available == 0 {
                self.frozen.push_back(self.active.split().freeze());
                self.active.reserve(self.chunk_size);
                continue;
            }

            let size = min(remaining.len(), available);
            self.active.extend_from_slice(&remaining[0..size]);

            remaining = &remaining[size..];
        }
    }

    /// Pull data from [`oio::WriteBuf`] into ChunkedBytes.
    pub fn extend_from_write_buf(&mut self, size: usize, buf: &dyn oio::WriteBuf) -> usize {
        let to_write = min(buf.chunk().len(), size);

        if buf.is_bytes_optimized(to_write) && to_write > self.chunk_size {
            // If the chunk is optimized, we can just push it directly.
            self.push(buf.bytes(to_write));
        } else {
            // Otherwise, we should copy it into the buffer.
            self.extend_from_slice(&buf.chunk()[..to_write]);
        }

        to_write
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

        while cnt > 0 {
            if let Some(front) = self.frozen.front_mut() {
                if front.len() <= cnt {
                    cnt -= front.len();
                    self.frozen.pop_front(); // Remove the entire chunk.
                } else {
                    front.advance(cnt); // Split and keep the remaining part.
                    break;
                }
            } else {
                // Here, cnt must be <= self.active.len() due to the checks above
                self.active.advance(cnt); // Remove cnt bytes from the active buffer.
                break;
            }
        }
    }

    fn chunk(&self) -> &[u8] {
        match self.frozen.front() {
            Some(v) => v,
            None => &self.active,
        }
    }

    fn vectored_chunk(&self) -> Vec<IoSlice> {
        let it = self.frozen.iter().map(|v| IoSlice::new(v));

        if !self.active.is_empty() {
            it.chain([IoSlice::new(&self.active)]).collect()
        } else {
            it.collect()
        }
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

        if let Some(bs) = self.frozen.front() {
            if size <= bs.len() {
                return bs.slice(..size);
            }
        }

        let mut remaining = size;
        let mut result = BytesMut::with_capacity(size);

        // First, go through the frozen buffer.
        for chunk in &self.frozen {
            let to_copy = min(remaining, chunk.len());
            result.extend_from_slice(&chunk[0..to_copy]);
            remaining -= to_copy;

            if remaining == 0 {
                break;
            }
        }

        // Then, get from the active buffer if necessary.
        if remaining > 0 {
            result.extend_from_slice(&self.active[0..remaining]);
        }

        result.freeze()
    }

    fn is_bytes_optimized(&self, size: usize) -> bool {
        if let Some(bs) = self.frozen.front() {
            return size <= bs.len();
        }

        false
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
        for bs in self.frozen.iter() {
            if remaining == 0 {
                break;
            }

            let to_take = min(remaining, bs.len());

            if to_take == bs.len() {
                buf.push(bs.clone()); // Clone is shallow; no data copy occurs.
            } else {
                buf.push(bs.slice(0..to_take));
            }

            remaining -= to_take;
        }

        if remaining > 0 {
            buf.push(Bytes::copy_from_slice(&self.active[0..remaining]));
        }

        buf
    }
}

impl oio::Stream for ChunkedBytes {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        match self.frozen.pop_front() {
            Some(bs) => {
                self.size -= bs.len();
                Poll::Ready(Some(Ok(bs)))
            }
            None if !self.active.is_empty() => {
                self.size -= self.active.len();
                Poll::Ready(Some(Ok(self.active.split().freeze())))
            }
            None => Poll::Ready(None),
        }
    }
}

impl Stream for ChunkedBytes {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.frozen.pop_front() {
            Some(bs) => {
                self.size -= bs.len();
                Poll::Ready(Some(Ok(bs)))
            }
            None if !self.active.is_empty() => {
                self.size -= self.active.len();
                Poll::Ready(Some(Ok(self.active.split().freeze())))
            }
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use log::debug;
    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use rand::RngCore;
    use sha2::Digest;
    use sha2::Sha256;

    use super::*;
    use crate::raw::oio::WriteBuf;

    #[test]
    fn test_chunked_bytes_write_buf() -> Result<()> {
        let mut c = ChunkedBytes::with_chunk_size(5);

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

    #[test]
    fn test_fuzz_chunked_bytes_push() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .pretty()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut rng = thread_rng();

        let chunk_size = rng.gen_range(1..10);
        let mut cb = ChunkedBytes::with_chunk_size(chunk_size);
        debug!("test_fuzz_chunked_bytes_push: chunk size: {chunk_size}");

        let mut expected = BytesMut::new();
        for _ in 0..1000 {
            let size = rng.gen_range(1..20);
            debug!("test_fuzz_chunked_bytes_push: write size: {size}");

            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);

            expected.extend_from_slice(&content);
            cb.push(Bytes::from(content.clone()));

            let cnt = rng.gen_range(0..expected.len());
            expected.advance(cnt);
            cb.advance(cnt);

            assert_eq!(expected.len(), cb.len());
            assert_eq!(
                format!("{:x}", Sha256::digest(&expected)),
                format!("{:x}", Sha256::digest(&cb.bytes(cb.len())))
            );
        }

        Ok(())
    }

    #[test]
    fn test_fuzz_chunked_bytes_extend_from_slice() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .pretty()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut rng = thread_rng();

        let chunk_size = rng.gen_range(1..10);
        let mut cb = ChunkedBytes::with_chunk_size(chunk_size);
        debug!("test_fuzz_chunked_bytes_extend_from_slice: chunk size: {chunk_size}");

        let mut expected = BytesMut::new();
        for _ in 0..1000 {
            let size = rng.gen_range(1..20);
            debug!("test_fuzz_chunked_bytes_extend_from_slice: write size: {size}");

            let mut content = vec![0; size];
            rng.fill_bytes(&mut content);

            expected.extend_from_slice(&content);
            cb.extend_from_slice(&content);

            let cnt = rng.gen_range(0..expected.len());
            expected.advance(cnt);
            cb.advance(cnt);

            assert_eq!(expected.len(), cb.len());
            assert_eq!(
                format!("{:x}", Sha256::digest(&expected)),
                format!("{:x}", Sha256::digest(&cb.bytes(cb.len())))
            );
        }

        Ok(())
    }
}
