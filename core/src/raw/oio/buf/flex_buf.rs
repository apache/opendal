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

use bytes::BytesMut;
use bytes::{Buf, BufMut, Bytes};

/// FlexBuf is a buffer that support frozen bytes and reuse existing allocated memory.
///
/// It's useful when we want to freeze the buffer and reuse the memory for the next buffer.
pub struct FlexBuf {
    cap: usize,
    buf: BytesMut,
    frozen: Option<Bytes>,
}

impl FlexBuf {
    /// Initializes a new `FlexBuf` with the given capacity.
    pub fn new(cap: usize) -> Self {
        FlexBuf {
            cap,

            buf: BytesMut::with_capacity(cap),
            frozen: None,
        }
    }

    /// Put slice into flex buf.
    ///
    /// Return 0 means the buffer is frozen.
    pub fn put(&mut self, bs: &[u8]) -> usize {
        if self.frozen.is_some() {
            return 0;
        }
        let n = self.buf.remaining_mut().min(bs.len());
        self.buf.put_slice(&bs[..n]);

        if !self.buf.has_remaining_mut() {
            let frozen = self.buf.split();
            self.frozen = Some(frozen.freeze());
        }

        n
    }

    /// Freeze the buffer no matter it's full or not.
    pub fn freeze(&mut self) {
        let frozen = self.buf.split();
        if frozen.is_empty() {
            return;
        }
        self.frozen = Some(frozen.freeze());
    }

    /// Get the frozen buffer.
    ///
    /// Return `None` if the buffer is not frozen.
    ///
    /// # Notes
    ///
    /// This operation did nothing to the buffer. We use `&mut self` just for make
    /// the API consistent with other APIs.
    pub fn get(&mut self) -> Option<Bytes> {
        self.frozen.clone()
    }

    // Advance the frozen buffer.
    ///
    /// # Panics
    ///
    /// Panic if the buffer is not frozen.
    pub fn advance(&mut self, cnt: usize) {
        let Some(bs) = self.frozen.as_mut() else {
            unreachable!("It must be a bug to advance on not frozen buffer")
        };
        bs.advance(cnt);

        if bs.is_empty() {
            self.frozen = None;
            // This reserve cloud be cheap since we can reuse already allocated memory.
            // (if all references to the frozen buffer are dropped)
            self.buf.reserve(self.cap);
        }
    }
}
