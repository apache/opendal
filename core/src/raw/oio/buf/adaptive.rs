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

use std::cmp;

use bytes::Bytes;
use bytes::BytesMut;
use tokio::io::ReadBuf;

/// The default minimum adaptive buffer size is 8 KiB.
const DEFAULT_MIN_BUFFER_SIZE: usize = 8192;

/// The default maximum adaptive buffer size is 4 MiB.
///
/// We will not grow the buffer beyond this size.
const DEFAULT_MAX_BUFFER_SIZE: usize = 4 * 1024 * 1024;

/// AdaptiveBuf is inspired by hyper [ReadStrategy](https://github.com/hyperium/hyper/blob/master/src/proto/h1/io.rs#L26).
///
/// We build this adaptive buf to make our internal buf grow and shrink automatically based on IO
/// throughput.
pub struct AdaptiveBuf {
    /// The underlying buffer.
    buffer: BytesMut,
    /// The next buffer size.
    next: usize,
    decrease_now: bool,
}

impl Default for AdaptiveBuf {
    fn default() -> Self {
        Self {
            buffer: BytesMut::default(),
            next: DEFAULT_MIN_BUFFER_SIZE,
            decrease_now: false,
        }
    }
}

impl AdaptiveBuf {
    /// reserve will reserve the buffer to the next size.
    pub fn reserve(&mut self) {
        if self.buffer.capacity() < self.next {
            self.buffer.reserve(self.next);
        }
    }

    /// Returning the initialized part of the buffer.
    pub fn initialized_mut(&mut self) -> ReadBuf {
        assert_eq!(
            self.buffer.len(),
            0,
            "buffer must be empty before initialized_mut"
        );

        let dst = self.buffer.spare_capacity_mut();
        let length = dst.len();
        let mut buf = ReadBuf::uninit(dst);

        // Safety: we make sure that we only return the initialized part of the buffer.
        unsafe {
            buf.assume_init(length);
        }
        buf
    }

    /// Records the number of bytes read from the underlying IO.
    pub fn record(&mut self, read: usize) {
        if read >= self.next {
            // Growing if we uses the whole buffer.
            self.next = cmp::min(self.next.saturating_mul(2), DEFAULT_MAX_BUFFER_SIZE);
            self.decrease_now = false;
        } else {
            // Shrinking if we uses less than half of the buffer.
            let decr_to = self.next.saturating_div(2);
            if read < decr_to {
                if self.decrease_now {
                    self.next = cmp::max(decr_to, DEFAULT_MIN_BUFFER_SIZE);
                    self.decrease_now = false;
                } else {
                    // Mark decrease_now as true to shrink the buffer next time.
                    self.decrease_now = true;
                }
            } else {
                // Mark decrease_now as false to keep current buffer size.
                self.decrease_now = false;
            }
        }
    }

    /// Splits the buffer into two at the given index.
    ///
    /// # Safety
    ///
    /// It's required that buffer has been filled with given bytes.
    pub fn split(&mut self, n: usize) -> Bytes {
        unsafe { self.buffer.set_len(n) }
        self.buffer.split().freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_strategy_adaptive_decrements() {
        let mut huf = AdaptiveBuf::default();
        huf.record(8192);
        assert_eq!(huf.next, 16384);

        huf.record(1);
        assert_eq!(
            huf.next, 16384,
            "first smaller record doesn't decrement yet"
        );
        huf.record(8192);
        assert_eq!(huf.next, 16384, "record was with range");

        huf.record(1);
        assert_eq!(
            huf.next, 16384,
            "in-range record should make this the 'first' again"
        );

        huf.record(1);
        assert_eq!(huf.next, 8192, "second smaller record decrements");

        huf.record(1);
        assert_eq!(huf.next, 8192, "first doesn't decrement");
        huf.record(1);
        assert_eq!(huf.next, 8192, "doesn't decrement under minimum");
    }
}
