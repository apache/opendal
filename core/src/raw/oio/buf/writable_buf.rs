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

use std::mem::MaybeUninit;

use bytes::buf::UninitSlice;
use bytes::BufMut;

/// WritableBuf is the buf used in `oio::Read`.
///
/// This API is used internally by the `oio` crate. Users SHOULD never use it in any way.
///
/// # Safety
///
/// - Caller MUST make sure that input buffer lives longer than `WritableBuf`. Otherwise
///   `WritableBuf` might point to invalid memory.
/// - Caller MUST not mutate the original buffer in any way out of `WritableBuf`.
/// - Caller SHOULD NOT remote from `WritableBuf` in anyway.
#[derive(Copy, Clone)]
pub struct WritableBuf {
    ptr: *mut u8,
    size: usize,
    offset: usize,
}

/// # Safety
///
/// We make sure that `ptr` itself will never be changed.
unsafe impl Send for WritableBuf {}
/// # Safety
///
/// We make sure that `ptr` itself will never be changed.
unsafe impl Sync for WritableBuf {}

impl WritableBuf {
    /// Build a WritableBuf from slice.
    pub fn from_slice(slice: &mut [u8]) -> Self {
        Self {
            ptr: slice.as_mut_ptr(),
            size: slice.len(),
            offset: 0,
        }
    }

    /// Build a WritableBuf from maybe uninit.
    pub fn from_maybe_uninit_slice(slice: &mut [MaybeUninit<u8>]) -> Self {
        Self {
            ptr: slice.as_mut_ptr() as *mut u8,
            size: slice.len(),
            offset: 0,
        }
    }

    /// Build a WritableBuf from mutable BufMut.
    pub fn from_buf_mut(buf: &mut impl BufMut) -> Self {
        let slice = buf.chunk_mut();
        Self {
            ptr: slice.as_mut_ptr(),
            size: slice.len(),
            offset: 0,
        }
    }

    /// Get the slice represents of the buffer.
    ///
    /// This op is zero cost.
    pub fn as_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr.add(self.offset), self.size - self.offset)
        }
    }
}

unsafe impl BufMut for WritableBuf {
    fn remaining_mut(&self) -> usize {
        assert!(
            !self.ptr.is_null(),
            "ptr of slice must be valid across the lifetime of WritableBuf"
        );
        self.size - self.offset
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        assert!(
            !self.ptr.is_null(),
            "ptr of slice must be valid across the lifetime of WritableBuf"
        );
        assert!(
            self.size >= self.offset + cnt,
            "cnt {cnt} exceeds the remaining size {}",
            self.size - self.offset
        );
        self.offset += cnt;
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        assert!(
            !self.ptr.is_null(),
            "ptr of slice must be valid across the lifetime of WritableBuf"
        );

        unsafe {
            UninitSlice::from_raw_parts_mut(self.ptr.add(self.offset), self.size - self.offset)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_writable_buf_from_slice() {
        let mut buf = [0u8; 10];
        let mut writable_buf = WritableBuf::from_slice(&mut buf);
        assert_eq!(writable_buf.remaining_mut(), 10);
        assert_eq!(writable_buf.chunk_mut().len(), 10);

        writable_buf.put_slice(b"hello");

        assert_eq!(writable_buf.remaining_mut(), 5);
        assert_eq!(writable_buf.chunk_mut().len(), 5);
        assert_eq!(&buf[..5], b"hello");

        writable_buf.put_slice(b"world");
        assert_eq!(writable_buf.remaining_mut(), 0);
        assert_eq!(writable_buf.chunk_mut().len(), 0);
        assert_eq!(&buf[..], b"helloworld");
    }

    #[test]
    fn test_writable_buf_from_bytes_mut() {
        let mut buf = BytesMut::with_capacity(10);
        let mut writable_buf = WritableBuf::from_buf_mut(&mut buf);
        assert_eq!(writable_buf.remaining_mut(), 10);
        assert_eq!(writable_buf.chunk_mut().len(), 10);

        writable_buf.put_slice(b"hello");
        assert_eq!(writable_buf.remaining_mut(), 5);
        assert_eq!(writable_buf.chunk_mut().len(), 5);

        writable_buf.put_slice(b"world");
        assert_eq!(writable_buf.remaining_mut(), 0);
        assert_eq!(writable_buf.chunk_mut().len(), 0);

        unsafe {
            buf.advance_mut(5);
        }
        assert_eq!(&buf[..5], b"hello");

        unsafe {
            buf.advance_mut(5);
        }
        assert_eq!(&buf[..], b"helloworld");
    }
}
