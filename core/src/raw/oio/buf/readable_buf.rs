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

use bytes::{Buf, Bytes};
use std::ops::Deref;

/// ReadableBuf is the buf used in `oio::Write`.
///
/// This API is used internally by the `oio` crate. Users should never use it directly.
///
/// # Safety
///
/// Caller must make sure that input buffer lives longer than `ReadableBuf` otherwise `ReadableBuf`
/// might point to invalid memory.
#[derive(Clone)]
pub struct ReadableBuf(Inner);

#[derive(Clone)]
enum Inner {
    Slice {
        ptr: *const u8,
        size: usize,
        offset: usize,
    },
    Bytes(Bytes),
}

/// # Safety
///
/// We make sure that `ptr` will never be changed.
unsafe impl Send for Inner {}

impl ReadableBuf {
    /// Build a ReadableBuf from slice.
    ///
    /// # Performance
    ///
    /// This op itself is zero cost. But the buffer will be copied to Bytes when `to_bytes` is called.
    ///
    /// # Safety
    ///
    /// Users must ensure that the buffer is valid and will not be dropped before the `ReadableBuf`.
    pub fn from_slice(buf: &[u8]) -> Self {
        Self(Inner::Slice {
            ptr: buf.as_ptr(),
            size: buf.len(),
            offset: 0,
        })
    }

    /// Build a ReadableBuf from Bytes.
    ///
    /// # Performance
    ///
    /// This op itself is zero cost if input buffer can into bytes zero cost.
    pub fn from_bytes(buf: impl Into<Bytes>) -> Self {
        Self(Inner::Bytes(buf.into()))
    }

    /// Get the slice represents of the buffer.
    ///
    /// This op is zero cost.
    pub fn as_slice(&self) -> &[u8] {
        match &self.0 {
            Inner::Slice { ptr, size, offset } => unsafe {
                assert!(
                    !ptr.is_null(),
                    "ptr of slice must be valid across the lifetime of ReadableBuf"
                );
                std::slice::from_raw_parts((*ptr).add(*offset), *size - *offset)
            },
            Inner::Bytes(buf) => buf.as_ref(),
        }
    }

    /// Get the Bytes represents of the buffer.
    ///
    /// This op is cheap if the buffer is already in Bytes format. Otherwise, it will copy the
    /// the slice to Bytes.
    pub fn to_bytes(&self) -> Bytes {
        match &self.0 {
            Inner::Slice { ptr, size, offset } => unsafe {
                assert!(
                    !ptr.is_null(),
                    "ptr of slice must be valid across the lifetime of ReadableBuf"
                );

                Bytes::copy_from_slice(std::slice::from_raw_parts(
                    (*ptr).add(*offset),
                    *size - *offset,
                ))
            },
            Inner::Bytes(buf) => buf.clone(),
        }
    }

    /// Take the first limit bytes of the buffer.
    pub fn take(self, limit: usize) -> Self {
        match self.0 {
            Inner::Slice { ptr, size, offset } => {
                assert!(
                    !ptr.is_null(),
                    "ptr of slice must be valid across the lifetime of ReadableBuf"
                );
                assert!(size >= offset + limit, "limit exceeds the remaining size");
                Self(Inner::Slice {
                    ptr,
                    size: limit,
                    offset,
                })
            }
            Inner::Bytes(buf) => Self(Inner::Bytes(buf.slice(0..limit))),
        }
    }
}

impl From<&[u8]> for ReadableBuf {
    fn from(buf: &[u8]) -> Self {
        Self::from_slice(buf)
    }
}

impl From<Bytes> for ReadableBuf {
    fn from(buf: Bytes) -> Self {
        Self::from_bytes(buf)
    }
}

impl From<Vec<u8>> for ReadableBuf {
    fn from(value: Vec<u8>) -> Self {
        Self::from_bytes(Bytes::from(value))
    }
}

impl AsRef<[u8]> for ReadableBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for ReadableBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Buf for ReadableBuf {
    fn remaining(&self) -> usize {
        match &self.0 {
            Inner::Slice { size, offset, .. } => size - offset,
            Inner::Bytes(buf) => buf.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    fn advance(&mut self, cnt: usize) {
        match &mut self.0 {
            Inner::Slice {
                ptr, size, offset, ..
            } => {
                assert!(
                    !ptr.is_null(),
                    "ptr of slice must be valid across the lifetime of ReadableBuf"
                );
                assert!(
                    *size > *offset + cnt,
                    "cnt {cnt} exceeds the remaining size {}",
                    *size - *offset
                );
                *offset += cnt
            }
            Inner::Bytes(buf) => buf.advance(cnt),
        }
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        match &mut self.0 {
            Inner::Slice { ptr, size, offset } => {
                assert!(
                    !ptr.is_null(),
                    "ptr of slice must be valid across the lifetime of ReadableBuf"
                );
                assert!(
                    *size > *offset + len,
                    "len {len} exceeds the remaining size {}",
                    *size - *offset
                );
                let buf = unsafe {
                    Bytes::copy_from_slice(std::slice::from_raw_parts((*ptr).add(*offset), len))
                };
                *offset += len;
                buf
            }
            Inner::Bytes(buf) => buf.copy_to_bytes(len),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readable_buf_from_slice() {
        let buf = ReadableBuf::from_slice(b"hello world");
        assert_eq!(buf.remaining(), 11);
        assert_eq!(buf.chunk(), b"hello world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"hello world"));

        let mut buf = ReadableBuf::from_slice(b"hello world");
        assert_eq!(buf.copy_to_bytes(5), Bytes::from_static(b"hello"));
        assert_eq!(buf.remaining(), 6);
        assert_eq!(buf.chunk(), b" world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b" world"));
    }

    #[test]
    fn test_readable_buf_from_bytes() {
        let buf = ReadableBuf::from_bytes("hello world");
        assert_eq!(buf.remaining(), 11);
        assert_eq!(buf.chunk(), b"hello world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"hello world"));

        let mut buf = ReadableBuf::from_bytes("hello world");
        assert_eq!(buf.copy_to_bytes(5), Bytes::from_static(b"hello"));
        assert_eq!(buf.remaining(), 6);
        assert_eq!(buf.chunk(), b" world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b" world"));
    }

    #[test]
    fn test_readable_buf_from_reference() {
        let bs = "hello world".as_bytes();
        let buf = ReadableBuf::from_slice(bs);
        assert_eq!(buf.remaining(), 11);
        assert_eq!(buf.chunk(), b"hello world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"hello world"));

        let mut buf = ReadableBuf::from_slice(bs);
        assert_eq!(buf.copy_to_bytes(5), Bytes::from_static(b"hello"));
        assert_eq!(buf.remaining(), 6);
        assert_eq!(buf.chunk(), b" world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b" world"));
    }

    #[test]
    fn test_readable_buf_from_reference_of_vec() {
        let bs = "hello world".as_bytes().to_vec();
        let buf = ReadableBuf::from_slice(&bs);
        assert_eq!(buf.remaining(), 11);
        assert_eq!(buf.chunk(), b"hello world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b"hello world"));

        let mut buf = ReadableBuf::from_slice(&bs);
        assert_eq!(buf.copy_to_bytes(5), Bytes::from_static(b"hello"));
        assert_eq!(buf.remaining(), 6);
        assert_eq!(buf.chunk(), b" world");
        assert_eq!(buf.to_bytes(), Bytes::from_static(b" world"));
    }
}
