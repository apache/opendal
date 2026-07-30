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

//! Read payloads travelling from this crate to the caller.
//!
//! Bytes going the other way, such as write payloads, are passed as a plain
//! pointer and length, because the caller already owns that memory.

use std::ffi::c_void;
use std::ptr;

#[repr(C)]
/// Read payload owned by this crate.
///
/// Holds the [`opendal::Buffer`] itself rather than a flattened copy, so the
/// caller can copy straight into its final destination. `handle` must be
/// released with `opendal_read_result_release` exactly once.
pub struct OpendalBuffer {
    /// Leaked `Box<opendal::Buffer>`, or null when there is no payload.
    pub handle: *mut c_void,
    /// Total readable bytes across every chunk.
    pub len: usize,
}

impl OpendalBuffer {
    /// Create an empty payload that does not own anything.
    pub fn empty() -> Self {
        Self {
            handle: ptr::null_mut(),
            len: 0,
        }
    }

    /// Take ownership of a buffer without flattening or copying it.
    pub fn from_buffer(value: opendal::Buffer) -> Self {
        let len = value.len();
        if len == 0 {
            return Self::empty();
        }

        Self {
            handle: Box::into_raw(Box::new(value)) as *mut c_void,
            len,
        }
    }
}

/// Copy the whole payload into `dest`, returning the number of bytes written.
///
/// Copies at most `dest_len` bytes. An `opendal::Buffer` may be split across
/// several chunks, so this walks them and writes each one directly into the
/// destination; that is the only copy a read performs.
/// # Safety
///
/// - `handle` must be null or come from `OpendalBuffer::from_buffer`.
/// - `dest` must be valid for writes of `dest_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn buffer_copy_to(
    handle: *const c_void,
    dest: *mut u8,
    dest_len: usize,
) -> usize {
    if handle.is_null() || dest.is_null() || dest_len == 0 {
        return 0;
    }

    // Iterating consumes the buffer, so work on a clone. `Buffer` is backed by
    // `Bytes`, meaning this only bumps reference counts.
    let chunks = unsafe { &*(handle as *const opendal::Buffer) }.clone();

    let mut written = 0usize;
    for chunk in chunks {
        if written == dest_len {
            break;
        }

        let take = chunk.len().min(dest_len - written);
        unsafe {
            ptr::copy_nonoverlapping(chunk.as_ptr(), dest.add(written), take);
        }
        written += take;
    }

    written
}

/// # Safety
///
/// - `handle` must be null or come from `OpendalBuffer::from_buffer`.
/// - This function must be called at most once for the same handle.
/// - Callers must not use `handle` after this function returns.
pub unsafe fn buffer_free(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }

    drop(unsafe { Box::from_raw(handle as *mut opendal::Buffer) });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three parts, so the copy has to advance the destination across chunks.
    /// Backends that hand back a single contiguous buffer never exercise this.
    fn split_buffer() -> opendal::Buffer {
        opendal::Buffer::from(vec![
            bytes::Bytes::from_static(b"abc"),
            bytes::Bytes::from_static(b"de"),
            bytes::Bytes::from_static(b"fgh"),
        ])
    }

    fn copy(buffer: &opendal::Buffer, dest: &mut [u8]) -> usize {
        let owned = OpendalBuffer::from_buffer(buffer.clone());
        let written = unsafe { buffer_copy_to(owned.handle, dest.as_mut_ptr(), dest.len()) };
        unsafe { buffer_free(owned.handle) };
        written
    }

    #[test]
    fn copies_every_chunk_in_order() {
        let buffer = split_buffer();
        let mut dest = vec![0u8; buffer.len()];

        assert_eq!(copy(&buffer, &mut dest), 8);
        assert_eq!(&dest, b"abcdefgh");
    }

    #[test]
    fn stops_at_the_destination_length() {
        let mut dest = vec![0u8; 4];

        assert_eq!(copy(&split_buffer(), &mut dest), 4);
        assert_eq!(&dest, b"abcd");
    }

    #[test]
    fn honors_a_slice_that_starts_mid_chunk() {
        // Ranged reads produce exactly this: a non-contiguous buffer whose logical
        // start sits inside the second part.
        let buffer = split_buffer().slice(4..7);
        let mut dest = vec![0u8; buffer.len()];

        assert_eq!(copy(&buffer, &mut dest), 3);
        assert_eq!(&dest, b"efg");
    }

    #[test]
    fn empty_payload_owns_nothing() {
        let owned = OpendalBuffer::from_buffer(opendal::Buffer::new());

        assert!(owned.handle.is_null());
        assert_eq!(owned.len, 0);
        unsafe { buffer_free(owned.handle) };
    }
}
