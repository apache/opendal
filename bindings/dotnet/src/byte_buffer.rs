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

#[repr(C)]
/// FFI-safe representation of a Rust `Vec<u8>` buffer.
///
/// The buffer ownership is transferred to the caller and must be released by
/// calling `buffer_free(data, len, capacity)` exactly once.
pub struct ByteBuffer {
    /// Pointer to the start of the allocated bytes.
    pub data: *mut u8,
    /// Number of initialized bytes.
    pub len: usize,
    /// Total allocated capacity in bytes.
    pub capacity: usize,
}

impl ByteBuffer {
    /// Create an empty buffer that does not own any allocation.
    pub fn empty() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }

    /// Convert a vector into a raw FFI buffer without copying.
    ///
    /// The returned memory must be released by the C# side via `buffer_free`.
    pub fn from_vec(mut value: Vec<u8>) -> Self {
        if value.is_empty() {
            return Self::empty();
        }

        let data = value.as_mut_ptr();
        let len = value.len();
        let capacity = value.capacity();
        std::mem::forget(value);

        Self {
            data,
            len,
            capacity,
        }
    }
}
