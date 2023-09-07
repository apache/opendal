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
use std::{cmp, ptr};

/// Buf is used to provide a trait similar to [`bytes::Buf`].
///
/// The biggest difference is that `Buf`'s `copy_to_slice` and `copy_to_bytes` only needs `&self`
/// instead of `&mut self`.
pub trait Buf: Send + Sync {
    /// Returns the number of bytes between the current position and the end of the buffer.
    ///
    /// This value is greater than or equal to the length of the slice returned by chunk().
    ///
    /// # Notes
    ///
    /// Implementations of remaining should ensure that the return value does not change unless a
    /// call is made to advance or any other function that is documented to change the Buf's
    /// current position.
    fn remaining(&self) -> usize;

    /// Returns a slice starting at the current position and of length between 0 and
    /// Buf::remaining(). Note that this can return shorter slice (this allows non-continuous
    /// internal representation).
    ///
    /// # Notes
    ///
    /// This function should never panic. Once the end of the buffer is reached, i.e.,
    /// Buf::remaining returns 0, calls to chunk() should return an empty slice.
    fn chunk(&self) -> &[u8];

    /// Advance the internal cursor of the Buf
    ///
    /// The next call to chunk() will return a slice starting cnt bytes further into the underlying buffer.
    ///
    /// Panics
    /// This function may panic if cnt > self.remaining().
    fn advance(&mut self, cnt: usize);

    /// Copies current chunk into dst.
    ///
    /// Returns the number of bytes copied.
    ///
    /// # Notes
    ///
    /// Users should not assume the returned bytes is the same as the Buf::remaining().
    fn copy_to_slice(&self, dst: &mut [u8]) -> usize {
        let src = self.chunk();
        let size = cmp::min(src.len(), dst.len());

        // # Safety
        //
        // `src` and `dst` are guaranteed have enough space for `size` bytes.
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), size);
        }

        size
    }

    /// Copies current chunk into a bytes.
    ///
    /// This function may be optimized by the underlying type to avoid actual copies.
    /// For example, Bytes implementation will do a shallow copy (ref-count increment).
    ///
    /// # Notes
    ///
    /// Users should not assume the returned bytes is the same as the Buf::remaining().
    fn copy_to_bytes(&self, len: usize) -> Bytes {
        let src = self.chunk();
        let size = cmp::min(src.len(), len);

        let mut ret = BytesMut::with_capacity(size);
        ret.extend_from_slice(&src[..size]);
        ret.freeze()
    }
}

macro_rules! deref_forward_buf {
    () => {
        fn remaining(&self) -> usize {
            (**self).remaining()
        }

        fn chunk(&self) -> &[u8] {
            (**self).chunk()
        }

        fn advance(&mut self, cnt: usize) {
            (**self).advance(cnt)
        }

        fn copy_to_slice(&self, dst: &mut [u8]) -> usize {
            (**self).copy_to_slice(dst)
        }

        fn copy_to_bytes(&self, len: usize) -> Bytes {
            (**self).copy_to_bytes(len)
        }
    };
}

impl<T: Buf + ?Sized> Buf for &mut T {
    deref_forward_buf!();
}

impl<T: Buf + ?Sized> Buf for Box<T> {
    deref_forward_buf!();
}

impl Buf for &[u8] {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        *self = &self[cnt..];
    }
}

impl<T: AsRef<[u8]> + Send + Sync> Buf for std::io::Cursor<T> {
    fn remaining(&self) -> usize {
        let len = self.get_ref().as_ref().len();
        let pos = self.position();

        if pos >= len as u64 {
            return 0;
        }

        len - pos as usize
    }

    fn chunk(&self) -> &[u8] {
        let len = self.get_ref().as_ref().len();
        let pos = self.position();

        if pos >= len as u64 {
            return &[];
        }

        &self.get_ref().as_ref()[pos as usize..]
    }

    fn advance(&mut self, cnt: usize) {
        let pos = (self.position() as usize)
            .checked_add(cnt)
            .expect("overflow");

        assert!(pos <= self.get_ref().as_ref().len());
        self.set_position(pos as u64);
    }
}

impl Buf for Bytes {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        bytes::Buf::advance(self, cnt)
    }

    #[inline]
    fn copy_to_bytes(&self, len: usize) -> Bytes {
        let size = cmp::min(self.len(), len);
        self.slice(..size)
    }
}

impl Buf for BytesMut {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        bytes::Buf::advance(self, cnt)
    }

    #[inline]
    fn copy_to_bytes(&self, len: usize) -> Bytes {
        let size = cmp::min(self.len(), len);
        Bytes::copy_from_slice(&self[..size])
    }
}
