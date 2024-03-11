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

use std::io::IoSlice;

use bytes::Bytes;
use bytes::BytesMut;

/// WriteBuf is used in [`oio::Write`] to provide in-memory buffer support.
pub trait WriteBuf: Send + Sync {
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

    /// Advance the internal cursor of the Buf
    ///
    /// The next call to chunk() will return a slice starting cnt bytes further into the underlying buffer.
    ///
    /// # Panics
    ///
    /// This function may panic if cnt > self.remaining().
    fn advance(&mut self, cnt: usize);

    /// Returns a slice starting at the current position and of length between 0 and
    /// Buf::remaining(). Note that this can return shorter slice (this allows non-continuous
    /// internal representation).
    ///
    /// # Notes
    ///
    /// This function should never panic. Once the end of the buffer is reached, i.e.,
    /// Buf::remaining returns 0, calls to chunk() should return an empty slice.
    fn chunk(&self) -> &[u8];

    /// Returns a vectored view of the underlying buffer at the current position and of
    /// length between 0 and Buf::remaining(). Note that this can return shorter slice
    /// (this allows non-continuous internal representation).
    ///
    /// # Notes
    ///
    /// This function should never panic.
    fn vectored_chunk(&self) -> Vec<IoSlice>;

    /// Returns a bytes starting at the current position and of length between 0 and
    /// Buf::remaining().
    ///
    /// # Notes
    ///
    /// This functions is used to concat a single bytes from underlying chunks.
    /// Use `vectored_bytes` if you want to avoid copy when possible.
    ///
    /// # Panics
    ///
    /// This function will panic if size > self.remaining().
    fn bytes(&self, size: usize) -> Bytes;

    /// Returns true if the underlying buffer is optimized for bytes with given size.
    ///
    /// # Notes
    ///
    /// This function is used to avoid copy when possible. Implementers should return `true`
    /// the given `self.bytes(size)` could be done without cost. For example, the underlying
    /// buffer is `Bytes`.
    ///
    /// # Panics
    ///
    /// This function will panic if size > self.remaining().
    fn is_bytes_optimized(&self, size: usize) -> bool {
        let _ = size;
        false
    }

    /// Returns a vectored bytes of the underlying buffer at the current position and of
    /// length between 0 and Buf::remaining().
    ///
    /// # Notes for Users
    ///
    /// This functions is used to return a vectored bytes from underlying chunks.
    /// Use `bytes` if you just want to get a continuous bytes.
    ///
    /// # Notes for implementers
    ///
    /// It's better to align the vectored bytes with underlying chunks to avoid copy.
    ///
    /// # Panics
    ///
    /// This function will panic if size > self.remaining().
    fn vectored_bytes(&self, size: usize) -> Vec<Bytes>;
}

macro_rules! deref_forward_buf {
    () => {
        fn remaining(&self) -> usize {
            (**self).remaining()
        }

        fn advance(&mut self, cnt: usize) {
            (**self).advance(cnt)
        }

        fn chunk(&self) -> &[u8] {
            (**self).chunk()
        }

        fn vectored_chunk(&self) -> Vec<IoSlice> {
            (**self).vectored_chunk()
        }

        fn bytes(&self, size: usize) -> Bytes {
            (**self).bytes(size)
        }

        fn is_bytes_optimized(&self, size: usize) -> bool {
            (**self).is_bytes_optimized(size)
        }

        fn vectored_bytes(&self, size: usize) -> Vec<Bytes> {
            (**self).vectored_bytes(size)
        }
    };
}

impl<T: WriteBuf + ?Sized> WriteBuf for &mut T {
    deref_forward_buf!();
}

impl<T: WriteBuf + ?Sized> WriteBuf for Box<T> {
    deref_forward_buf!();
}

impl WriteBuf for &[u8] {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        *self = &self[cnt..];
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self
    }

    #[inline]
    fn vectored_chunk(&self) -> Vec<IoSlice> {
        vec![IoSlice::new(self)]
    }

    #[inline]
    fn bytes(&self, size: usize) -> Bytes {
        Bytes::copy_from_slice(&self[..size])
    }

    #[inline]
    fn vectored_bytes(&self, size: usize) -> Vec<Bytes> {
        vec![self.bytes(size)]
    }
}

impl<T: AsRef<[u8]> + Send + Sync> WriteBuf for std::io::Cursor<T> {
    fn remaining(&self) -> usize {
        let len = self.get_ref().as_ref().len();
        let pos = self.position();

        if pos >= len as u64 {
            return 0;
        }

        len - pos as usize
    }

    fn advance(&mut self, cnt: usize) {
        let pos = (self.position() as usize)
            .checked_add(cnt)
            .expect("overflow");

        assert!(pos <= self.get_ref().as_ref().len());
        self.set_position(pos as u64);
    }

    fn chunk(&self) -> &[u8] {
        let len = self.get_ref().as_ref().len();
        let pos = self.position();

        if pos >= len as u64 {
            return &[];
        }

        &self.get_ref().as_ref()[pos as usize..]
    }

    fn vectored_chunk(&self) -> Vec<IoSlice> {
        vec![IoSlice::new(self.chunk())]
    }

    fn bytes(&self, size: usize) -> Bytes {
        Bytes::copy_from_slice(&self.chunk()[..size])
    }

    fn vectored_bytes(&self, size: usize) -> Vec<Bytes> {
        vec![self.bytes(size)]
    }
}

impl WriteBuf for Bytes {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        bytes::Buf::advance(self, cnt)
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self
    }

    #[inline]
    fn vectored_chunk(&self) -> Vec<IoSlice> {
        vec![IoSlice::new(self)]
    }

    #[inline]
    fn bytes(&self, size: usize) -> Bytes {
        self.slice(..size)
    }

    #[inline]
    fn is_bytes_optimized(&self, _: usize) -> bool {
        true
    }

    #[inline]
    fn vectored_bytes(&self, size: usize) -> Vec<Bytes> {
        vec![self.slice(..size)]
    }
}

impl WriteBuf for BytesMut {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        bytes::Buf::advance(self, cnt)
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self
    }

    #[inline]
    fn vectored_chunk(&self) -> Vec<IoSlice> {
        vec![IoSlice::new(self)]
    }

    #[inline]
    fn bytes(&self, size: usize) -> Bytes {
        Bytes::copy_from_slice(&self[..size])
    }

    #[inline]
    fn vectored_bytes(&self, size: usize) -> Vec<Bytes> {
        vec![self.bytes(size)]
    }
}
