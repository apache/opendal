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

use bytes::Bytes;
use compio::buf::{IntoInner, IoBuf, IoVectoredBuf, OwnedIter, OwnedIterator};

use crate::Buffer;

#[derive(Debug, Clone)]
struct BufferOwnedIter {
    buf: Buffer,
    current: Bytes,
}

unsafe impl IoBuf for Buffer {
    fn as_buf_ptr(&self) -> *const u8 {
        self.current().as_ptr()
    }

    fn buf_len(&self) -> usize {
        self.current().len()
    }

    fn buf_capacity(&self) -> usize {
        // `Bytes` doesn't expose uninitialized capacity, so treat it as the same as `len`
        self.current().len()
    }
}

impl IoVectoredBuf for Buffer {
    fn as_dyn_bufs(&self) -> impl Iterator<Item = &dyn IoBuf> {
        self
    }

    fn owned_iter(self) -> Result<OwnedIter<impl OwnedIterator<Inner = Self>>, Self> {
        Ok(OwnedIter::new(BufferOwnedIter {
            current: self.current(),
            buf: self,
        }))
    }
}

impl IntoInner for BufferOwnedIter {
    type Inner = Buffer;

    fn into_inner(self) -> Self::Inner {
        self.buf
    }
}

impl OwnedIterator for BufferOwnedIter {
    fn next(mut self) -> Result<Self, Self::Inner> {
        let Some(current) = self.buf.next() else {
            return Err(self.buf);
        };
        self.current = current;
        Ok(self)
    }

    fn current(&self) -> &dyn IoBuf {
        &self.current
    }
}
