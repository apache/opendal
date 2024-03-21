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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::VecDeque;

/// Buffer is a wrapper of `Bytes` and `VecDeque<Bytes>`.
///
/// We designed buffer to allow underlying storage to return non-contiguous bytes.
///
/// For example, http based storage like s3 could generate non-contiguous bytes by stream.
#[derive(Clone)]
pub struct Buffer(Inner);

#[derive(Clone)]
enum Inner {
    Contiguous(Bytes),
    NonContiguous(VecDeque<Bytes>),
}

impl Buffer {
    /// Create a new empty buffer.
    ///
    /// This operation is const and no allocation will be performed.
    #[inline]
    pub const fn new() -> Self {
        Self(Inner::NonContiguous(VecDeque::new()))
    }

    /// Clone internal bytes to a new `Bytes`.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        let mut bs = self.clone();
        bs.copy_to_bytes(bs.remaining())
    }

    /// Merge two buffer together without copying internal bytes.
    pub fn merge(self, buf: Buffer) -> Self {
        let mut vec = match self.0 {
            Inner::Contiguous(b) => {
                // NOTE: we will have at least two bytes in the vec.
                let mut vec = VecDeque::with_capacity(2);
                vec.push_back(b);
                vec
            }
            Inner::NonContiguous(v) => v,
        };

        match buf.0 {
            Inner::Contiguous(b) => vec.push_back(b),
            Inner::NonContiguous(bs) => {
                vec.reserve(bs.len());
                vec.extend(bs)
            }
        }

        Self(Inner::NonContiguous(vec))
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(bs: Vec<u8>) -> Self {
        Self(Inner::Contiguous(bs.into()))
    }
}

impl From<Bytes> for Buffer {
    fn from(bs: Bytes) -> Self {
        Self(Inner::Contiguous(bs))
    }
}

impl From<VecDeque<Bytes>> for Buffer {
    fn from(bs: VecDeque<Bytes>) -> Self {
        Self(Inner::NonContiguous(bs))
    }
}

impl From<Vec<Bytes>> for Buffer {
    fn from(bs: Vec<Bytes>) -> Self {
        Self(Inner::NonContiguous(bs.into()))
    }
}

impl Buf for Buffer {
    #[inline]
    fn remaining(&self) -> usize {
        match &self.0 {
            Inner::Contiguous(b) => b.remaining(),
            Inner::NonContiguous(v) => v.iter().map(|b| b.remaining()).sum(),
        }
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        match &self.0 {
            Inner::Contiguous(b) => b.chunk(),
            Inner::NonContiguous(v) => {
                if let Some(b) = v.front() {
                    b.chunk()
                } else {
                    &[]
                }
            }
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        match &mut self.0 {
            Inner::Contiguous(b) => b.advance(cnt),
            Inner::NonContiguous(v) => {
                let mut cnt = cnt;
                while cnt > 0 {
                    let b = &mut v[0];
                    if b.remaining() > cnt {
                        b.advance(cnt);
                        break;
                    } else {
                        cnt -= b.remaining();
                        v.remove(0);
                    }
                }
            }
        }
    }

    #[inline]
    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        match &mut self.0 {
            Inner::Contiguous(b) => b.copy_to_bytes(len),
            Inner::NonContiguous(v) => {
                if len > 0 && len <= v[0].remaining() {
                    let bs = v[0].copy_to_bytes(len);
                    if v[0].is_empty() {
                        v.remove(0);
                    }
                    return bs;
                }

                let mut bs = BytesMut::with_capacity(len);
                bs.put(self.take(len));
                bs.freeze()
            }
        }
    }
}
