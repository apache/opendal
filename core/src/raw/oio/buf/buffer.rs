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

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;

/// Buffer is a wrapper of contiguous `Bytes` and non contiguous `[Bytes]`.
///
/// We designed buffer to allow underlying storage to return non-contiguous bytes.
///
/// For example, http based storage like s3 could generate non-contiguous bytes by stream.
#[derive(Clone)]
pub struct Buffer(Inner);

#[derive(Clone)]
enum Inner {
    Contiguous(Bytes),
    NonContiguous {
        parts: Arc<[Bytes]>,
        idx: usize,
        offset: usize,
    },
}

impl Buffer {
    /// Create a new empty buffer.
    ///
    /// This operation is const and no allocation will be performed.
    #[inline]
    pub const fn new() -> Self {
        Self(Inner::Contiguous(Bytes::new()))
    }

    /// Clone internal bytes to a new `Bytes`.
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        let mut bs = self.clone();
        bs.copy_to_bytes(bs.remaining())
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

/// Transform `VecDeque<Bytes>` to `Arc<[Bytes]>`.
impl From<VecDeque<Bytes>> for Buffer {
    fn from(bs: VecDeque<Bytes>) -> Self {
        Self(Inner::NonContiguous {
            parts: Vec::from(bs).into(),
            idx: 0,
            offset: 0,
        })
    }
}

/// Transform `Vec<Bytes>` to `Arc<[Bytes]>`.
impl From<Vec<Bytes>> for Buffer {
    fn from(bs: Vec<Bytes>) -> Self {
        Self(Inner::NonContiguous {
            parts: bs.into(),
            idx: 0,
            offset: 0,
        })
    }
}

impl Buf for Buffer {
    #[inline]
    fn remaining(&self) -> usize {
        match &self.0 {
            Inner::Contiguous(b) => b.remaining(),
            Inner::NonContiguous { parts, idx, offset } => {
                parts[*idx..].iter().map(|p| p.len()).sum::<usize>() - offset
            }
        }
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        match &self.0 {
            Inner::Contiguous(b) => b.chunk(),
            Inner::NonContiguous { parts, idx, offset } => {
                if parts.is_empty() {
                    &[]
                } else {
                    &parts[*idx][*offset..]
                }
            }
        }
    }

    #[inline]
    fn advance(&mut self, mut cnt: usize) {
        match &mut self.0 {
            Inner::Contiguous(b) => b.advance(cnt),
            Inner::NonContiguous { parts, idx, offset } => {
                while cnt > 0 {
                    let remaining = parts[*idx].len() - *offset;
                    if cnt < remaining {
                        *offset += cnt;
                        return;
                    } else {
                        cnt -= remaining;
                        *idx += 1;
                        *offset = 0;
                        if *idx >= parts.len() {
                            break;
                        }
                    }
                }
            }
        }
    }
}
