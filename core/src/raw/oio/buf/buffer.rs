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
        size: usize,
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

    /// Get the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        match &self.0 {
            Inner::Contiguous(b) => b.remaining(),
            Inner::NonContiguous { size, .. } => *size,
        }
    }

    /// Check if buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Shortens the buffer, keeping the first `len` bytes and dropping the rest.
    ///
    /// If `len` is greater than the buffer’s current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match &mut self.0 {
            Inner::Contiguous(bs) => bs.truncate(len),
            Inner::NonContiguous { size, .. } => {
                *size = (*size).min(len);
            }
        }
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
        let size = bs.iter().map(|b| b.len()).sum();
        Self(Inner::NonContiguous {
            parts: Vec::from(bs).into(),
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl From<Vec<Bytes>> for Buffer {
    fn from(bs: Vec<Bytes>) -> Self {
        let size = bs.iter().map(|b| b.len()).sum();
        Self(Inner::NonContiguous {
            parts: bs.into(),
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl From<Arc<[Bytes]>> for Buffer {
    fn from(bs: Arc<[Bytes]>) -> Self {
        let size = bs.iter().map(|b| b.len()).sum();
        Self(Inner::NonContiguous {
            parts: bs,
            size,
            idx: 0,
            offset: 0,
        })
    }
}

impl Buf for Buffer {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        match &self.0 {
            Inner::Contiguous(b) => b.chunk(),
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                if *size == 0 {
                    return &[];
                }

                let chunk = &parts[*idx];
                let n = (chunk.len() - *offset).min(*size);
                &parts[*idx][*offset..*offset + n]
            }
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        match &mut self.0 {
            Inner::Contiguous(b) => b.advance(cnt),
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                let mut new_cnt = cnt;
                let mut new_idx = *idx;
                let mut new_offset = *offset;

                while new_cnt > 0 {
                    let remaining = parts[new_idx].len() - new_offset;
                    if new_cnt < remaining {
                        new_offset += new_cnt;
                        new_cnt = 0;
                        break;
                    } else {
                        new_cnt -= remaining;
                        new_idx += 1;
                        new_offset = 0;
                        if new_idx > parts.len() {
                            break;
                        }
                    }
                }

                if new_cnt == 0 {
                    *idx = new_idx;
                    *offset = new_offset;
                    *size -= cnt;
                } else {
                    panic!("cannot advance past {cnt} bytes")
                }
            }
        }
    }
}

/// TODO: maybe we can optimize this by avoiding not needed operations?
impl Iterator for Buffer {
    type Item = Bytes;

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.is_empty() {
            return (0, Some(0));
        }

        match &self.0 {
            Inner::Contiguous(_) => (1, Some(1)),
            Inner::NonContiguous { parts, idx, .. } => (parts.len() - idx, Some(parts.len() - idx)),
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_empty() {
            return None;
        }

        match &mut self.0 {
            Inner::Contiguous(bs) => {
                let buf = bs.split_off(0);
                Some(buf)
            }
            Inner::NonContiguous {
                parts,
                size,
                idx,
                offset,
            } => {
                let chunk = &parts[*idx];
                let n = (chunk.len() - *offset).min(*size);
                let buf = chunk.slice(*offset..*offset + n);
                *size -= n;
                *offset += n;
                if *offset == chunk.len() {
                    *idx += 1;
                    *offset = 0;
                }
                Some(buf)
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    const EMPTY_SLICE: &[u8] = &[];

    #[test]
    fn test_contiguous_buffer() {
        let mut buf = Buffer::new();

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
        assert_eq!(buf.next(), None);
    }

    #[test]
    fn test_empty_non_contiguous_buffer() {
        let mut buf = Buffer::from(vec![Bytes::new()]);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
        assert_eq!(buf.next(), None);
    }

    #[test]
    fn test_non_contiguous_buffer_with_empty_chunks() {
        let mut buf = Buffer::from(vec![Bytes::from("a")]);

        assert_eq!(buf.remaining(), 1);
        assert_eq!(buf.chunk(), b"a");

        buf.advance(1);

        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
    }

    #[test]
    fn test_non_contiguous_buffer_with_next() {
        let mut buf = Buffer::from(vec![Bytes::from("a")]);

        assert_eq!(buf.remaining(), 1);
        assert_eq!(buf.chunk(), b"a");

        let bs = buf.next();

        assert_eq!(bs, Some(Bytes::from("a")));
        assert_eq!(buf.remaining(), 0);
        assert_eq!(buf.chunk(), EMPTY_SLICE);
    }
}
