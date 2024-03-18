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

use std::cmp::min;
use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;

use crate::raw::*;
use crate::*;

/// Cursor is the cursor for [`Bytes`] that implements [`oio::Read`]
#[derive(Default)]
pub struct Cursor {
    inner: Bytes,
    pos: u64,
}

impl Cursor {
    /// Create a new empty cursor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if the remaining slice is empty.
    pub fn is_empty(&self) -> bool {
        self.pos as usize >= self.inner.len()
    }

    /// Returns the remaining slice.
    pub fn remaining_slice(&self) -> &[u8] {
        let len = self.pos.min(self.inner.len() as u64) as usize;
        &self.inner.as_ref()[len..]
    }

    /// Return the length of remaining slice.
    pub fn len(&self) -> usize {
        self.inner.len() - self.pos as usize
    }
}

impl From<Bytes> for Cursor {
    fn from(v: Bytes) -> Self {
        Cursor { inner: v, pos: 0 }
    }
}

impl From<Vec<u8>> for Cursor {
    fn from(v: Vec<u8>) -> Self {
        Cursor {
            inner: Bytes::from(v),
            pos: 0,
        }
    }
}

// impl oio::Read for Cursor {
//     async fn read(&mut self, limit: usize) -> Result<Bytes> {
//         if self.is_empty() {
//             Ok(Bytes::new())
//         } else {
//             // The clone here is required as we don't want to change it.
//             let mut bs = self.inner.clone().split_off(self.pos as usize);
//             let bs = bs.split_to(min(bs.len(), limit));
//             self.pos += bs.len() as u64;
//             Ok(bs)
//         }
//     }
//
//     async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
//         let (base, amt) = match pos {
//             SeekFrom::Start(n) => (0, n as i64),
//             SeekFrom::End(n) => (self.inner.len() as i64, n),
//             SeekFrom::Current(n) => (self.pos as i64, n),
//         };
//
//         let n = match base.checked_add(amt) {
//             Some(n) if n >= 0 => n as u64,
//             _ => {
//                 return Err(Error::new(
//                     ErrorKind::InvalidInput,
//                     "invalid seek to a negative or overflowing position",
//                 ))
//             }
//         };
//         self.pos = n;
//         Ok(n)
//     }
// }

impl oio::BlockingRead for Cursor {
    fn read(&mut self, limit: usize) -> Result<Bytes> {
        if self.is_empty() {
            Ok(Bytes::new())
        } else {
            // The clone here is required as we don't want to change it.
            let mut bs = self.inner.clone().split_off(self.pos as usize);
            let bs = bs.split_to(min(bs.len(), limit));
            self.pos += bs.len() as u64;
            Ok(bs)
        }
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::End(n) => (self.inner.len() as i64, n),
            SeekFrom::Current(n) => (self.pos as i64, n),
        };

        let n = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                ))
            }
        };
        self.pos = n;
        Ok(n)
    }
}

impl oio::Stream for Cursor {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.is_empty() {
            return Poll::Ready(None);
        }

        let bs = self.inner.clone();
        self.pos += bs.len() as u64;
        Poll::Ready(Some(Ok(bs)))
    }
}
