// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::SeekFrom;
use std::task::Context;
use std::task::Poll;

use super::output;
use bytes::Bytes;

/// BytesCursor is the cursor for [`Bytes`] that implements `AsyncRead`
/// and `BytesStream`
pub struct BytesCursor {
    inner: Bytes,
    pos: u64,
}

impl BytesCursor {
    /// Returns `true` if the remaining slice is empty.
    pub fn is_empty(&self) -> bool {
        self.pos as usize >= self.inner.len()
    }

    /// Returns the remaining slice.
    pub fn remaining_slice(&self) -> &[u8] {
        let len = self.pos.min(self.inner.len() as u64) as usize;
        &self.inner.as_ref()[len..]
    }
}

impl From<Vec<u8>> for BytesCursor {
    fn from(v: Vec<u8>) -> Self {
        BytesCursor {
            inner: Bytes::from(v),
            pos: 0,
        }
    }
}

impl output::Read for BytesCursor {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize>> {
        let n = Read::read(&mut self.remaining_slice(), buf)?;
        self.pos += n as u64;
        Poll::Ready(Ok(n))
    }

    fn poll_seek(&mut self, _: &mut Context<'_>, pos: SeekFrom) -> Poll<Result<u64>> {
        let (base, amt) = match pos {
            SeekFrom::Start(n) => (0, n as i64),
            SeekFrom::End(n) => (self.inner.len() as i64, n),
            SeekFrom::Current(n) => (self.pos as i64, n),
        };

        let n = match base.checked_add(amt) {
            Some(n) if n >= 0 => n as u64,
            _ => {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid seek to a negative or overflowing position",
                )))
            }
        };
        self.pos = n;
        Poll::Ready(Ok(n))
    }

    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Option<Result<Bytes>>> {
        if self.is_empty() {
            Poll::Ready(None)
        } else {
            let bs = self.inner.split_off(self.pos as usize);
            self.pos += bs.len() as u64;
            Poll::Ready(Some(Ok(bs)))
        }
    }
}
