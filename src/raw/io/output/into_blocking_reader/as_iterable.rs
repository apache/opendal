// Copyright 2023 Datafuse Labs.
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

use std::io::Result;
use std::io::SeekFrom;

use crate::raw::*;
use bytes::Bytes;
use tokio::io::ReadBuf;

/// as_iterable is used to make [`output::BlockingReader`] iterable.
pub fn as_iterable(r: output::BlockingReader, capacity: usize) -> IntoIter {
    IntoIter {
        r,
        cap: capacity,
        buf: Vec::with_capacity(capacity),
    }
}

pub struct IntoIter {
    r: output::BlockingReader,
    cap: usize,
    buf: Vec<u8>,
}

impl output::BlockingRead for IntoIter {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.r.read(buf)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.r.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        let dst = self.buf.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);
        unsafe { buf.assume_init(self.cap) };

        match self.r.read(buf.initialized_mut()) {
            Err(err) => Some(Err(err)),
            Ok(0) => None,
            Ok(n) => {
                buf.set_filled(n);
                Some(Ok(Bytes::from(buf.filled().to_vec())))
            }
        }
    }
}
