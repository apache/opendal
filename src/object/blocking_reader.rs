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

use std::io;
use std::io::SeekFrom;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::error::Error;
use crate::error::Result;
use crate::ops::OpRead;
use crate::raw::*;
use crate::ErrorKind;
use crate::ObjectMetadata;

/// BlockingObjectReader is the public API for users.
pub struct BlockingObjectReader {
    pub(crate) inner: output::BlockingReader,
}

impl BlockingObjectReader {
    /// Create a new blocking object reader.
    ///
    /// Create will use internal information to decide the most suitable
    /// implementaion for users.
    ///
    /// We don't want to expose those detials to users so keep this fuction
    /// in crate only.
    pub(crate) fn create(
        acc: FusedAccessor,
        path: &str,
        _meta: Arc<Mutex<ObjectMetadata>>,
        op: OpRead,
    ) -> Result<Self> {
        let acc_meta = acc.metadata();

        let r = if acc_meta.hints().contains(AccessorHint::ReadIsSeekable) {
            let (_, r) = acc.blocking_read(path, op)?;
            r
        } else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "non seekable blocking reader is not supported",
            ));
        };

        let r = if acc_meta.hints().contains(AccessorHint::ReadIsStreamable) {
            r
        } else {
            // Make this capacity configurable.
            Box::new(output::into_streamable_reader(r, 256 * 1024))
        };

        Ok(BlockingObjectReader { inner: r })
    }
}

impl output::BlockingRead for BlockingObjectReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }

    #[inline]
    fn next(&mut self) -> Option<io::Result<Bytes>> {
        output::BlockingRead::next(&mut self.inner)
    }
}

impl io::Read for BlockingObjectReader {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl io::Seek for BlockingObjectReader {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Iterator for BlockingObjectReader {
    type Item = io::Result<Bytes>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
