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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;

use bytes::Bytes;

/// BlockingReader is a boxed dyn `BlockingRead`.
pub type BlockingReader = Box<dyn BlockingRead>;

/// Read is the trait that OpenDAL returns to callers.
///
/// Read is compose of the following trait
///
/// - `Read`
/// - `Seek`
/// - `Iterator<Item = Result<Bytes>>`
///
/// `Read` is required to be implemented, `Seek` and `Iterator`
/// is optional. We use `Read` to make users life easier.
pub trait BlockingRead: Send + Sync + 'static {
    /// Return the inner output bytes reader if there is one.
    #[inline]
    fn inner(&mut self) -> Option<&mut BlockingReader> {
        None
    }

    /// Read synchronously.
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self.inner() {
            Some(v) => v.read(buf),
            None => unimplemented!("read is required to be implemented for output::BlockingRead"),
        }
    }

    /// Seek synchronously.
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self.inner() {
            Some(v) => v.seek(pos),
            None => Err(Error::new(
                ErrorKind::Unsupported,
                "output blocking reader doesn't support seeking",
            )),
        }
    }

    /// Iterating [`Bytes`] from underlying reader.
    #[inline]
    fn next(&mut self) -> Option<Result<Bytes>> {
        match self.inner() {
            Some(v) => v.next(),
            None => Some(Err(Error::new(
                ErrorKind::Unsupported,
                "output reader doesn't support iterating",
            ))),
        }
    }
}

impl std::io::Read for dyn BlockingRead {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let this: &mut dyn BlockingRead = &mut *self;
        this.read(buf)
    }
}

impl std::io::Seek for dyn BlockingRead {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let this: &mut dyn BlockingRead = &mut *self;
        this.seek(pos)
    }
}

impl Iterator for dyn BlockingRead {
    type Item = Result<Bytes>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let this: &mut dyn BlockingRead = &mut *self;
        this.next()
    }
}
