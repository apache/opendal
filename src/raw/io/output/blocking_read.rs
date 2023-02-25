// Copyright 2022 Datafuse Labs
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
    /// Read synchronously.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Seek synchronously.
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;

    /// Iterating [`Bytes`] from underlying reader.
    fn next(&mut self) -> Option<Result<Bytes>>;
}

impl BlockingRead for () {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _ = buf;

        unimplemented!("read is required to be implemented for output::BlockingRead")
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let _ = pos;

        Err(Error::new(
            ErrorKind::Unsupported,
            "output blocking reader doesn't support seeking",
        ))
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support iterating",
        )))
    }
}

/// `Box<dyn BlockingRead>` won't implement `BlockingRead` automanticly.
/// To make BlockingReader work as expected, we must add this impl.
impl<T: BlockingRead + ?Sized> BlockingRead for Box<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        (**self).next()
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
