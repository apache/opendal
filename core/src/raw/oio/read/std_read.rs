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
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

/// FuturesReader implements [`oio::BlockingRead`] via [`Read`] + [`Seek`].
pub struct StdReader<R: Read + Seek> {
    inner: R,
    buf: Vec<u8>,
}

impl<R: Read + Seek> StdReader<R> {
    /// Create a new std reader.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(64 * 1024),
        }
    }
}

impl<R> oio::BlockingRead for StdReader<R>
where
    R: Read + Seek + Send + Sync,
{
    fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        // Make sure buf has enough space.
        if self.buf.capacity() < limit {
            self.buf.reserve(limit);
        }
        let buf = self.buf.spare_capacity_mut();
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `size` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(limit);
        }

        let n = self.inner.read(read_buf.initialized_mut()).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::Read)
                .with_context("source", "TokioReader")
        })?;
        read_buf.set_filled(n);

        Ok(Bytes::copy_from_slice(read_buf.filled()))
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::BlockingSeek)
                .with_context("source", "StdReader")
        })
    }
}
