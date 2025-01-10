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

use std::io::Read;
use std::sync::Arc;

use tokio::io::AsyncReadExt;
use tokio::io::ReadBuf;

use super::core::*;
use crate::raw::*;
use crate::*;

pub struct FsReader<F> {
    core: Arc<FsCore>,
    f: F,
    read: usize,
    size: usize,
    buf_size: usize,
}

impl<F> FsReader<F> {
    pub fn new(core: Arc<FsCore>, f: F, size: usize) -> Self {
        Self {
            core,
            f,
            read: 0,
            size,
            // Use 2 MiB as default value.
            buf_size: 2 * 1024 * 1024,
        }
    }
}

impl oio::Read for FsReader<tokio::fs::File> {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        let mut bs = self.core.buf_pool.get();
        bs.reserve(self.buf_size);

        let size = (self.size - self.read).min(self.buf_size);
        let buf = &mut bs.spare_capacity_mut()[..size];
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `limit` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        let n = self
            .f
            .read_buf(&mut read_buf)
            .await
            .map_err(new_std_io_error)?;
        self.read += n;

        // Safety: We make sure that bs contains `n` more bytes.
        let filled = read_buf.filled().len();
        unsafe { bs.set_len(filled) }

        let frozen = bs.split().freeze();
        // Return the buffer to the pool.
        self.core.buf_pool.put(bs);

        Ok(Buffer::from(frozen))
    }
}

impl oio::BlockingRead for FsReader<std::fs::File> {
    fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        let mut bs = self.core.buf_pool.get();
        bs.reserve(self.buf_size);

        let size = (self.size - self.read).min(self.buf_size);
        let buf = &mut bs.spare_capacity_mut()[..size];
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `limit` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        let n = self
            .f
            .read(read_buf.initialize_unfilled())
            .map_err(new_std_io_error)?;
        read_buf.advance(n);
        self.read += n;

        // Safety: We make sure that bs contains `n` more bytes.
        let filled = read_buf.filled().len();
        unsafe { bs.set_len(filled) }

        let frozen = bs.split().freeze();
        // Return the buffer to the pool.
        self.core.buf_pool.put(bs);

        Ok(Buffer::from(frozen))
    }
}
