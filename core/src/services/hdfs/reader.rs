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

use bytes::BytesMut;
use futures::AsyncReadExt;
use hdrs::AsyncFile;
use hdrs::File;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

pub struct HdfsReader<F> {
    f: F,
    read: usize,
    size: usize,
    buf_size: usize,
    buf: BytesMut,
}

impl<F> HdfsReader<F> {
    pub fn new(f: F, size: usize) -> Self {
        Self {
            f,
            read: 0,
            size,
            // Use 2 MiB as default value.
            buf_size: 2 * 1024 * 1024,
            buf: BytesMut::new(),
        }
    }
}

impl oio::Read for HdfsReader<AsyncFile> {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        let size = (self.size - self.read).min(self.buf_size);
        self.buf.reserve(size);

        let buf = &mut self.buf.spare_capacity_mut()[..size];
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `limit` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        let n = self
            .f
            .read(read_buf.initialize_unfilled())
            .await
            .map_err(new_std_io_error)?;
        read_buf.advance(n);
        self.read += n;

        // Safety: We make sure that bs contains `n` more bytes.
        let filled = read_buf.filled().len();
        unsafe { self.buf.set_len(filled) }

        let frozen = self.buf.split().freeze();
        Ok(Buffer::from(frozen))
    }
}

impl oio::BlockingRead for HdfsReader<File> {
    fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        let size = (self.size - self.read).min(self.buf_size);
        self.buf.reserve(size);

        let buf = &mut self.buf.spare_capacity_mut()[..size];
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
        unsafe { self.buf.set_len(filled) }

        let frozen = self.buf.split().freeze();
        Ok(Buffer::from(frozen))
    }
}
