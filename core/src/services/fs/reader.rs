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

use std::sync::Arc;
use tokio::io::ReadBuf;

use super::core::*;
use crate::raw::*;
use crate::*;

pub struct FsReader {
    core: Arc<FsCore>,
    f: std::fs::File,
}

impl FsReader {
    pub fn new(core: Arc<FsCore>, f: std::fs::File) -> Self {
        Self { core, f }
    }

    fn try_clone(&self) -> Result<Self> {
        let f = self.f.try_clone().map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "tokio fs clone file description failed",
            )
            .set_source(err)
        })?;

        Ok(Self {
            core: self.core.clone(),
            f,
        })
    }

    #[cfg(target_family = "unix")]
    pub fn read_at_inner(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        self.f.read_at(buf, offset).map_err(new_std_io_error)
    }

    #[cfg(target_family = "windows")]
    pub fn read_at_inner(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::windows::fs::FileExt;
        self.f.seek_read(buf, offset).map_err(new_std_io_error)
    }
}

impl oio::Read for FsReader {
    async fn read(&mut self) -> Result<Buffer> {
        let handle = self.try_clone()?;

        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn_blocking(move || oio::BlockingRead::read_at(&handle, offset, size))
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "tokio spawn io task failed").set_source(err)
                })?,
            Err(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "no tokio runtime found, failed to run io task",
            )),
        }
    }
}

impl oio::BlockingRead for FsReader {
    fn read_at(&self, mut offset: u64, size: usize) -> Result<Buffer> {
        let mut bs = self.core.buf_pool.get();
        bs.reserve(size);

        let buf = &mut bs.spare_capacity_mut()[..size];
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `limit` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        loop {
            // If the buffer is full, we are done.
            if read_buf.initialize_unfilled().is_empty() {
                break;
            }
            let n = self.read_at_inner(read_buf.initialize_unfilled(), offset)?;
            if n == 0 {
                break;
            }
            read_buf.advance(n);
            offset += n as u64;
        }

        // Safety: We make sure that bs contains `n` more bytes.
        let filled = read_buf.filled().len();
        unsafe { bs.set_len(filled) }

        let frozen = bs.split().freeze();
        // Return the buffer to the pool.
        self.core.buf_pool.put(bs);

        Ok(Buffer::from(frozen))
    }
}
