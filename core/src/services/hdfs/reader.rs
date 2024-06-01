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

use hdrs::File;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

pub struct HdfsReader {
    f: Arc<File>,
}

impl HdfsReader {
    pub fn new(f: File) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl oio::Read for HdfsReader {
    async fn read(&mut self) -> Result<Buffer> {
        let r = Self { f: self.f.clone() };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn_blocking(move || oio::BlockingRead::read_at(&r, offset, size))
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

impl oio::BlockingRead for HdfsReader {
    fn read(&mut self) -> Result<Buffer> {
        let mut bs = Vec::with_capacity(size);

        let buf = bs.spare_capacity_mut();
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `size` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        loop {
            // If the buffer is full, we are done.
            if read_buf.initialize_unfilled().is_empty() {
                break;
            }
            let n = self
                .f
                .read_at(read_buf.initialize_unfilled(), offset)
                .map_err(new_std_io_error)?;
            if n == 0 {
                break;
            }
            read_buf.advance(n);
            offset += n as u64;
        }

        // Safety: We make sure that bs contains `n` more bytes.
        let filled = read_buf.filled().len();
        unsafe { bs.set_len(filled) }
        Ok(Buffer::from(bs))
    }
}
