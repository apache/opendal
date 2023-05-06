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

use std::io::Write;

use async_trait::async_trait;
use bytes::Bytes;
use futures::AsyncWriteExt;

use super::error::parse_io_error;
use crate::raw::*;
use crate::*;

pub struct HdfsWriter<F> {
    f: F,
    /// The position of current written bytes in the buffer.
    ///
    /// We will maintain the posstion in pos to make sure the buffer is written correctly.
    pos: usize,
}

impl<F> HdfsWriter<F> {
    pub fn new(f: F) -> Self {
        Self { f, pos: 0 }
    }
}

#[async_trait]
impl oio::Write for HdfsWriter<hdrs::AsyncFile> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        while self.pos < bs.len() {
            let n = self
                .f
                .write(&bs[self.pos..])
                .await
                .map_err(parse_io_error)?;
            self.pos += n;
        }
        // Reset pos to 0 for next write.
        self.pos = 0;

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support abort",
        ))
    }

    async fn close(&mut self) -> Result<()> {
        self.f.close().await.map_err(parse_io_error)?;

        Ok(())
    }
}

impl oio::BlockingWrite for HdfsWriter<hdrs::File> {
    fn write(&mut self, bs: Bytes) -> Result<()> {
        while self.pos < bs.len() {
            let n = self.f.write(&bs[self.pos..]).map_err(parse_io_error)?;
            self.pos += n;
        }
        // Reset pos to 0 for next write.
        self.pos = 0;

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.f.flush().map_err(parse_io_error)?;

        Ok(())
    }
}
