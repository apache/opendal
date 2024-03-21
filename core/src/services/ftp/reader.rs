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

use super::backend::FtpBackend;
use super::err::parse_error;
use crate::raw::{new_std_io_error, oio, OpRead, Operation};
use crate::*;
use futures::AsyncReadExt;

pub struct FtpReader {
    core: FtpBackend,

    path: String,
    op: OpRead,
}

impl FtpReader {
    pub fn new(core: FtpBackend, path: &str, op: OpRead) -> Self {
        FtpReader {
            core,
            path: path.to_string(),
            op: op,
        }
    }
}

impl oio::Read for FtpReader {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let Some(range) = self.op.range().apply_on_offset(offset, limit) else {
            return Ok(oio::Buffer::new());
        };

        let mut ftp_stream = self.core.ftp_connect(Operation::Read).await?;

        let _meta = self.core.ftp_stat(&self.path).await?;

        match (range.offset(), range.size()) {
            (Some(offset), Some(size)) => {
                ftp_stream
                    .resume_transfer(offset as usize)
                    .await
                    .map_err(parse_error)?;
                let mut ds = ftp_stream
                    .retr_as_stream(&self.path)
                    .await
                    .map_err(parse_error)?
                    .take(size);
                let mut bs = Vec::with_capacity(size as usize);
                ds.read_to_end(&mut bs).await.map_err(new_std_io_error)?;
                Ok(oio::Buffer::from(bs))
            }
            (Some(offset), None) => {
                ftp_stream
                    .resume_transfer(offset as usize)
                    .await
                    .map_err(parse_error)?;
                let mut ds = ftp_stream
                    .retr_as_stream(&self.path)
                    .await
                    .map_err(parse_error)?;
                let mut bs = vec![];
                ds.read_to_end(&mut bs).await.map_err(new_std_io_error)?;
                Ok(oio::Buffer::from(bs))
            }
            _ => unimplemented!(),
        }
    }
}
