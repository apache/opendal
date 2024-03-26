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

use super::backend::SftpBackend;
use super::error::parse_sftp_error;
use crate::raw::*;
use crate::*;
use bytes::BytesMut;
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

pub struct SftpReader {
    inner: SftpBackend,
    root: String,
    path: String,
}

impl SftpReader {
    pub fn new(inner: SftpBackend, root: String, path: String) -> Self {
        Self { inner, root, path }
    }
}

impl oio::Read for SftpReader {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let client = self.inner.connect().await?;

        let mut fs = client.fs();
        fs.set_cwd(&self.root);

        let path = fs
            .canonicalize(&self.path)
            .await
            .map_err(parse_sftp_error)?;

        let mut f = client
            .open(path.as_path())
            .await
            .map_err(parse_sftp_error)?;

        f.seek(SeekFrom::Start(offset))
            .await
            .map_err(new_std_io_error)?;

        let mut size = limit;
        if size == 0 {
            return Ok(oio::Buffer::new());
        }

        let mut buf = BytesMut::with_capacity(limit);
        while size > 0 {
            let len = buf.len();
            if let Some(bytes) = f
                .read(size as u32, buf.split_off(len))
                .await
                .map_err(parse_sftp_error)?
            {
                size -= bytes.len();
                buf.unsplit(bytes);
            } else {
                break;
            }
        }
        Ok(oio::Buffer::from(buf.freeze()))
    }
}
