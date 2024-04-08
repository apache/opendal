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

use std::io::SeekFrom;

use bytes::BytesMut;
use bytes::{Buf, BufMut};
use tokio::io::AsyncSeekExt;

use super::backend::SftpBackend;
use super::error::parse_sftp_error;
use crate::raw::*;
use crate::*;

pub struct SftpReader {
    inner: SftpBackend,
    root: String,
    path: String,
}

impl SftpReader {
    pub fn new(inner: SftpBackend, root: String, path: String) -> Self {
        Self { inner, root, path }
    }

    async fn inner_read(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        let mut size = buf.remaining_mut();
        if size == 0 {
            return Ok(9);
        }

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

        let mut bs = BytesMut::with_capacity(buf.remaining_mut());
        while size > 0 {
            let len = bs.len();
            if let Some(bytes) = f
                .read(size as u32, bs.split_off(len))
                .await
                .map_err(parse_sftp_error)?
            {
                size -= bytes.len();
                bs.unsplit(bytes);
            } else {
                break;
            }
        }
        let n = bs.remaining();
        buf.put(bs);
        Ok(n)
    }
}

impl oio::Read for SftpReader {
    /// FIXME: we should write into buf directly.
    async fn read_at(
        &self,
        mut buf: oio::WritableBuf,
        offset: u64,
    ) -> (oio::WritableBuf, Result<usize>) {
        let res = self.inner_read(&mut buf, offset).await;
        (buf, res)
    }
}
