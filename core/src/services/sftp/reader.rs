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

use bb8::PooledConnection;
use std::io::SeekFrom;
use std::mem;

use bytes::{Buf, BytesMut};
use openssh_sftp_client::file::File;
use tokio::io::AsyncSeekExt;

use super::backend::Manager;
use super::backend::SftpBackend;
use super::error::parse_sftp_error;
use crate::raw::*;
use crate::*;

pub struct SftpReader {
    /// Keep the connection alive while data stream is alive.
    _conn: PooledConnection<'static, Manager>,

    file: File,
    chunk: usize,
    size: usize,
    read: usize,
    buf: BytesMut,
}

impl SftpReader {
    pub fn new(conn: PooledConnection<'static, Manager>, file: File, size: usize) -> Self {
        Self {
            _conn: conn,
            file,
            size,
            chunk: 2 * 1024 * 1024,
            read: 0,
            buf: BytesMut::new(),
        }
    }
}

impl oio::Read for SftpReader {
    async fn read(&mut self) -> Result<Buffer> {
        // let client = self.inner.connect().await?;
        //
        // let mut fs = client.fs();
        // fs.set_cwd(&self.root);
        //
        // let path = fs
        //     .canonicalize(&self.path)
        //     .await
        //     .map_err(parse_sftp_error)?;
        //
        // let mut f = client
        //     .open(path.as_path())
        //     .await
        //     .map_err(parse_sftp_error)?;

        // f.seek(SeekFrom::Start(offset))
        //     .await
        //     .map_err(new_std_io_error)?;

        // let mut size = size;
        // if size == 0 {
        //     return Ok(Buffer::new());
        // }

        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        let size = (self.size - self.read).min(self.chunk);
        self.buf.reserve(size);

        let Some(bytes) = self
            .file
            .read(size as u32, self.buf.split_off(size))
            .await
            .map_err(parse_sftp_error)?
        else {
            return Err(Error::new(
                ErrorKind::RangeNotSatisfied,
                "sftp read file reaching EoF",
            ));
        };

        self.read += bytes.len();
        Ok(Buffer::from(bytes.freeze()))
    }
}
