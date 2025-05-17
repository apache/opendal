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
use bytes::BytesMut;
use openssh_sftp_client::file::File;

use super::core::Manager;
use super::error::parse_sftp_error;
use crate::raw::*;
use crate::*;

pub struct SftpReader {
    /// Keep the connection alive while data stream is alive.
    _conn: PooledConnection<'static, Manager>,

    file: File,
    chunk: usize,
    size: Option<usize>,
    read: usize,
    buf: BytesMut,
}

impl SftpReader {
    pub fn new(conn: PooledConnection<'static, Manager>, file: File, size: Option<u64>) -> Self {
        Self {
            _conn: conn,
            file,
            size: size.map(|v| v as usize),
            chunk: 2 * 1024 * 1024,
            read: 0,
            buf: BytesMut::new(),
        }
    }
}

impl oio::Read for SftpReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size.unwrap_or(usize::MAX) {
            return Ok(Buffer::new());
        }

        let size = if let Some(size) = self.size {
            (size - self.read).min(self.chunk)
        } else {
            self.chunk
        };
        self.buf.reserve(size);

        let Some(bytes) = self
            .file
            .read(size as u32, self.buf.split_off(0))
            .await
            .map_err(parse_sftp_error)?
        else {
            return Ok(Buffer::new());
        };

        self.read += bytes.len();
        self.buf = bytes;
        let bs = self.buf.split();
        Ok(Buffer::from(bs.freeze()))
    }
}
