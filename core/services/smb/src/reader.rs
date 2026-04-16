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

use bytes::BytesMut;
use fastpool::bounded;
use smb::{File, ReadAt};

use super::core::Manager;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SmbReader {
    _client: bounded::Object<Manager>,
    file: Option<File>,
    chunk: usize,
    offset: u64,
    remaining: Option<u64>,
    buf: BytesMut,
}

impl SmbReader {
    pub fn new(client: bounded::Object<Manager>, file: File, range: BytesRange) -> Self {
        Self {
            _client: client,
            file: Some(file),
            chunk: 64 * 1024,
            offset: range.offset(),
            remaining: range.size(),
            buf: BytesMut::new(),
        }
    }

    async fn close_file(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            file.close().await.map_err(super::error::parse_smb_error)?;
        }
        Ok(())
    }
}

impl oio::Read for SmbReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.remaining == Some(0) {
            self.close_file().await?;
            return Ok(Buffer::new());
        }

        let size = self
            .remaining
            .map(|size| size.min(self.chunk as u64) as usize)
            .unwrap_or(self.chunk);
        self.buf.resize(size, 0);

        let file = self
            .file
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "smb reader is already closed"))?;
        let read = match file.read_at(&mut self.buf[..], self.offset).await {
            Ok(read) => read,
            Err(err) => {
                let _ = self.close_file().await;
                return Err(super::error::parse_smb_error(err));
            }
        };
        if read == 0 {
            self.close_file().await?;
            return Ok(Buffer::new());
        }

        self.offset += read as u64;
        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining.saturating_sub(read as u64);
            if *remaining == 0 {
                self.close_file().await?;
            }
        }

        Ok(Buffer::from(self.buf.split_to(read).freeze()))
    }
}
