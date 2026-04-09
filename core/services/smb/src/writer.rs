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

use bytes::Buf;
use fastpool::bounded;
use smb::{File, WriteAt};

use super::core::Manager;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SmbWriter {
    _client: bounded::Object<Manager>,
    file: Option<File>,
    chunk: usize,
    offset: u64,
}

impl SmbWriter {
    pub fn new(file: File, client: bounded::Object<Manager>) -> Self {
        Self {
            _client: client,
            file: Some(file),
            chunk: 64 * 1024,
            offset: 0,
        }
    }
}

impl oio::Write for SmbWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        let file = self
            .file
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "smb writer is already closed"))?;

        while bs.has_remaining() {
            let chunk_len = bs.chunk().len().min(self.chunk);
            let written = self
                .file
                .as_ref()
                .expect("checked above")
                .write_at(&bs.chunk()[..chunk_len], self.offset)
                .await
                .map_err(super::error::parse_smb_error)?;
            self.offset += written as u64;
            bs.advance(written);
        }

        let _ = file;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if let Some(file) = self.file.take() {
            file.flush().await.map_err(new_std_io_error)?;
            file.close().await.map_err(super::error::parse_smb_error)?;
        }
        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            file.close().await.map_err(super::error::parse_smb_error)?;
        }
        Err(Error::new(
            ErrorKind::Unsupported,
            "SmbWriter doesn't support abort",
        ))
    }
}
