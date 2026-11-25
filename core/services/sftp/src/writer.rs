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

use bytes::Bytes;
use openssh_sftp_client::file::File;

use super::core::SftpConnection;
use super::core::parse_sftp_error;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SftpWriter {
    file: Option<File>,
    // Keep the connection checked out until after the file handle closes.
    _conn: Option<SftpConnection>,
}

impl SftpWriter {
    pub fn new(file: File, conn: SftpConnection) -> Self {
        SftpWriter {
            file: Some(file),
            _conn: Some(conn),
        }
    }
}

impl oio::Write for SftpWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let file = self.file.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "sftp writer has already been closed")
        })?;

        let mut chunks: Vec<Bytes> = bs.into_iter().collect();
        file.write_all_zero_copy(&mut chunks)
            .await
            .map_err(parse_sftp_error)?;

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if let Some(file) = self.file.take() {
            let res = file.close().await.map_err(parse_sftp_error);
            self._conn = None;
            res?;
        }

        Ok(MetadataBuilder::unknown().build())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "SftpWriter doesn't support abort",
        ))
    }
}
