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

use async_trait::async_trait;
use bytes::Bytes;

use super::backend::SftpBackend;
use crate::raw::oio;
use crate::{Error, ErrorKind, Result};

pub struct SftpWriter {
    backend: SftpBackend,
    path: String,
}

impl SftpWriter {
    pub fn new(backend: SftpBackend, path: String) -> Self {
        SftpWriter { backend, path }
    }
}

#[async_trait]
impl oio::Write for SftpWriter {
    /// TODO
    ///
    /// this implementation is wrong.
    ///
    /// We should hold the file until users call `close`.
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let conn = self.backend.connect().await?;
        let mut file = conn.sftp.create(&self.path).await?;

        file.write_all(&bs).await?;
        file.close().await?;

        let sftp = conn.into_sftp();
        sftp.close().await?;

        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "SFTP does not support aborting writes",
        ))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
