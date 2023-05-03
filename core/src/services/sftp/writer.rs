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
use openssh::Stdio;
use openssh_sftp_client::Sftp;

use super::backend::{Connection, SftpBackend};
use crate::raw::{get_basename, get_parent, oio};
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
        let session = self.backend.connect_inner().await?;

        let mut child = session
            .subsystem("sftp")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .await?;

        let sftp = Sftp::new(
            child.stdin().take().unwrap(),
            child.stdout().take().unwrap(),
            Default::default(),
        )
        .await?;

        let mut file = sftp.create("test").await?;

        file.write_all(&bs).await?;
        file.close().await?;

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
