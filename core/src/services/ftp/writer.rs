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
use futures::AsyncWriteExt;

use super::backend::FtpBackend;

use crate::raw::*;
use crate::services::ftp::err::parse_error;
use crate::*;

pub type FtpWriters = oio::OneShotWriter<FtpWriter>;

pub struct FtpWriter {
    backend: FtpBackend,
    path: String,
}

/// # TODO
///
/// Writer is not implemented correctly.
///
/// After we can use data stream, we should return it directly.
impl FtpWriter {
    pub fn new(backend: FtpBackend, path: String) -> Self {
        FtpWriter { backend, path }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FtpWriter.
unsafe impl Sync for FtpWriter {}

#[async_trait]
impl oio::OneShotWrite for FtpWriter {
    async fn write_once(&self, bs: Bytes) -> Result<()> {
        let mut ftp_stream = self.backend.ftp_connect(Operation::Write).await?;
        let mut data_stream = ftp_stream
            .append_with_stream(&self.path)
            .await
            .map_err(parse_error)?;
        data_stream.write_all(&bs).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "copy from ftp stream").set_source(err)
        })?;

        ftp_stream
            .finalize_put_stream(data_stream)
            .await
            .map_err(parse_error)?;
        Ok(())
    }
}
