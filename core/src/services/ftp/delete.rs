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

use super::backend::FtpBackend;
use super::err::parse_error;
use crate::raw::*;
use crate::*;
use std::sync::Arc;
use suppaftp::types::Response;
use suppaftp::FtpError;
use suppaftp::Status;

pub struct FtpDeleter {
    core: Arc<FtpBackend>,
}

impl FtpDeleter {
    pub fn new(core: Arc<FtpBackend>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for FtpDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let mut ftp_stream = self.core.ftp_connect(Operation::Delete).await?;

        let result = if path.ends_with('/') {
            ftp_stream.rmdir(&path).await
        } else {
            ftp_stream.rm(&path).await
        };

        match result {
            Err(FtpError::UnexpectedResponse(Response {
                status: Status::FileUnavailable,
                ..
            }))
            | Ok(_) => (),
            Err(e) => {
                return Err(parse_error(e));
            }
        }

        Ok(())
    }
}
