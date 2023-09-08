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
use futures::AsyncWrite;
use openssh_sftp_client::file::File;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::raw::oio;
use crate::*;

pub struct SftpWriter {
    file: File,
}

impl SftpWriter {
    pub fn new(file: File) -> Self {
        SftpWriter { file }
    }
}

#[async_trait]
impl oio::Write for SftpWriter {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        Pin::new(&mut self.file)
            .poll_write(cx, bs.chunk())
            .map_err(Error::from)
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx).map_err(Error::from)
    }
}
