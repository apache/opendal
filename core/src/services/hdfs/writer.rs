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

use std::io::Write;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::AsyncWrite;

use crate::raw::*;
use crate::*;

pub struct HdfsWriter<F> {
    f: F,
}

impl<F> HdfsWriter<F> {
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

#[async_trait]
impl oio::Write for HdfsWriter<hdrs::AsyncFile> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        Pin::new(&mut self.f)
            .poll_write(cx, bs.chunk())
            .map_err(new_std_io_error)
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "HdfsWriter doesn't support abort",
        )))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.f)
            .poll_close(cx)
            .map_err(new_std_io_error)
    }
}

/// adding a dummy commit
impl oio::BlockingWrite for HdfsWriter<hdrs::File> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        self.f.write(bs.chunk()).map_err(new_std_io_error)
    }

    fn close(&mut self) -> Result<()> {
        self.f.flush().map_err(new_std_io_error)?;

        Ok(())
    }
}
