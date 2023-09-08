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
use futures::future::BoxFuture;
use futures::AsyncWriteExt;
use std::task::{ready, Context, Poll};

use super::backend::FtpBackend;
use crate::raw::*;
use crate::*;

pub struct FtpWriter {
    backend: FtpBackend,
    path: String,

    fut: Option<BoxFuture<'static, Result<usize>>>,
}

/// # TODO
///
/// Writer is not implemented correctly.
///
/// After we can use data stream, we should return it directly.
impl FtpWriter {
    pub fn new(backend: FtpBackend, path: String) -> Self {
        FtpWriter {
            backend,
            path,
            fut: None,
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FtpWriter.
unsafe impl Sync for FtpWriter {}

#[async_trait]
impl oio::Write for FtpWriter {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        loop {
            if let Some(fut) = self.fut.as_mut() {
                let res = ready!(fut.poll_unpin(cx));
                self.fut = None;
                return Poll::Ready(res);
            }

            let size = bs.remaining();
            let bs = bs.copy_to_bytes(size);

            let path = self.path.clone();
            let backend = self.backend.clone();
            let fut = async {
                let mut ftp_stream = backend.ftp_connect(Operation::Write).await?;
                let mut data_stream = ftp_stream.append_with_stream(&path).await?;
                data_stream.write_all(&bs).await.map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "copy from ftp stream").set_source(err)
                })?;

                ftp_stream.finalize_put_stream(data_stream).await?;
                Ok(size)
            };
            self.fut = Some(Box::pin(fut));
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
