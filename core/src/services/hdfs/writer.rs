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

use futures::future::BoxFuture;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::{ready, Context};

use async_trait::async_trait;
use futures::{AsyncWrite, AsyncWriteExt, FutureExt};

use crate::raw::*;
use crate::*;

pub struct HdfsWriter<F> {
    target_path: String,
    tmp_path: Option<String>,
    f: Option<F>,
    client: Arc<hdrs::Client>,
    fut: Option<BoxFuture<'static, Result<()>>>,
}

/// # Safety
///
/// We will only take `&mut Self` reference for HdfsWriter.
unsafe impl<F> Sync for HdfsWriter<F> {}

impl<F> HdfsWriter<F> {
    pub fn new(
        target_path: String,
        tmp_path: Option<String>,
        f: F,
        client: Arc<hdrs::Client>,
    ) -> Self {
        Self {
            target_path,
            tmp_path,
            f: Some(f),
            client,
            fut: None,
        }
    }
}

#[async_trait]
impl oio::Write for HdfsWriter<hdrs::AsyncFile> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        let f = self.f.as_mut().expect("HdfsWriter must be initialized");

        Pin::new(f)
            .poll_write(cx, bs.chunk())
            .map_err(new_std_io_error)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            #[allow(unused_mut)]
            if let Some(mut fut) = self.fut.as_mut() {
                let res = ready!(fut.poll_unpin(cx));
                self.fut = None;
                return Poll::Ready(res);
            }

            let mut f = self.f.take().expect("HdfsWriter must be initialized");
            let tmp_path = self.tmp_path.clone();
            let target_path = self.target_path.clone();
            // Clone client to allow move into the future.
            let client = self.client.clone();

            self.fut = Some(Box::pin(async move {
                f.close().await.map_err(new_std_io_error)?;

                if let Some(tmp_path) = tmp_path {
                    client
                        .rename_file(&tmp_path, &target_path)
                        .map_err(new_std_io_error)?;
                }

                Ok(())
            }));
        }
    }

    fn poll_abort(&mut self, _: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "HdfsWriter doesn't support abort",
        )))
    }
}

impl oio::BlockingWrite for HdfsWriter<hdrs::File> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let f = self.f.as_mut().expect("HdfsWriter must be initialized");
        f.write(bs.chunk()).map_err(new_std_io_error)
    }

    fn close(&mut self) -> Result<()> {
        let f = self.f.as_mut().expect("HdfsWriter must be initialized");
        f.flush().map_err(new_std_io_error)?;

        if let Some(tmp_path) = &self.tmp_path {
            let client = Arc::as_ref(&self.client);
            client
                .rename_file(tmp_path, &self.target_path)
                .map_err(new_std_io_error)?;
        }

        Ok(())
    }
}
