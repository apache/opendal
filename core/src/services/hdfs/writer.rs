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
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::task::{ready, Context};

use async_trait::async_trait;
use futures::{AsyncWrite, FutureExt};

use crate::raw::*;
use crate::*;

// A simple wrapper around a future that implements Future + Send + Sync
struct SyncFutureWrapper(pub BoxFuture<'static, Result<()>>);

impl Future for SyncFutureWrapper {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Delegate the polling to the inner future
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

// Explicitly mark SyncFutureWrapper as Send and Sync
unsafe impl Send for SyncFutureWrapper {}
unsafe impl Sync for SyncFutureWrapper {}

pub struct HdfsWriter<F> {
    target_path: String,
    tmp_path: Option<String>,
    f: F,
    client: Arc<hdrs::Client>,
    fut: Option<SyncFutureWrapper>,
}

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
            f,
            client,
            fut: None,
        }
    }
}

#[async_trait]
impl oio::Write for HdfsWriter<hdrs::AsyncFile> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        Pin::new(&mut self.f)
            .poll_write(cx, bs.chunk())
            .map_err(new_std_io_error)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            if let Some(mut fut) = self.fut.take() {
                let res = ready!(fut.poll_unpin(cx));
                return Poll::Ready(res);
            }

            let _ = Pin::new(&mut self.f)
                .poll_close(cx)
                .map_err(new_std_io_error);

            // Clone client to allow move into the future.
            let tmp_path = self.tmp_path.clone();
            let client = Arc::clone(&self.client);
            let target_path = self.target_path.clone();

            let fut = SyncFutureWrapper(Box::pin(async move {
                if let Some(tmp_path) = tmp_path {
                    client
                        .rename_file(&tmp_path, &target_path)
                        .map_err(new_std_io_error)?;
                }

                Ok(())
            }));

            self.fut = Some(fut);
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
        self.f.write(bs.chunk()).map_err(new_std_io_error)
    }

    fn close(&mut self) -> Result<()> {
        self.f.flush().map_err(new_std_io_error)?;

        if let Some(tmp_path) = &self.tmp_path {
            let client = Arc::as_ref(&self.client);
            client
                .rename_file(tmp_path, &self.target_path)
                .map_err(new_std_io_error)?;
        }

        Ok(())
    }
}
