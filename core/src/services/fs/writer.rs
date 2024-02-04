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
use std::path::PathBuf;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::raw::*;
use crate::*;

pub struct FsWriter<F> {
    target_path: PathBuf,
    tmp_path: Option<PathBuf>,

    f: Option<F>,
    fut: Option<BoxFuture<'static, (F, Result<()>)>>,
}

impl<F> FsWriter<F> {
    pub fn new(target_path: PathBuf, tmp_path: Option<PathBuf>, f: F) -> Self {
        Self {
            target_path,
            tmp_path,

            f: Some(f),
            fut: None,
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsWriter.
unsafe impl<F> Sync for FsWriter<F> {}

impl oio::Write for FsWriter<tokio::fs::File> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        let f = self.f.as_mut().expect("FsWriter must be initialized");

        Pin::new(f)
            .poll_write_vectored(cx, &bs.vectored_chunk())
            .map_err(new_std_io_error)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            if let Some(fut) = self.fut.as_mut() {
                let res = ready!(fut.poll_unpin(cx));
                self.fut = None;
                if let Err(e) = res.1 {
                    self.f = Some(res.0);
                    return Poll::Ready(Err(e));
                }
                return Poll::Ready(Ok(()));
            }

            let mut f = self.f.take().expect("FsWriter must be initialized");
            let tmp_path = self.tmp_path.clone();
            let target_path = self.target_path.clone();
            self.fut = Some(Box::pin(async move {
                if let Err(e) = f.flush().await.map_err(new_std_io_error) {
                    // Reserve the original error for retry.
                    return (f, Err(e));
                }
                if let Err(e) = f.sync_all().await.map_err(new_std_io_error) {
                    return (f, Err(e));
                }

                if let Some(tmp_path) = &tmp_path {
                    if let Err(e) = tokio::fs::rename(tmp_path, &target_path)
                        .await
                        .map_err(new_std_io_error)
                    {
                        return (f, Err(e));
                    }
                }

                (f, Ok(()))
            }));
        }
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            if let Some(fut) = self.fut.as_mut() {
                let res = ready!(fut.poll_unpin(cx));
                self.fut = None;
                if let Err(e) = res.1 {
                    self.f = Some(res.0);
                    return Poll::Ready(Err(e));
                }
                return Poll::Ready(Ok(()));
            }

            let f = self.f.take().expect("FsWriter must be initialized");
            let tmp_path = self.tmp_path.clone();
            self.fut = Some(Box::pin(async move {
                if let Some(tmp_path) = &tmp_path {
                    if let Err(e) = tokio::fs::remove_file(tmp_path)
                        .await
                        .map_err(new_std_io_error)
                    {
                        return (f, Err(e));
                    }
                    (f, Ok(()))
                } else {
                    (
                        f,
                        Err(Error::new(
                            ErrorKind::Unsupported,
                            "Fs doesn't support abort if atomic_write_dir is not set",
                        )),
                    )
                }
            }));
        }
    }
}

impl oio::BlockingWrite for FsWriter<std::fs::File> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let f = self.f.as_mut().expect("FsWriter must be initialized");

        f.write_vectored(&bs.vectored_chunk())
            .map_err(new_std_io_error)
    }

    fn close(&mut self) -> Result<()> {
        if let Some(f) = self.f.take() {
            f.sync_all().map_err(new_std_io_error)?;

            if let Some(tmp_path) = &self.tmp_path {
                std::fs::rename(tmp_path, &self.target_path).map_err(new_std_io_error)?;
            }
        }

        Ok(())
    }
}
