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

use std::io;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::pin::{pin, Pin};
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::AsyncWrite;
use futures_lite::prelude::*;
use nuclei::Handle;
use pin_project_lite::pin_project;

use crate::raw::*;
use crate::*;

pin_project! {
    pub struct FsWriter<F: AsRawFd> {
        target_path: PathBuf,
        tmp_path: Option<PathBuf>,

        #[pin]
        f: Handle<F>
    }
}

impl<F: AsRawFd> FsWriter<F> {
    pub fn new(target_path: PathBuf, tmp_path: Option<PathBuf>, f: F) -> Self {
        Self {
            target_path,
            tmp_path,

            f: Handle::new(f).unwrap(),
        }
    }
}

impl oio::Write for FsWriter<std::fs::File> {
    fn poll_write(&mut self, cx: &mut Context<'_>, bs: &dyn oio::WriteBuf) -> Poll<Result<usize>> {
        let this = Pin::new(self).project();
        this.f
            .poll_write_vectored(cx, &bs.vectored_chunk())
            .map_err(new_std_io_error)
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = Pin::new(self).project();
        let mut handle = this.f;

        let tmp_path = this.tmp_path.clone();
        let target_path = this.target_path.clone();

        let mut clo = async move {
            handle.flush().await.map_err(new_std_io_error)?;
            handle.sync_all().map_err(new_std_io_error)?;

            if let Some(tmp_path) = &tmp_path {
                std::fs::rename(tmp_path, &target_path).map_err(new_std_io_error)?;
            }

            Ok(())
        }
        .boxed_local();

        clo.poll(cx)
    }

    fn poll_abort(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut this = Pin::new(self).project();
        let tmp_path = this.tmp_path.clone();

        let mut clo = async move {
            if let Some(tmp_path) = &tmp_path {
                std::fs::remove_file(tmp_path).map_err(new_std_io_error)
            } else {
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "Fs doesn't support abort if atomic_write_dir is not set",
                ))
            }
        }
        .boxed();

        clo.poll(cx)
    }
}

impl oio::BlockingWrite for FsWriter<std::fs::File> {
    fn write(&mut self, bs: &dyn oio::WriteBuf) -> Result<usize> {
        let mut this = Pin::new(self).project();

        nuclei::block_on(async move {
            this.f
                .write_vectored(&bs.vectored_chunk())
                .await
                .map_err(new_std_io_error)
        })
    }

    fn close(&mut self) -> Result<()> {
        self.f.sync_all().map_err(new_std_io_error)?;

        if let Some(tmp_path) = &self.tmp_path {
            std::fs::rename(tmp_path, &self.target_path).map_err(new_std_io_error)?;
        }

        Ok(())
    }
}
