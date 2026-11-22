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

use super::core::*;
#[cfg(target_os = "linux")]
use super::direct_io::DirectIoAlignment;
use opendal_core::raw::*;
use opendal_core::*;
use std::fs::File;
use std::sync::Arc;

/// Reader returned by this backend.
pub struct FsReader {
    core: Arc<FsCore>,
    path: String,
}

impl FsReader {
    pub(super) fn new(core: Arc<FsCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
        }
    }
}

pub struct FsReaderHandle {
    core: Arc<FsCore>,
    file: Arc<File>,
    #[cfg(target_os = "linux")]
    direct_io: Option<DirectIoAlignment>,
}

impl oio::PositionRead for FsReader {
    type Handle = FsReaderHandle;

    async fn open(&self) -> Result<Self::Handle> {
        let path = self.core.root_join(&self.path)?;
        #[cfg(target_os = "linux")]
        if self.core.direct_io {
            let (file, alignment) = tokio::task::spawn_blocking(move || {
                let mut options = std::fs::OpenOptions::new();
                options.read(true);
                DirectIoAlignment::open(&path, options, ServiceOperation("OpenForRead"), false)
            })
            .await
            .map_err(new_task_join_error)??;
            return Ok(FsReaderHandle {
                core: self.core.clone(),
                file: file.into(),
                direct_io: Some(alignment),
            });
        }

        let file = tokio::fs::File::open(path)
            .await
            .map_err(new_std_io_error)?
            .into_std()
            .await;
        Ok(FsReaderHandle {
            core: self.core.clone(),
            file: file.into(),
            #[cfg(target_os = "linux")]
            direct_io: None,
        })
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }

        #[cfg(target_os = "linux")]
        if let Some(alignment) = handle.direct_io {
            let file = handle.file.clone();
            return tokio::task::spawn_blocking(move || alignment.read_at(&file, offset, size))
                .await
                .map_err(new_task_join_error)?;
        }

        let mut bs = handle.core.buf_pool.get();
        bs.resize(size, 0);

        let f = handle.file.clone();
        let (n, mut bs) = tokio::task::spawn_blocking(move || {
            let n = super::backend::read_at(&f, &mut bs, offset)?;
            Ok::<_, Error>((n, bs))
        })
        .await
        .map_err(new_task_join_error)??;

        let frozen = bs.split_to(n).freeze();
        handle.core.buf_pool.put(bs);

        Ok(Buffer::from(frozen))
    }
}
