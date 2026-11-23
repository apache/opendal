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

use std::path::PathBuf;
use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::{Buffer, Result};

use crate::backend::Hf3fsCore;
use crate::driver::{IoDriver, RegisteredFile};

pub(crate) struct Hf3fsReader {
    pub core: Arc<Hf3fsCore>,
    pub path: PathBuf,
}

pub(crate) struct Hf3fsReadHandle {
    file: Arc<RegisteredFile>,
    driver: IoDriver,
}

impl oio::PositionRead for Hf3fsReader {
    type Handle = Hf3fsReadHandle;

    async fn open(&self) -> Result<Self::Handle> {
        let api = self.core.api.clone();
        let path = self.path.clone();
        let file = tokio::task::spawn_blocking(move || {
            RegisteredFile::new(api, std::fs::File::open(path).map_err(new_std_io_error)?)
        })
        .await
        .map_err(new_task_join_error)??;
        Ok(Hf3fsReadHandle {
            file,
            driver: self.core.read.clone(),
        })
    }

    async fn read_at(handle: &Self::Handle, offset: u64, size: usize) -> Result<Buffer> {
        if size == 0 {
            return Ok(Buffer::new());
        }
        let (_, buffer) = handle
            .driver
            .submit(
                handle.file.clone(),
                offset,
                size.min(handle.driver.block_size),
                None,
            )
            .await?;
        Ok(buffer)
    }
}
