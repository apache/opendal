// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;

use async_trait::async_trait;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

use crate::raw::*;
use crate::*;

use super::error::parse_io_error;

pub struct FsWriter<F> {
    target_path: PathBuf,
    tmp_path: Option<PathBuf>,
    f: F,
    pos: u64,
}

impl<F> FsWriter<F> {
    pub fn new(target_path: PathBuf, tmp_path: Option<PathBuf>, f: F) -> Self {
        Self {
            target_path,
            tmp_path,
            f,
            pos: 0,
        }
    }
}

#[async_trait]
impl output::Write for FsWriter<tokio::fs::File> {
    /// # Notes
    ///
    /// File could be partial written, so we will seek to start to make sure
    /// we write the same content.
    async fn write(&mut self, bs: Vec<u8>) -> Result<()> {
        self.f
            .seek(SeekFrom::Start(0))
            .await
            .map_err(parse_io_error)?;
        self.f.write_all(&bs).await.map_err(parse_io_error)?;

        Ok(())
    }

    async fn append(&mut self, bs: Vec<u8>) -> Result<()> {
        self.f
            .seek(SeekFrom::Start(self.pos))
            .await
            .map_err(parse_io_error)?;
        self.f.write_all(&bs).await.map_err(parse_io_error)?;
        self.pos += bs.len() as u64;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.f.sync_all().await.map_err(parse_io_error)?;

        Ok(())
    }
}

impl output::BlockingWrite for FsWriter<std::fs::File> {
    /// # Notes
    ///
    /// File could be partial written, so we will seek to start to make sure
    /// we write the same content.
    fn write(&mut self, bs: Vec<u8>) -> Result<()> {
        self.f.seek(SeekFrom::Start(0)).map_err(parse_io_error)?;
        self.f.write_all(&bs).map_err(parse_io_error)?;

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.f.sync_all().map_err(parse_io_error)?;

        Ok(())
    }
}
