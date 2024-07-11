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

use std::{io::Cursor, sync::Arc};

use compio::{buf::buf_try, fs::File, io::AsyncWrite};

use super::core::CompfsCore;
use crate::raw::*;
use crate::*;

#[derive(Debug)]
pub struct CompfsWriter {
    core: Arc<CompfsCore>,
    file: Option<Cursor<File>>,
}

impl CompfsWriter {
    pub(super) fn new(core: Arc<CompfsCore>, file: Cursor<File>) -> Self {
        Self {
            core,
            file: Some(file),
        }
    }
}

impl oio::Write for CompfsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<usize> {
        let mut file = self
            .file
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "file has been closed"))?;

        let n = self
            .core
            .exec(move || async move {
                let (n, _) = buf_try!(@try file.write(bs).await);
                Ok(n)
            })
            .await?;
        Ok(n)
    }

    async fn close(&mut self) -> Result<()> {
        let f = self
            .file
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "file has been closed"))?;
        self.core
            .exec(move || async move { f.get_ref().sync_all().await })
            .await?;

        let f = self
            .file
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "file has been closed"))?;
        self.core
            .exec(move || async move { f.into_inner().close().await })
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}
