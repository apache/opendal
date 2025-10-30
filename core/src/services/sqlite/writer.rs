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

use crate::raw::oio;
use crate::services::sqlite::core::SqliteCore;
use crate::{Buffer, EntryMode, Metadata};

pub struct SqliteWriter {
    core: std::sync::Arc<SqliteCore>,
    path: String,
    buffer: oio::QueueBuf,
}

impl SqliteWriter {
    pub fn new(core: std::sync::Arc<SqliteCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
            buffer: oio::QueueBuf::new(),
        }
    }
}

impl oio::Write for SqliteWriter {
    async fn write(&mut self, bs: Buffer) -> crate::Result<()> {
        self.buffer.push(bs);
        Ok(())
    }

    async fn close(&mut self) -> crate::Result<Metadata> {
        let buf = self.buffer.clone().collect();
        let length = buf.len() as u64;
        self.core.set(&self.path, buf).await?;

        let meta = Metadata::new(EntryMode::from_path(&self.path)).with_content_length(length);
        Ok(meta)
    }

    async fn abort(&mut self) -> crate::Result<()> {
        self.buffer.clear();
        Ok(())
    }
}
