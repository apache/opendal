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

use std::sync::Arc;
use std::time::SystemTime;

use super::core::DashmapCore;
use super::core::DashmapValue;
use crate::raw::{oio, OpWrite};
use crate::*;

pub struct DashmapWriter {
    core: Arc<DashmapCore>,
    path: String,
    op: OpWrite,

    buf: oio::QueueBuf,
}

impl DashmapWriter {
    pub fn new(core: Arc<DashmapCore>, path: String, op: OpWrite) -> Self {
        DashmapWriter {
            core,
            path,
            op,
            buf: oio::QueueBuf::new(),
        }
    }
}

impl oio::Write for DashmapWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buf.push(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let content = self.buf.clone().collect();

        let entry_mode = EntryMode::from_path(&self.path);
        let mut meta = Metadata::new(entry_mode);
        meta.set_content_length(content.len() as u64);
        meta.set_last_modified(SystemTime::now().into());

        if let Some(v) = self.op.content_type() {
            meta.set_content_type(v);
        }
        if let Some(v) = self.op.content_disposition() {
            meta.set_content_disposition(v);
        }
        if let Some(v) = self.op.cache_control() {
            meta.set_cache_control(v);
        }
        if let Some(v) = self.op.content_encoding() {
            meta.set_content_encoding(v);
        }

        self.core.set(
            &self.path,
            DashmapValue {
                metadata: meta.clone(),
                content,
            },
        )?;

        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        self.buf.clear();
        Ok(())
    }
}
