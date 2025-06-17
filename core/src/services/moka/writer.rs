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

use super::core::{MokaCore, MokaValue};
use crate::raw::oio;
use crate::raw::*;
use crate::*;

pub struct MokaWriter {
    core: std::sync::Arc<MokaCore>,
    path: String,
    op: OpWrite,
    buffer: oio::QueueBuf,
}

impl MokaWriter {
    pub fn new(core: std::sync::Arc<MokaCore>, path: String, op: OpWrite) -> Self {
        Self {
            core,
            path,
            op,
            buffer: oio::QueueBuf::new(),
        }
    }
}

impl oio::Write for MokaWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buffer.push(bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let buf = self.buffer.clone().collect();
        let length = buf.len() as u64;

        // Build metadata with write options
        let mut metadata =
            Metadata::new(EntryMode::from_path(&self.path)).with_content_length(length);

        if let Some(content_type) = self.op.content_type() {
            metadata.set_content_type(content_type);
        }
        if let Some(content_disposition) = self.op.content_disposition() {
            metadata.set_content_disposition(content_disposition);
        }
        if let Some(cache_control) = self.op.cache_control() {
            metadata.set_cache_control(cache_control);
        }
        if let Some(content_encoding) = self.op.content_encoding() {
            metadata.set_content_encoding(content_encoding);
        }

        let value = MokaValue {
            metadata: metadata.clone(),
            content: buf,
        };

        self.core.set(&self.path, value).await?;
        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        self.buffer.clear();
        Ok(())
    }
}
