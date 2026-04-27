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

use goosefs_sdk::io::GooseFsFileWriter as ClientWriter;

use super::core::GooseFsCore;
use super::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub type GooseFsWriters = GooseFsWriter;

/// GooseFsWriter implements `oio::Write` using goosefs-sdk
/// high-level `GooseFsFileWriter`.
///
/// Key differences from AlluxioWriter:
/// - Alluxio uses stream_id based REST write: `create_file() → write(stream_id, data) → close(stream_id)`
/// - GooseFS uses block-level gRPC streaming: `GooseFsFileWriter::create() → write() → close()`
///   which internally handles block splitting, worker routing, and gRPC bidirectional streams.
pub struct GooseFsWriter {
    core: Arc<GooseFsCore>,
    _op: OpWrite,
    path: String,
    /// Lazily initialized GooseFsFileWriter from goosefs-sdk.
    ///
    /// Created on first `write()` call, closed in `close()`.
    writer: Option<ClientWriter>,
}

impl GooseFsWriter {
    pub fn new(core: Arc<GooseFsCore>, _op: OpWrite, path: String) -> Self {
        GooseFsWriter {
            core,
            _op,
            path,
            writer: None,
        }
    }
}

impl oio::Write for GooseFsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let writer = match &mut self.writer {
            Some(w) => w,
            None => {
                // Lazy init: create GooseFsFileWriter on first write
                let w = self.core.create_writer(&self.path).await?;
                self.writer = Some(w);
                self.writer.as_mut().unwrap()
            }
        };

        writer.write(&bs.to_bytes()).await.map_err(parse_error)?;

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let Some(mut writer) = self.writer.take() else {
            // No data was written, nothing to close
            return Ok(Metadata::default());
        };

        writer.close().await.map_err(parse_error)?;

        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> Result<()> {
        // GooseFsFileWriter doesn't support abort natively.
        // Drop the writer — incomplete writes won't be committed
        // since complete_file() won't be called.
        self.writer.take();
        Ok(())
    }
}
