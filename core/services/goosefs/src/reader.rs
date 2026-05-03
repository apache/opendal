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

use super::core::GooseFsCore;
use opendal_core::raw::*;
use opendal_core::*;

/// GooseFsReader implements `oio::Read` using goosefs-sdk
/// high-level `GooseFsFileReader`.
///
/// Unlike Alluxio which returns `HttpBody` (streaming HTTP response),
/// GooseFS uses block-level gRPC streaming. The reader lazily opens
/// a `GooseFsFileReader` on first `read()` call.
pub struct GooseFsReader {
    core: Arc<GooseFsCore>,
    path: String,
    args: OpRead,
    /// Cached file data — we read all requested data on first call.
    ///
    /// This is a simplified implementation. For very large files,
    /// a future optimization would be to stream block-by-block.
    data: Option<Buffer>,
    done: bool,
}

impl GooseFsReader {
    pub fn new(core: Arc<GooseFsCore>, path: String, args: OpRead) -> Self {
        GooseFsReader {
            core,
            path,
            args,
            data: None,
            done: false,
        }
    }
}

impl oio::Read for GooseFsReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.done {
            return Ok(Buffer::new());
        }

        // If we already have cached data, return it and mark done
        if let Some(data) = self.data.take() {
            self.done = true;
            return Ok(data);
        }

        // Lazy initialization: read from GooseFS on first call
        let range = self.args.range();
        let offset = range.offset();
        let size = range.size();

        let bytes = if offset > 0 || size.is_some() {
            // Range read
            let len = match size {
                Some(s) => s,
                None => {
                    // Need file length first
                    let info = self.core.get_status(&self.path).await?;
                    let file_len = info.length.unwrap_or(0) as u64;
                    file_len.saturating_sub(offset)
                }
            };
            self.core
                .read_file(&self.path, Some(offset), Some(len))
                .await?
        } else {
            // Full file read
            self.core.read_file(&self.path, None, None).await?
        };

        self.done = true;
        Ok(Buffer::from(bytes))
    }
}
