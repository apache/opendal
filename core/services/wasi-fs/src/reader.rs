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

use opendal_core::raw::*;
use opendal_core::*;
use wasi::filesystem::types::{DescriptorFlags, OpenFlags};
use wasi::io::streams::InputStream;

use super::core::WasiFsCore;
use super::error::{parse_stream_error, parse_wasi_error};

pub struct WasiFsReader {
    stream: InputStream,
    remaining: u64,
}

impl WasiFsReader {
    pub fn new(core: Arc<WasiFsCore>, path: &str, range: BytesRange) -> Result<Self> {
        let file = core.open_file(path, OpenFlags::empty(), DescriptorFlags::READ)?;

        let stat = file.stat().map_err(parse_wasi_error)?;
        let (offset, size) = range.to_offset_size(stat.size);

        let stream = file.read_via_stream(offset).map_err(parse_wasi_error)?;

        Ok(Self {
            stream,
            remaining: size.unwrap_or(stat.size - offset),
        })
    }
}

/// # Safety
///
/// WasiFsReader only accesses WASI resources which are single-threaded in WASM.
unsafe impl Sync for WasiFsReader {}

impl oio::Read for WasiFsReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.remaining == 0 {
            return Ok(Buffer::new());
        }

        let to_read = self.remaining.min(64 * 1024);
        let data = self
            .stream
            .blocking_read(to_read)
            .map_err(parse_stream_error)?;

        if data.is_empty() {
            self.remaining = 0;
            return Ok(Buffer::new());
        }

        self.remaining -= data.len() as u64;
        Ok(Buffer::from(data))
    }
}
