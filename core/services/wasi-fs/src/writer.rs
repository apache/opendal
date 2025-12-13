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
use wasi::filesystem::types::{Descriptor, DescriptorFlags, OpenFlags};
use wasi::io::streams::OutputStream;

use super::core::WasiFsCore;
use super::error::{parse_stream_error, parse_wasi_error};

pub struct WasiFsWriter {
    file: Descriptor,
    stream: OutputStream,
    offset: u64,
}

impl WasiFsWriter {
    pub fn new(core: Arc<WasiFsCore>, path: &str, args: OpWrite) -> Result<Self> {
        let mut open_flags = OpenFlags::CREATE;

        if !args.append() {
            open_flags |= OpenFlags::TRUNCATE;
        }

        let file = core.open_file(path, open_flags, DescriptorFlags::WRITE)?;

        let offset = if args.append() {
            let stat = file.stat().map_err(parse_wasi_error)?;
            stat.size
        } else {
            0
        };

        let stream = file.write_via_stream(offset).map_err(parse_wasi_error)?;

        Ok(Self {
            file,
            stream,
            offset,
        })
    }
}

/// # Safety
///
/// WasiFsWriter only accesses WASI resources which are single-threaded in WASM.
unsafe impl Sync for WasiFsWriter {}

impl oio::Write for WasiFsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let data: Vec<u8> = bs.to_vec();

        self.stream
            .blocking_write_and_flush(&data)
            .map_err(parse_stream_error)?;

        self.offset += data.len() as u64;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.stream.blocking_flush().map_err(parse_stream_error)?;
        self.file.sync_data().map_err(parse_wasi_error)?;

        let stat = self.file.stat().map_err(parse_wasi_error)?;

        Ok(Metadata::new(EntryMode::FILE).with_content_length(stat.size))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "WasiFs doesn't support abort without atomic write support",
        ))
    }
}
