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

use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemFileHandle;

use super::error::*;
use opendal_core::raw::*;
use opendal_core::*;

pub struct OpfsReader {
    handle: FileSystemFileHandle,
    range: BytesRange,
    done: bool,
}

/// Safety: wasm32 is single-threaded, `Send` and `Sync` are meaningless.
unsafe impl Send for OpfsReader {}
/// Safety: wasm32 is single-threaded, `Send` and `Sync` are meaningless.
unsafe impl Sync for OpfsReader {}

impl OpfsReader {
    pub fn new(handle: FileSystemFileHandle, range: BytesRange) -> Self {
        Self {
            handle,
            range,
            done: false,
        }
    }
}

impl oio::Read for OpfsReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.done {
            return Ok(Buffer::new());
        }
        self.done = true;

        let file: web_sys::File = JsFuture::from(self.handle.get_file())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;

        let blob: &web_sys::Blob = file.as_ref();
        let blob = if self.range.is_full() {
            blob.clone()
        } else {
            let offset = self.range.offset() as f64;
            let end = match self.range.size() {
                Some(size) => offset + size as f64,
                None => blob.size(),
            };
            blob.slice_with_f64_and_f64(offset, end)
                .map_err(parse_js_error)?
        };

        let array_buffer = JsFuture::from(blob.array_buffer())
            .await
            .map_err(parse_js_error)?;
        let uint8_array = js_sys::Uint8Array::new(&array_buffer);

        Ok(Buffer::from(uint8_array.to_vec()))
    }
}
