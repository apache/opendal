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

use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemWritableFileStream;
use web_sys::WriteCommandType;
use web_sys::WriteParams;

use opendal_core::raw::*;
use opendal_core::*;

use super::error::*;

pub struct OpfsWriter {
    stream: SendWrapper<FileSystemWritableFileStream>,
    bytes_written: u64,
}

impl OpfsWriter {
    pub fn new(stream: FileSystemWritableFileStream) -> Self {
        Self {
            stream: SendWrapper::new(stream),
            bytes_written: 0,
        }
    }
}

impl oio::Write for OpfsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let bytes = bs.to_bytes();
        let params = WriteParams::new(WriteCommandType::Write);
        params.set_size(Some(bytes.len() as f64));
        let data: JsValue = js_sys::Uint8Array::from(bytes.as_ref()).into();
        params.set_data(&data);
        JsFuture::from(
            self.stream
                .write_with_write_params(&params.into())
                .map_err(parse_js_error)?,
        )
        .await
        .map_err(parse_js_error)?;

        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        JsFuture::from(self.stream.close())
            .await
            .map_err(parse_js_error)?;

        // We cannot set LastModified here - stream does not have such metadata
        let mut meta = Metadata::new(EntryMode::FILE);
        meta.set_content_length(self.bytes_written);
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        JsFuture::from(self.stream.abort())
            .await
            .map_err(parse_js_error)?;
        Ok(())
    }
}
