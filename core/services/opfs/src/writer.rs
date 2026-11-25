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

use send_wrapper::SendWrapper;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemWritableFileStream;
use web_sys::WriteCommandType;
use web_sys::WriteParams;

use opendal_core::raw::*;
use opendal_core::*;

use super::core::OpfsCore;
use super::core::*;

enum WriterState {
    Idle,
    Open(SendWrapper<FileSystemWritableFileStream>),
    Closed,
    Aborted,
}

pub struct OpfsWriter {
    core: Arc<OpfsCore>,
    path: String,
    state: WriterState,
    bytes_written: u64,
}

impl OpfsWriter {
    pub fn new(core: Arc<OpfsCore>, path: String) -> Self {
        Self {
            core,
            path,
            state: WriterState::Idle,
            bytes_written: 0,
        }
    }

    async fn stream(&mut self) -> Result<&FileSystemWritableFileStream> {
        if matches!(self.state, WriterState::Idle) {
            let path = build_absolute_path(&self.core.root, &self.path);
            let handle = get_file_handle(&path, true).await?;
            let stream = JsFuture::from(handle.create_writable())
                .await
                .and_then(JsCast::dyn_into)
                .map_err(parse_js_error)?;
            self.state = WriterState::Open(SendWrapper::new(stream));
        }

        match &self.state {
            WriterState::Idle => unreachable!("opfs writable stream must be initialized"),
            WriterState::Open(stream) => Ok(stream),
            WriterState::Closed => Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already closed",
            )),
            WriterState::Aborted => Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already aborted",
            )),
        }
    }
}

impl oio::Write for OpfsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let stream = self.stream().await?;
        let bytes = bs.to_bytes();
        let params = WriteParams::new(WriteCommandType::Write);
        params.set_size(Some(bytes.len() as f64));
        let data: JsValue = js_sys::Uint8Array::from(bytes.as_ref()).into();
        params.set_data(&data);
        JsFuture::from(
            stream
                .write_with_write_params(&params)
                .map_err(parse_js_error)?,
        )
        .await
        .map_err(parse_js_error)?;

        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let stream = self.stream().await?;
        // Keep the original handle if close fails or is canceled. Opening a
        // fresh writable stream could truncate the file on a subsequent close.
        JsFuture::from(stream.close())
            .await
            .map_err(parse_js_error)?;
        self.state = WriterState::Closed;

        Ok(MetadataBuilder::file(self.bytes_written).build())
    }

    async fn abort(&mut self) -> Result<()> {
        match &self.state {
            WriterState::Idle | WriterState::Aborted => (),
            WriterState::Open(stream) => {
                JsFuture::from(stream.abort())
                    .await
                    .map_err(parse_js_error)?;
            }
            WriterState::Closed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer is already closed",
                ));
            }
        }
        self.state = WriterState::Aborted;
        Ok(())
    }
}
