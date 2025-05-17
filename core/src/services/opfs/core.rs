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

use crate::{Error, ErrorKind, Result};
use std::fmt::Debug;

use web_sys::{
    window, File, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetFileOptions,
    FileSystemWritableFileStream,
};

use wasm_bindgen::{JsCast, JsValue};

use wasm_bindgen_futures::JsFuture;

fn parse_js_error(msg: JsValue) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        msg.as_string().unwrap_or_else(String::new),
    )
}

fn dyn_map_err<T: JsCast>(result: Result<JsValue, JsValue>) -> Result<T, Error> {
    result.and_then(JsCast::dyn_into).map_err(parse_js_error)
}

async fn get_handle_by_filename(filename: &str) -> Result<FileSystemFileHandle, Error> {
    let navigator = window().unwrap().navigator();
    let storage_manager = navigator.storage();
    let root: FileSystemDirectoryHandle = JsFuture::from(storage_manager.get_directory())
        .await
        .and_then(JsCast::dyn_into)
        .map_err(parse_js_error)?;

    // maybe the option should be exposed?
    let opt = FileSystemGetFileOptions::new();
    opt.set_create(true);

    JsFuture::from(root.get_file_handle_with_options(filename, &opt))
        .await
        .and_then(JsCast::dyn_into)
        .map_err(parse_js_error)
}

#[derive(Default, Debug)]
pub struct OpfsCore {}

impl OpfsCore {
    async fn store_file(&self, file_name: &str, content: &[u8]) -> Result<(), Error> {
        let handle = get_handle_by_filename(file_name).await?;

        let writable: FileSystemWritableFileStream = JsFuture::from(handle.create_writable())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;

        // QuotaExceeded or NotAllowed
        JsFuture::from(
            writable
                .write_with_u8_array(content)
                .map_err(parse_js_error)?,
        )
        .await
        .map_err(parse_js_error)?;

        JsFuture::from(writable.close())
            .await
            .map_err(parse_js_error)?;

        Ok(())
    }

    async fn read_file(&self, file_name: &str) -> Result<Vec<u8>, Error> {
        let handle = get_handle_by_filename(file_name).await?;

        let file: File = JsFuture::from(handle.get_file())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;
        let array_buffer = JsFuture::from(file.array_buffer())
            .await
            .map_err(parse_js_error)?;

        Ok(js_sys::Uint8Array::new(&array_buffer).to_vec())
    }
}
