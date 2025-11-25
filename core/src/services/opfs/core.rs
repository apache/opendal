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

use std::fmt::Debug;

use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::File;
use web_sys::FileSystemWritableFileStream;

use super::error::*;
use super::utils::*;
use crate::Error;
use crate::Result;

#[derive(Default, Debug)]
pub struct OpfsCore {}

impl OpfsCore {
    #[allow(unused)]
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

    #[allow(unused)]
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
