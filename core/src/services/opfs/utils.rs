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

use crate::Result;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    window, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions,
};

use super::error::*;

pub(crate) async fn get_root_directory_handle() -> Result<FileSystemDirectoryHandle> {
    let navigator = window().unwrap().navigator();
    let storage_manager = navigator.storage();
    JsFuture::from(storage_manager.get_directory())
        .await
        .and_then(JsCast::dyn_into)
        .map_err(parse_js_error)
}

pub(crate) async fn get_directory_handle(
    dir: &str,
    dir_opt: &FileSystemGetDirectoryOptions,
) -> Result<FileSystemDirectoryHandle> {
    let dirs: Vec<&str> = dir.trim_matches('/').split('/').collect();

    let mut handle = get_root_directory_handle().await?;
    for dir in dirs {
        handle = JsFuture::from(handle.get_directory_handle_with_options(dir, dir_opt))
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;
    }

    Ok(handle)
}

pub(crate) async fn get_handle_by_filename(filename: &str) -> Result<FileSystemFileHandle> {
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
