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

use opendal_core::Result;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemDirectoryHandle;
use web_sys::FileSystemFileHandle;
use web_sys::FileSystemGetDirectoryOptions;
use web_sys::FileSystemGetFileOptions;
use web_sys::window;

use super::error::*;

/// Get the OPFS root directory handle.
pub(crate) async fn get_root_directory_handle() -> Result<FileSystemDirectoryHandle> {
    console_debug!("get_root_directory_handle");
    let navigator = window().unwrap().navigator();
    let storage_manager = navigator.storage();
    // This may fail if not secure (not: HTTPS or localhost)
    JsFuture::from(storage_manager.get_directory())
        .await
        .and_then(JsCast::dyn_into)
        .map_err(parse_js_error)
}

/// Navigate to a directory handle by path.
///
/// When `create` is true, intermediate directories are created as needed.
pub(crate) async fn get_directory_handle(
    path: &str,
    create: bool,
) -> Result<FileSystemDirectoryHandle> {
    console_debug!("get_directory_handle path={path:?} create={create:?}");
    let opt = FileSystemGetDirectoryOptions::new();
    opt.set_create(create);

    let dirs = path.trim_matches('/').split('/');
    let mut handle = get_root_directory_handle().await?;
    for dir in dirs {
        handle = JsFuture::from(handle.get_directory_handle_with_options(dir, &opt))
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;
    }

    Ok(handle)
}

/// Split a file path into its parent directory handle and filename.
///
/// When `create` is true, intermediate directories are created as needed.
pub(crate) async fn get_parent_dir_and_name<'a>(
    path: &'a str,
    create: bool,
) -> Result<(FileSystemDirectoryHandle, &'a str)> {
    let trimmed = path.trim_matches('/');
    match trimmed.rsplit_once('/') {
        Some((parent, name)) => {
            let dir = get_directory_handle(parent, create).await?;
            Ok((dir, name))
        }
        None => {
            let root = get_root_directory_handle().await?;
            Ok((root, trimmed))
        }
    }
}

/// Get a file handle by its full path.
///
/// When `create` is true, intermediate directories and the file itself are created as needed.
pub(crate) async fn get_file_handle(path: &str, create: bool) -> Result<FileSystemFileHandle> {
    let (dir, name) = get_parent_dir_and_name(path, create).await?;

    let opt = FileSystemGetFileOptions::new();
    opt.set_create(create);

    JsFuture::from(dir.get_file_handle_with_options(name, &opt))
        .await
        .and_then(JsCast::dyn_into)
        .map_err(parse_js_error)
}
