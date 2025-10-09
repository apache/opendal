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

use js_sys::AsyncIterator;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::File;
use web_sys::FileSystemDirectoryHandle;
use web_sys::FileSystemFileHandle;
use web_sys::FileSystemGetDirectoryOptions;
use web_sys::FileSystemGetFileOptions;

use crate::raw::build_abs_path;
use crate::raw::parse_datetime_from_from_timestamp_millis;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

use super::error::*;
use super::utils::*;

#[derive(Debug)]
pub struct OpfsCore {
    root: String,
}

impl OpfsCore {
    pub(crate) fn new(root: String) -> Self {
        Self { root }
    }

    pub(crate) fn path(&self, path: &str) -> String {
        build_abs_path(&self.root, path)
    }

    /// Build write path and ensure the parent dirs created
    pub(crate) async fn ensure_write_path(&self, path: &str) -> Result<()> {
        let opt = FileSystemGetDirectoryOptions::new();
        opt.set_create(true);

        let path = build_abs_path(&self.root, path);
        let path = path
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|s| s.0)
            .unwrap_or("/");

        get_directory_handle(path, &opt).await?;

        Ok(())
    }

    pub(crate) async fn opfs_stat(&self, path: &str) -> Result<Metadata> {
        let parent_handle = self.parent_dir_handle(path).await?;
        let path = build_abs_path(&self.root, &path);
        let last_component = path
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|s| s.1)
            .unwrap_or("/");

        match JsFuture::from(parent_handle.get_directory_handle(last_component)).await {
            // TODO: set content length for directory metadata
            Ok(_) => Ok(Metadata::new(EntryMode::DIR)),
            Err(err) => {
                let err = js_sys::Error::from(err);
                match String::from(err.name()).as_str() {
                    JS_TYPE_MISMATCH_ERROR => {
                        // the entry is a file and not a directory
                        let handle: FileSystemFileHandle =
                            JsFuture::from(parent_handle.get_file_handle(last_component))
                                .await
                                .and_then(JsCast::dyn_into)
                                .map_err(parse_js_error)?;

                        let file: File = JsFuture::from(handle.get_file())
                            .await
                            .and_then(JsCast::dyn_into)
                            .map_err(parse_js_error)?;

                        let last_modified = file.last_modified() as i64;
                        let metadata = Metadata::new(EntryMode::FILE)
                            .with_content_length(file.size() as u64)
                            .with_last_modified(parse_datetime_from_from_timestamp_millis(
                                last_modified,
                            )?);

                        Ok(metadata)
                    }
                    _ => Err(parse_js_error(err.into())),
                }
            }
        }
    }

    pub(crate) async fn opfs_create_dir(&self, path: &str) -> Result<()> {
        let opt = FileSystemGetDirectoryOptions::new();
        opt.set_create(true);

        self.dir_handle_with_option(path, &opt).await?;

        Ok(())
    }

    /// List directory entries. Returns an [`AsyncIterator`] over the entries in the directory.
    pub(crate) async fn opfs_list(&self, path: &str) -> Result<AsyncIterator> {
        let opt = FileSystemGetDirectoryOptions::new();

        let handle = self.dir_handle_with_option(path, &opt).await?;

        Ok(handle.entries())
    }

    /// Get directory handle with options
    pub(crate) async fn dir_handle_with_option(
        &self,
        path: &str,
        opt: &FileSystemGetDirectoryOptions,
    ) -> Result<FileSystemDirectoryHandle> {
        let path = build_abs_path(&self.root, path);
        let dirs: Vec<&str> = path.trim_matches('/').split('/').collect();

        let mut handle = get_root_directory_handle().await?;
        for dir in dirs {
            handle = JsFuture::from(handle.get_directory_handle_with_options(dir, &opt))
                .await
                .and_then(JsCast::dyn_into)
                .map_err(parse_js_error)?;
        }
        Ok(handle)
    }

    /// Get parent directory handle
    pub(crate) async fn parent_dir_handle(&self, path: &str) -> Result<FileSystemDirectoryHandle> {
        let path = build_abs_path(&self.root, path);

        let paths: Vec<&str> = path.trim_matches('/').split('/').collect();

        let mut handle = get_root_directory_handle().await?;
        for dir in paths[0..paths.len() - 1].iter() {
            handle = JsFuture::from(handle.get_directory_handle(dir))
                .await
                .and_then(JsCast::dyn_into)
                .map_err(parse_js_error)?;
        }

        Ok(handle)
    }

    /// Get file handle
    pub(crate) async fn file_handle(&self, path: &str) -> Result<FileSystemFileHandle> {
        let opt = FileSystemGetFileOptions::new();
        self.file_handle_with_option(path, &opt).await
    }

    /// Get file handle with options
    pub(crate) async fn file_handle_with_option(
        &self,
        path: &str,
        opt: &FileSystemGetFileOptions,
    ) -> Result<FileSystemFileHandle> {
        let path = build_abs_path(&self.root, path);
        let paths: Vec<&str> = path.trim_matches('/').split('/').collect();

        let mut handle = get_root_directory_handle().await?;
        for dir in paths[0..paths.len() - 1].iter() {
            handle = JsFuture::from(handle.get_directory_handle(dir))
                .await
                .and_then(JsCast::dyn_into)
                .map_err(parse_js_error)?;
        }

        JsFuture::from(handle.get_file_handle_with_options(paths[paths.len() - 1], &opt))
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)
    }
}
