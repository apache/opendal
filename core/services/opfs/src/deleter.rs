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

use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemDirectoryHandle;
use web_sys::FileSystemRemoveOptions;

use super::core::{OpfsCore, get_parent_dir_and_name, parse_js_error};
use opendal_core::raw::*;
use opendal_core::*;

pub struct OpfsDeleter {
    core: Arc<OpfsCore>,
}

impl OpfsDeleter {
    pub fn new(core: Arc<OpfsCore>) -> Self {
        Self { core }
    }

    async fn remove_entry(
        dir: &FileSystemDirectoryHandle,
        name: &str,
        options: &FileSystemRemoveOptions,
    ) -> Result<()> {
        match JsFuture::from(dir.remove_entry_with_options(name, options))
            .await
            .map_err(parse_js_error)
        {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}

impl oio::OneShotDelete for OpfsDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let p = build_absolute_path(&self.core.root, &path);
        let (dir, name) = match get_parent_dir_and_name(&p, false).await {
            Ok(entry) => entry,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

        let options = FileSystemRemoveOptions::new();
        options.set_recursive(args.recursive());

        // The origin root has no parent entry. Remove its children in place.
        if name.is_empty() && args.recursive() {
            let keys = dir.keys();
            loop {
                let next = JsFuture::from(keys.next().map_err(parse_js_error)?)
                    .await
                    .map_err(parse_js_error)?;
                let next: js_sys::IteratorNext = next.unchecked_into();
                if next.done() {
                    return Ok(());
                }
                let name = next.value().as_string().ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "OPFS directory key is not a string")
                })?;
                Self::remove_entry(&dir, &name, &options).await?;
            }
        }

        Self::remove_entry(&dir, name, &options).await
    }
}
