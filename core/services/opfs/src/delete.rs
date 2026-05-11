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

use opendal_core::{
    ErrorKind, Result,
    raw::{OpDelete, oio::OneShotDelete},
};
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemRemoveOptions;

use super::core::OpfsCore;
use super::error::*;

pub struct OpfsDeleter {
    core: Arc<OpfsCore>,
}

impl OpfsDeleter {
    pub(crate) fn new(core: Arc<OpfsCore>) -> Self {
        Self { core }
    }
}

impl OneShotDelete for OpfsDeleter {
    async fn delete_once(&self, path: String, op: OpDelete) -> Result<()> {
        let handle = match self.core.parent_dir_handle(&path).await {
            Ok(handle) => handle,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };

        let path = self.core.path(&path);
        let entry_name = path
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|p| p.1)
            .unwrap_or("/");

        let opt = FileSystemRemoveOptions::new();
        opt.set_recursive(op.recursive());

        match JsFuture::from(handle.remove_entry_with_options(entry_name, &opt))
            .await
            .map_err(parse_js_error)
        {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }
}
