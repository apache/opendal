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

use http::StatusCode;

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct GdriveDeleter {
    core: Arc<GdriveCore>,
}

impl GdriveDeleter {
    pub fn new(core: Arc<GdriveCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for GdriveDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let path = build_abs_path(&self.core.root, &path);
        let file_id = self.core.path_cache.get(&path).await?;
        let file_id = if let Some(id) = file_id {
            id
        } else {
            return Ok(());
        };

        let resp = self.core.gdrive_trash(&file_id).await?;
        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        self.core.path_cache.remove(&path).await;

        Ok(())
    }
}
