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

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;
use http::StatusCode;
use std::sync::Arc;

pub struct UpyunDeleter {
    core: Arc<UpyunCore>,
}

impl UpyunDeleter {
    pub fn new(core: Arc<UpyunCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for UpyunDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.delete(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(()),
            // Allow 404 when deleting a non-existing object
            StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }
}
