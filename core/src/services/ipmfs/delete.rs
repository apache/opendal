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

use super::core::IpmfsCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;
use http::StatusCode;
use std::sync::Arc;

pub struct IpmfsDeleter {
    core: Arc<IpmfsCore>,
}

impl IpmfsDeleter {
    pub fn new(core: Arc<IpmfsCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for IpmfsDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.ipmfs_rm(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }
}
