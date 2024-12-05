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

pub struct B2Deleter {
    core: Arc<B2Core>,
}

impl B2Deleter {
    pub fn new(core: Arc<B2Core>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for B2Deleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.hide_file(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(()),
            _ => {
                let err = parse_error(resp);
                match err.kind() {
                    ErrorKind::NotFound => Ok(()),
                    // Representative deleted
                    ErrorKind::AlreadyExists => Ok(()),
                    _ => Err(err),
                }
            }
        }
    }
}
