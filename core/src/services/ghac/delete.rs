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

use super::backend::GhacBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::*;
use http::StatusCode;

pub struct GhacDeleter {
    core: GhacBackend,
}

impl GhacDeleter {
    pub fn new(core: GhacBackend) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for GhacDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        if self.core.api_token.is_empty() {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "github token is not configured, delete is permission denied",
            ));
        }

        let resp = self.core.ghac_delete(&path).await?;

        // deleting not existing objects is ok
        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(parse_error(resp))
        }
    }
}
