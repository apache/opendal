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

use bytes::Buf;
use http::StatusCode;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

use crate::core::*;

pub struct TosDeleter {
    core: Arc<TosCore>,
}

impl TosDeleter {
    pub fn new(core: Arc<TosCore>) -> Self {
        Self { core }
    }
}

impl oio::BatchDelete for TosDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        if self.core.root == "/" && path == "/" {
            return Ok(());
        }

        let resp = self.core.tos_delete_object(&path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => Ok(()),
            _ => Err(crate::error::parse_error(resp)),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let resp = self.core.tos_delete_objects(batch).await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(crate::error::parse_error(resp));
        }

        let bs = resp.into_body();

        let result: DeleteObjectsResult =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let mut batched_result = BatchDeleteResult {
            succeeded: Vec::with_capacity(result.deleted.len()),
            failed: Vec::with_capacity(result.errors.len()),
        };
        for i in result.deleted {
            let path = build_rel_path(&self.core.root, &i.key);
            let mut op = OpDelete::new();
            if let Some(version_id) = i.version_id {
                op = op.with_version(version_id.as_str());
            }
            batched_result.succeeded.push((path, op));
        }
        for i in result.errors {
            let path = build_rel_path(&self.core.root, &i.key);
            let mut op = OpDelete::new();
            if let Some(version_id) = &i.version_id {
                op = op.with_version(version_id.as_str());
            }
            batched_result
                .failed
                .push((path, op, Error::new(ErrorKind::Unexpected, i.message)));
        }

        Ok(batched_result)
    }
}
