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

use super::core::*;
use super::error::parse_error;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct OssDeleter {
    core: Arc<OssCore>,
}

impl OssDeleter {
    pub fn new(core: Arc<OssCore>) -> Self {
        Self { core }
    }
}

impl oio::BatchDelete for OssDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let resp = self.core.oss_delete_object(&path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let resp = self.core.oss_delete_objects(batch.clone()).await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let result: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        if result.deleted.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "oss delete this key failed for reason we don't know",
            ));
        }

        // Build a lookup from (rel_path, version) to input index.
        let mut lookup: std::collections::HashMap<(String, Option<String>), usize> =
            std::collections::HashMap::with_capacity(batch.len());
        for (idx, (path, op)) in batch.iter().enumerate() {
            lookup.insert((path.clone(), op.version().map(|v| v.to_string())), idx);
        }

        // Track which indices have been accounted for.
        let mut accounted: std::collections::HashSet<usize> =
            std::collections::HashSet::with_capacity(batch.len());

        let mut batched_result = BatchDeleteResult {
            succeeded: Vec::with_capacity(result.deleted.len()),
            failed: Vec::with_capacity(batch.len() - result.deleted.len()),
        };

        for i in result.deleted {
            let path = build_rel_path(&self.core.root, &i.key);
            let version = i.version_id;
            if let Some(&idx) = lookup.get(&(path, version)) {
                batched_result.succeeded.push(idx);
                accounted.insert(idx);
            }
        }

        // Any items not accounted for are considered failed.
        for idx in 0..batch.len() {
            if !accounted.contains(&idx) {
                batched_result.failed.push((
                    idx,
                    Error::new(
                        ErrorKind::Unexpected,
                        "oss delete this key failed for reason we don't know",
                    ),
                ));
            }
        }

        Ok(batched_result)
    }
}
