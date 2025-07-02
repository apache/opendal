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
use crate::raw::oio::BatchDeleteResult;
use crate::raw::*;
use crate::services::cloudflare_kv::model::CfKvDeleteResponse;
use crate::*;

pub struct CloudflareKvDeleter {
    core: Arc<CloudflareKvCore>,
}

impl CloudflareKvDeleter {
    pub fn new(core: Arc<CloudflareKvCore>) -> Self {
        Self { core }
    }
}

impl oio::BatchDelete for CloudflareKvDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let path = build_abs_path(&self.core.info.root(), &path);
        let resp = self
            .core
            .delete(&[path.trim_end_matches('/').to_string()])
            .await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(resp.clone()));
        }

        let bs = resp.clone().into_body();
        let res: CfKvDeleteResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if !res.success {
            return Err(parse_error(resp.clone()));
        }

        Ok(())
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let keys = batch
            .iter()
            .map(|path| {
                let path = build_abs_path(&self.core.info.root(), &path.0);
                path.trim_end_matches('/').to_string()
            })
            .collect::<Vec<String>>();

        let resp = self.core.delete(&keys).await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let res: CfKvDeleteResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let result = match (res.success, res.result) {
            (true, Some(result)) => result,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cloudflare_kv delete this key failed for reason we don't know",
                ))
            }
        };

        let mut batched_result = BatchDeleteResult {
            succeeded: Vec::with_capacity(result.successful_key_count),
            failed: Vec::with_capacity(result.unsuccessful_keys.len()),
        };

        for item in batch {
            if result.unsuccessful_keys.contains(&item.0) {
                batched_result.failed.push((
                    item.0,
                    item.1,
                    Error::new(
                        ErrorKind::Unexpected,
                        "cloudflare_kv delete this key failed for reason we don't know",
                    ),
                ));
            } else {
                batched_result.succeeded.push(item);
            }
        }

        Ok(batched_result)
    }
}
