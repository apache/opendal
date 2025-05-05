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
use super::error::{parse_error, parse_s3_error_code};
use crate::raw::oio::BatchDeleteResult;
use crate::raw::*;
use crate::*;
use bytes::Buf;
use http::StatusCode;
use std::sync::Arc;

pub struct S3Deleter {
    core: Arc<S3Core>,
}

impl S3Deleter {
    pub fn new(core: Arc<S3Core>) -> Self {
        Self { core }
    }
}

impl oio::BatchDelete for S3Deleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        // This would delete the bucket, do not perform
        if self.core.root == "/" && path == "/" {
            return Ok(());
        }

        let resp = self.core.s3_delete_object(&path, &args).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(()),
            // Allow 404 when deleting a non-existing object
            // This is not a standard behavior, only some s3 alike service like GCS XML API do this.
            // ref: <https://cloud.google.com/storage/docs/xml-api/delete-object>
            StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let resp = self.core.s3_delete_objects(batch).await?;

        let status = resp.status();
        if status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let mut result: DeleteObjectsResult =
            quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        // If no object is deleted, return directly.
        if result.deleted.is_empty() {
            let err = result.error.remove(0);
            return Err(parse_delete_objects_result_error(err));
        }

        let mut batched_result = BatchDeleteResult {
            succeeded: Vec::with_capacity(result.deleted.len()),
            failed: Vec::with_capacity(result.error.len()),
        };
        for i in result.deleted {
            let path = build_rel_path(&self.core.root, &i.key);
            let mut op = OpDelete::new();
            if let Some(version_id) = i.version_id {
                op = op.with_version(version_id.as_str());
            }
            batched_result.succeeded.push((path, op));
        }
        for i in result.error {
            let path = build_rel_path(&self.core.root, &i.key);
            let mut op = OpDelete::new();
            if let Some(version_id) = &i.version_id {
                op = op.with_version(version_id.as_str());
            }
            batched_result
                .failed
                .push((path, op, parse_delete_objects_result_error(i)));
        }

        Ok(batched_result)
    }
}

fn parse_delete_objects_result_error(err: DeleteObjectsResultError) -> Error {
    let (kind, retryable) =
        parse_s3_error_code(err.code.as_str()).unwrap_or((ErrorKind::Unexpected, false));
    let mut err: Error = Error::new(kind, format!("{err:?}"));
    if retryable {
        err = err.set_temporary();
    }
    err
}
