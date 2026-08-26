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

use super::core::parse_error;
use super::core::*;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct GcsDeleter {
    core: Arc<GcsCore>,
    ctx: OperationContext,
}

impl GcsDeleter {
    pub fn new(core: Arc<GcsCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for GcsDeleter {
    async fn delete_once(&self, path: String, args: OpDelete) -> Result<()> {
        let resp = self.core.gcs_delete_object(&self.ctx, &path, &args).await?;

        if resp.status().is_success() {
            Ok(())
        } else if resp.status() == StatusCode::NOT_FOUND
            && (args.if_match().is_some()
                || args.if_version_match().is_some()
                || args.if_version_not_match().is_some())
        {
            Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "delete precondition requires a live target",
            ))
        } else if resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(parse_error(
                ErrorContext::new(ServiceOperation("DeleteObject"))
                    .with_if_match(args.if_match().is_some())
                    .with_if_none_match(args.if_none_match().is_some())
                    .with_if_version_match(args.if_version_match().is_some())
                    .with_if_version_not_match(args.if_version_not_match().is_some()),
                resp,
            ))
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let resp = self.core.gcs_delete_objects(&self.ctx, &batch).await?;

        let status = resp.status();

        // If the overall request isn't formatted correctly and Cloud Storage is unable to parse it into sub-requests, you receive a 400 error.
        // Otherwise, Cloud Storage returns a 200 status code, even if some or all of the sub-requests fail.
        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BatchDeleteObjects")),
                resp,
            ));
        }

        let boundary = parse_multipart_boundary(resp.headers())?.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "gcs batch delete response content type is empty",
            )
        })?;
        let multipart: Multipart<MixedPart> = Multipart::new()
            .with_boundary(boundary)
            .parse(resp.into_body().to_bytes())?;
        let parts = multipart.into_parts();

        let mut batched_result = BatchDeleteResult::default();

        for (i, part) in parts.into_iter().enumerate() {
            let resp = part.into_response();
            // TODO: maybe we can take it directly?
            let (path, op) = batch[i].clone();

            if resp.status().is_success() {
                batched_result.succeeded.push((path, op));
            } else if resp.status() == StatusCode::NOT_FOUND
                && (op.if_match().is_some()
                    || op.if_version_match().is_some()
                    || op.if_version_not_match().is_some())
            {
                batched_result.failed.push((
                    path,
                    op,
                    Error::new(
                        ErrorKind::ConditionNotMatch,
                        "delete precondition requires a live target",
                    ),
                ));
            } else if resp.status() == StatusCode::NOT_FOUND {
                batched_result.succeeded.push((path, op));
            } else {
                let error_ctx = ErrorContext::new(ServiceOperation("BatchDeleteObjects"))
                    .with_if_match(op.if_match().is_some())
                    .with_if_none_match(op.if_none_match().is_some())
                    .with_if_version_match(op.if_version_match().is_some())
                    .with_if_version_not_match(op.if_version_not_match().is_some());
                batched_result
                    .failed
                    .push((path, op, parse_error(error_ctx, resp)));
            }
        }

        // If no object is deleted, return directly.
        if batched_result.succeeded.is_empty() {
            let err = batched_result.failed.remove(0).2;
            return Err(err);
        }

        Ok(batched_result)
    }
}
