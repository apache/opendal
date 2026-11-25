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

use std::num::NonZeroU64;

use bytes::Buf;
use http::Request;
use http::StatusCode;
use http::header::CONTENT_LENGTH;

use opendal_core::options::CopyOptions;
use opendal_core::raw::oio::Copy;
use opendal_core::raw::*;
use opendal_core::*;

use super::backend::GcsBackend;
use super::copier::GcsCopier;
use super::core::{
    ErrorContext, GcsCore, GcsErrorResponse, ListResponse, gcs_percent_encode_path, parse_error,
};

struct DeletedGeneration {
    id: NonZeroU64,
    deleted_at: Timestamp,
    soft_deleted: bool,
    requires_restore_token: bool,
}

impl GcsBackend {
    pub(super) async fn restore_object(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<()> {
        let version = args
            .version()
            .map(|value| {
                value
                    .parse::<NonZeroU64>()
                    .map(|id| id.to_string())
                    .map_err(|err| {
                        Error::new(ErrorKind::ConfigInvalid, "invalid GCS restore generation")
                            .set_source(err)
                    })
            })
            .transpose()?;
        if let Some(version) = version.as_deref() {
            match self
                .rewrite_generation(ctx, path, version, args.if_not_exists())
                .await
            {
                Err(err) if err.kind() == ErrorKind::NotFound => (),
                result => return result,
            }
        } else {
            let mut req = self
                .core
                .gcs_head_object_request(path, &OpStat::default())?;
            req.extensions_mut().insert(Operation::Restore);
            let req = self.core.sign(ctx, req).await?;
            let resp = self.core.send(ctx, req).await?;
            match resp.status() {
                StatusCode::OK => return Ok(()),
                StatusCode::NOT_FOUND => (),
                _ => {
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("GetObject")),
                        resp,
                    ));
                }
            }
        }

        let generation = self
            .core
            .find_deleted_generation(ctx, path, version.as_deref())
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    "no recoverable object generation exists",
                )
            })?;
        let version = generation.id.to_string();
        let protect_live = args.version().is_none() || args.if_not_exists();
        let result = if generation.soft_deleted {
            if generation.requires_restore_token {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "restore tokens for hierarchical namespace buckets are unsupported",
                ));
            }
            self.core
                .restore_soft_deleted_generation(ctx, path, &version, protect_live)
                .await
        } else {
            self.rewrite_generation(ctx, path, &version, protect_live)
                .await
        };
        // The destination condition proves the path became live after discovery.
        match result {
            Err(err) if args.version().is_none() && err.kind() == ErrorKind::ConditionNotMatch => {
                Ok(())
            }
            result => result,
        }
    }

    async fn rewrite_generation(
        &self,
        ctx: &OperationContext,
        path: &str,
        version: &str,
        protect_live: bool,
    ) -> Result<()> {
        let args = OpCopy::from_options(
            &self.core.capability,
            CopyOptions {
                source_version: Some(version.to_string()),
                // GCS reserves generation zero for an absent live destination.
                if_version_match: protect_live.then(|| "0".to_string()),
                ..Default::default()
            },
        )?;
        GcsCopier::new(self.core.clone(), ctx.clone(), path, path, args)
            .with_operation(Operation::Restore)
            .close()
            .await?;
        Ok(())
    }
}

impl GcsCore {
    async fn find_deleted_generation(
        &self,
        ctx: &OperationContext,
        path: &str,
        version: Option<&str>,
    ) -> Result<Option<DeletedGeneration>> {
        let key = build_absolute_path(&self.root, path);
        let prefix = gcs_percent_encode_path(&key);
        let list_url = format!("{}/storage/v1/b/{}/o", self.endpoint, self.bucket);
        let mut selected: Option<DeletedGeneration> = None;
        for soft_deleted in [false, true] {
            if version.is_some() && !soft_deleted {
                continue;
            }
            let selector = if soft_deleted {
                "softDeleted"
            } else {
                "versions"
            };
            let mut page_token = String::new();
            'pages: loop {
                let mut url = QueryPairsWriter::new(&list_url)
                    .push("prefix", &prefix)
                    .push(selector, "true")
                    .push("fields", "nextPageToken,items(name,generation,timeDeleted,softDeleteTime,restoreToken)");
                if !page_token.is_empty() {
                    url = url.push("pageToken", &gcs_percent_encode_path(&page_token));
                }
                let req = Request::get(url.finish())
                    .extension(Operation::Restore)
                    .extension(ServiceOperation("ListObjects"))
                    .body(Buffer::new())
                    .map_err(new_request_build_error)?;
                let req = self.sign(ctx, req).await?;
                let resp = self.send(ctx, req).await?;
                if resp.status() != StatusCode::OK {
                    // GCS rejects a soft-deleted listing on buckets without a retention policy.
                    if soft_deleted
                        && page_token.is_empty()
                        && resp.status() == StatusCode::BAD_REQUEST
                        && serde_json::from_reader::<_, GcsErrorResponse>(
                            resp.body().clone().reader(),
                        )
                        .is_ok_and(|err| err.has_reason("invalidArgument"))
                    {
                        break;
                    }
                    return Err(parse_error(
                        ErrorContext::new(ServiceOperation("ListObjects")),
                        resp,
                    ));
                }
                let page: ListResponse = serde_json::from_reader(resp.into_body().reader())
                    .map_err(new_json_deserialize_error)?;
                for item in page.items {
                    // Objects are grouped by exact name and sorted lexicographically.
                    if item.name != key {
                        break 'pages;
                    }
                    if version.is_some_and(|id| id != item.generation) {
                        continue;
                    }
                    let deleted_at = if soft_deleted {
                        item.soft_delete_time.as_deref().ok_or_else(|| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "soft-deleted generation has no deletion time",
                            )
                        })?
                    } else if let Some(deleted_at) = &item.time_deleted {
                        deleted_at
                    } else {
                        continue;
                    };
                    let candidate = DeletedGeneration {
                        id: item.generation.parse::<NonZeroU64>().map_err(|err| {
                            Error::new(ErrorKind::Unexpected, "invalid restore generation")
                                .set_source(err)
                        })?,
                        deleted_at: deleted_at.parse()?,
                        soft_deleted,
                        requires_restore_token: item.restore_token.is_some(),
                    };
                    if version.is_some() && selected.is_some() {
                        return Err(Error::new(
                            ErrorKind::Unsupported,
                            "multiple soft-deleted objects share the requested generation",
                        ));
                    }
                    if selected.as_ref().is_none_or(|previous| {
                        (candidate.deleted_at, candidate.id) > (previous.deleted_at, previous.id)
                    }) {
                        selected = Some(candidate);
                    }
                }
                let Some(next) = page.next_page_token.filter(|token| !token.is_empty()) else {
                    break;
                };
                if page_token == next {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "restore listing did not advance",
                    ));
                }
                page_token = next;
            }
        }
        Ok(selected)
    }

    async fn restore_soft_deleted_generation(
        &self,
        ctx: &OperationContext,
        path: &str,
        generation: &str,
        protect_live: bool,
    ) -> Result<()> {
        let path = build_absolute_path(&self.root, path);
        let url = format!(
            "{}/storage/v1/b/{}/o/{}/restore",
            self.endpoint,
            self.bucket,
            gcs_percent_encode_path(&path)
        );
        let mut url = QueryPairsWriter::new(&url).push("generation", generation);
        if protect_live {
            url = url.push("ifGenerationMatch", "0");
        }
        let operation = ServiceOperation("RestoreObject");
        let req = Request::post(url.finish())
            .header(CONTENT_LENGTH, 0)
            .extension(Operation::Restore)
            .extension(operation)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let req = self.sign(ctx, req).await?;
        // The source remains recoverable after a successful restore.
        let resp = self.send(ctx, req).await.map_err(Error::set_permanent)?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(operation).with_caller_condition(protect_live),
                resp,
            )
            .with_permanent(status.is_server_error())),
        }
    }
}
