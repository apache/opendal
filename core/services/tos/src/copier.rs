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
use std::sync::Mutex;

use bytes::Buf;
use http::StatusCode;

use crate::core::constants::X_TOS_VERSION_ID;
use crate::core::*;
use crate::error::parse_error;
use crate::utils::tos_parse_into_metadata;
use opendal_core::raw::*;
use opendal_core::*;

pub type TosCopiers = oio::MultipartCopier<TosCopier>;

pub fn new_tos_copier(
    core: Arc<TosCore>,
    from: &str,
    to: &str,
    args: OpCopy,
    opts: OpCopier,
) -> Result<TosCopiers> {
    let capability = core.info.full_capability();
    let max_part_size = capability.copy_multi_max_size.ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "multipart copy requires copy_multi_max_size capability",
        )
    })?;

    let (copy_once_threshold, part_size) = match opts.chunk() {
        Some(chunk) => {
            let min_part_size = capability.copy_multi_min_size.ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "multipart copy requires copy_multi_min_size capability",
                )
            })?;
            let part_size = chunk.clamp(min_part_size, max_part_size) as u64;
            (part_size.saturating_sub(1), part_size)
        }
        None => {
            let part_size = max_part_size as u64;
            (part_size, part_size)
        }
    };

    Ok(oio::MultipartCopier::new(
        core.info.clone(),
        TosCopier {
            core,
            from: from.to_string(),
            to: to.to_string(),
            args,
            source_metadata: Mutex::new(None),
        },
        opts.source_content_length_hint(),
        copy_once_threshold,
        part_size,
        opts.concurrent(),
    ))
}

pub struct TosCopier {
    core: Arc<TosCore>,
    from: String,
    to: String,
    args: OpCopy,
    source_metadata: Mutex<Option<Metadata>>,
}

impl TosCopier {
    async fn load_source_metadata(&self) -> Result<Metadata> {
        {
            let source_metadata = self
                .source_metadata
                .lock()
                .expect("source metadata mutex poisoned");
            if let Some(meta) = source_metadata.as_ref() {
                return Ok(meta.clone());
            }
        }

        let resp = self
            .core
            .tos_head_object(&self.from, OpStat::default())
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let headers = resp.headers();
                let mut meta = tos_parse_into_metadata(&self.from, headers)?;

                if let Some(v) = parse_header_to_str(headers, X_TOS_VERSION_ID)? {
                    meta.set_version(v);
                }

                let mut source_metadata = self
                    .source_metadata
                    .lock()
                    .expect("source metadata mutex poisoned");
                *source_metadata = Some(meta.clone());

                Ok(meta)
            }
            _ => Err(parse_error(resp)),
        }
    }
}

impl oio::MultipartCopy for TosCopier {
    async fn source_metadata(&self) -> Result<Metadata> {
        self.load_source_metadata().await
    }

    async fn copy_once(&self) -> Result<()> {
        let resp = self
            .core
            .tos_copy_object(&self.from, &self.to, &self.args)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let result: CopyObjectOutput = serde_json::from_reader(resp.into_body().reader())
                    .map_err(new_json_deserialize_error)?;
                if result.etag.is_empty() {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ETag not present in copy response",
                    )
                    .set_temporary());
                }

                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn initiate_copy(&self) -> Result<String> {
        self.load_source_metadata().await?;

        let resp = self.core.tos_initiate_multipart_copy(&self.to).await?;

        match resp.status() {
            StatusCode::OK => {
                let result: InitiateMultipartUploadResult =
                    serde_json::from_reader(resp.into_body().reader())
                        .map_err(new_json_deserialize_error)?;

                Ok(result.upload_id)
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn copy_part(
        &self,
        upload_id: &str,
        part_number: usize,
        range: BytesRange,
    ) -> Result<oio::MultipartPart> {
        let size = range.size().expect("multipart copy range must be sized");
        let part_number = part_number + 1;
        let (source_etag, source_version) = {
            let source_metadata = self
                .source_metadata
                .lock()
                .expect("source metadata mutex poisoned");
            (
                source_metadata
                    .as_ref()
                    .and_then(|meta| meta.etag())
                    .map(ToOwned::to_owned),
                source_metadata
                    .as_ref()
                    .and_then(|meta| meta.version())
                    .map(ToOwned::to_owned),
            )
        };

        let req = self
            .core
            .tos_upload_part_copy_request(TosUploadPartCopyRequest {
                from: &self.from,
                to: &self.to,
                upload_id,
                part_number,
                range,
                source_etag: source_etag.as_deref(),
                source_version: source_version.as_deref(),
            })?;
        let resp = self.core.send(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let result: UploadPartCopyOutput =
                    serde_json::from_reader(resp.into_body().reader())
                        .map_err(new_json_deserialize_error)?;
                if result.etag.is_empty() {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "ETag not present in copy part response",
                    )
                    .set_temporary());
                }

                Ok(oio::MultipartPart {
                    part_number,
                    etag: result.etag.trim_matches('"').to_string(),
                    checksum: None,
                    size: Some(size),
                })
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn complete_copy(&self, upload_id: &str, parts: &[oio::MultipartPart]) -> Result<()> {
        let parts = parts
            .iter()
            .map(|p| CompleteMultipartUploadRequestPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();

        let resp = self
            .core
            .tos_complete_multipart_copy(&self.to, upload_id, parts, &self.args)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let ret: CompleteMultipartUploadResult =
                    serde_json::from_reader(resp.into_body().reader())
                        .map_err(new_json_deserialize_error)?;
                if !ret.code.is_empty() {
                    return Err(Error::new(ErrorKind::Unexpected, ret.message));
                }

                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn abort_copy(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .tos_abort_multipart_copy(&self.to, upload_id)
            .await?;
        match resp.status() {
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }
}
