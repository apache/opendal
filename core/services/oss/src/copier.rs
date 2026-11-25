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
use http::HeaderMap;
use http::StatusCode;
use serde::Deserialize;

use super::core::*;
use opendal_core::raw::*;
use opendal_core::*;

const MAX_PARTS: u64 = 10_000;

struct CopySource {
    metadata: Metadata,
    headers: HeaderMap,
}

#[derive(Deserialize)]
struct CopyResult {
    #[serde(rename = "ETag")]
    etag: String,
}

pub struct OssCopier(CopyState);

enum CopyState {
    Pending {
        request: CopyRequest,
        args: OpCopy,
        part_size: u64,
    },
    OneShot(oio::OneShotCopier),
    Multipart(Box<oio::MultipartCopier<OssMultipartCopy>>),
    Aborted,
}

#[derive(Clone)]
struct CopyRequest {
    core: Arc<OssCore>,
    ctx: OperationContext,
    from: String,
    to: String,
}

struct OssMultipartCopy {
    request: CopyRequest,
    source: CopySource,
    tags: String,
}

impl OssCopier {
    pub fn new(
        core: Arc<OssCore>,
        ctx: OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        part_size: Option<u64>,
    ) -> Self {
        let request = CopyRequest {
            core,
            ctx,
            from: from.to_string(),
            to: to.to_string(),
        };
        let size_hint = args.source_content_length_hint();
        if let Some(part_size) = part_size.filter(|&part| size_hint.is_none_or(|size| size >= part))
        {
            Self(CopyState::Pending {
                request,
                args,
                part_size,
            })
        } else {
            Self(CopyState::OneShot(request.one_shot(args, size_hint)))
        }
    }

    async fn start(&mut self) -> Result<()> {
        let CopyState::Pending {
            request,
            args,
            part_size,
        } = &self.0
        else {
            return Ok(());
        };
        let source = request.source(args).await?;
        let size = source.metadata.content_length();
        if args
            .source_content_length_hint()
            .is_some_and(|hint| hint != size)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "OSS multipart copy source length does not match the supplied hint",
            ));
        }
        if size < *part_size {
            self.0 = CopyState::OneShot(request.clone().one_shot(args.clone(), Some(size)));
            return Ok(());
        }
        if source.metadata.etag().is_none_or(str::is_empty) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "OSS multipart copy requires a source ETag",
            ));
        }
        if size.div_ceil(*part_size) > MAX_PARTS {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "OSS multipart copy exceeds 10,000 parts, please increase `OpCopy::chunk`",
            ));
        }

        // OSS omits the tag count for untagged objects and callers without tag-read access.
        let tags = if source
            .headers
            .get(constants::X_OSS_TAGGING_COUNT)
            .is_some_and(|count| count == "0")
        {
            String::new()
        } else {
            match request
                .core
                .oss_get_copy_tags(&request.ctx, &request.from)
                .await
            {
                Ok(tags) => tags,
                Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                    // Let native CopyObject handle tags and authorization when we cannot read tags.
                    self.0 = CopyState::OneShot(request.clone().one_shot(args.clone(), Some(size)));
                    return Ok(());
                }
                Err(err) => return Err(err),
            }
        };
        self.0 = CopyState::Multipart(Box::new(oio::MultipartCopier::new(
            request.ctx.executor().clone(),
            request.core.capability,
            OssMultipartCopy {
                request: request.clone(),
                source,
                tags,
            },
            Some(size),
            part_size - 1,
            *part_size,
            args.concurrent(),
        )));
        Ok(())
    }
}

impl oio::Copy for OssCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        self.start().await?;
        match &mut self.0 {
            CopyState::Pending { .. } => unreachable!("copy starts before advancing"),
            CopyState::OneShot(copier) => copier.next().await,
            CopyState::Multipart(copier) => copier.next().await,
            CopyState::Aborted => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.start().await?;
        match &mut self.0 {
            CopyState::Pending { .. } => unreachable!("copy starts before closing"),
            CopyState::OneShot(copier) => copier.close().await,
            CopyState::Multipart(copier) => copier.close().await,
            CopyState::Aborted => Err(Error::new(ErrorKind::Unexpected, "OSS copy was aborted")),
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match &mut self.0 {
            CopyState::Pending { .. } => {
                self.0 = CopyState::Aborted;
                Ok(())
            }
            CopyState::OneShot(copier) => copier.abort().await,
            CopyState::Multipart(copier) => copier.abort().await,
            CopyState::Aborted => Ok(()),
        }
    }
}

impl CopyRequest {
    fn one_shot(self, args: OpCopy, size: Option<u64>) -> oio::OneShotCopier {
        oio::OneShotCopier::new(async move {
            let size = match size {
                Some(size) => size,
                None => self.source(&args).await?.metadata.content_length(),
            };
            self.copy_once().await?;
            Ok(MetadataBuilder::file(size).build())
        })
    }

    async fn source(&self, args: &OpCopy) -> Result<CopySource> {
        let args = options::StatOptions {
            version: args.source_version().map(str::to_owned),
            ..Default::default()
        }
        .into();
        let resp = self
            .core
            .oss_head_object(&self.ctx, &self.from, &args)
            .await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("HeadObject")),
                resp,
            ));
        }
        let (parts, _) = resp.into_parts();
        let metadata = parse_into_metadata(&self.from, &parts.headers)?;
        Ok(CopySource {
            metadata,
            headers: parts.headers,
        })
    }

    async fn copy_once(&self) -> Result<()> {
        let resp = self
            .core
            .oss_copy_object(&self.ctx, &self.from, &self.to)
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CopyObject")),
                resp,
            )),
        }
    }
}

impl oio::MultipartCopy for OssMultipartCopy {
    async fn source_metadata(&self) -> Result<Metadata> {
        Ok(self.source.metadata.clone())
    }

    async fn copy_once(&self) -> Result<Metadata> {
        self.request.copy_once().await?;
        Ok(MetadataBuilder::file(self.source.metadata.content_length()).build())
    }

    async fn initiate_copy(&self) -> Result<String> {
        let request = &self.request;
        let resp = request
            .core
            .oss_initiate_copy(&request.ctx, &request.to, &self.source.headers, &self.tags)
            .await?;
        match resp.status() {
            StatusCode::OK => {
                let result: InitiateMultipartUploadResult =
                    quick_xml::de::from_reader(resp.into_body().reader())
                        .map_err(new_xml_deserialize_error)?;
                Ok(result.upload_id)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("InitiateMultipartUpload")),
                resp,
            )),
        }
    }

    async fn copy_part(
        &self,
        upload_id: &str,
        part_number: usize,
        range: BytesRange,
    ) -> Result<oio::MultipartPart> {
        let etag = self
            .source
            .metadata
            .etag()
            .expect("multipart copy requires a source ETag");
        let part_number = part_number + 1;
        let request = &self.request;
        let resp = request
            .core
            .oss_upload_part_copy(
                &request.ctx,
                &request.from,
                &request.to,
                upload_id,
                part_number,
                range,
                etag,
            )
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let result: CopyResult = quick_xml::de::from_reader(resp.into_body().reader())
                    .map_err(new_xml_deserialize_error)?;
                Ok(oio::MultipartPart {
                    part_number,
                    etag: result.etag,
                    checksum: None,
                    size: range.size(),
                })
            }
            StatusCode::PRECONDITION_FAILED => Err(Error::new(
                ErrorKind::Conflict,
                "OSS multipart copy source changed",
            )
            .set_source(parse_error(
                ErrorContext::new(ServiceOperation("UploadPartCopy")),
                resp,
            ))),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadPartCopy")),
                resp,
            )),
        }
    }

    async fn complete_copy(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        let parts = parts
            .iter()
            .map(|part| MultipartUploadPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            })
            .collect();
        let request = &self.request;
        let resp = request
            .core
            .oss_complete_multipart_upload_request(
                &request.ctx,
                &request.to,
                upload_id,
                false,
                parts,
                &OpWrite::new(),
            )
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let (parts, body) = resp.into_parts();
                let result: CopyResult =
                    quick_xml::de::from_reader(body.reader()).map_err(new_xml_deserialize_error)?;
                let mut metadata = MetadataBuilder::unknown();
                metadata.etag(result.etag);
                if let Some(version) =
                    parse_header_to_str(&parts.headers, constants::X_OSS_VERSION_ID)?
                {
                    metadata.version(version);
                }
                Ok(metadata.build())
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CompleteMultipartUpload")),
                resp,
            )),
        }
    }

    async fn abort_copy(&self, upload_id: &str) -> Result<()> {
        let request = &self.request;
        let resp = request
            .core
            .oss_abort_multipart_upload(&request.ctx, &request.to, upload_id)
            .await?;
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(());
        }
        let error = parse_error(
            ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
            resp,
        );
        match error.kind() {
            ErrorKind::NotFound => Ok(()),
            _ => Err(error),
        }
    }
}
