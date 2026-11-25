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
use std::sync::OnceLock;

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

pub struct OssCopier {
    core: Arc<OssCore>,
    ctx: OperationContext,
    from: String,
    to: String,
    args: OpCopy,
    part_size: u64,
    source: OnceLock<CopySource>,
}

impl OssCopier {
    pub fn new(
        core: Arc<OssCore>,
        ctx: OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        part_size: u64,
    ) -> Self {
        Self {
            core,
            ctx,
            from: from.to_string(),
            to: to.to_string(),
            args,
            part_size,
            source: OnceLock::new(),
        }
    }

    pub fn into_one_shot(self) -> oio::OneShotCopier {
        oio::OneShotCopier::new(async move {
            let size = match self.args.source_content_length_hint() {
                Some(size) => size,
                None => self.source().await?.metadata.content_length(),
            };
            oio::MultipartCopy::copy_once(&self).await?;
            Ok(MetadataBuilder::file(size).build())
        })
    }

    async fn source(&self) -> Result<&CopySource> {
        if let Some(source) = self.source.get() {
            return Ok(source);
        }

        let args = options::StatOptions {
            version: self.args.source_version().map(str::to_owned),
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
        Ok(self.source.get_or_init(|| CopySource {
            metadata,
            headers: parts.headers,
        }))
    }
}

impl oio::MultipartCopy for OssCopier {
    async fn source_metadata(&self) -> Result<Metadata> {
        Ok(self.source().await?.metadata.clone())
    }

    async fn copy_once(&self) -> Result<Metadata> {
        let resp = self
            .core
            .oss_copy_object(&self.ctx, &self.from, &self.to)
            .await?;
        match resp.status() {
            StatusCode::OK => Ok(MetadataBuilder::unknown().build()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CopyObject")),
                resp,
            )),
        }
    }

    async fn initiate_copy(&self) -> Result<String> {
        // Read an ETag even with a length hint so all parts use the same source identity.
        let source = self.source().await?;
        let size = source.metadata.content_length();
        if self
            .args
            .source_content_length_hint()
            .is_some_and(|hint| hint != size)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "OSS multipart copy source length does not match the supplied hint",
            ));
        }
        if source.metadata.etag().is_none_or(str::is_empty) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "OSS multipart copy requires a source ETag",
            ));
        }
        if size.div_ceil(self.part_size) > MAX_PARTS {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "OSS multipart copy exceeds 10,000 parts, please increase `OpCopy::chunk`",
            ));
        }

        let resp = self
            .core
            .oss_initiate_copy(&self.ctx, &self.to, &source.headers)
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
            .get()
            .and_then(|source| source.metadata.etag())
            .expect("initiate_copy requires a source ETag");
        let part_number = part_number + 1;
        let resp = self
            .core
            .oss_upload_part_copy(
                &self.ctx,
                &self.from,
                &self.to,
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
        let resp = self
            .core
            .oss_complete_multipart_upload_request(
                &self.ctx,
                &self.to,
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
        let resp = self
            .core
            .oss_abort_multipart_upload(&self.ctx, &self.to, upload_id)
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
