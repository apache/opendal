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

use crate::core::*;
use crate::error::S3Error;
use crate::error::from_s3_error;
use crate::error::parse_error;
use opendal_core::raw::*;
use opendal_core::*;

pub type S3Copiers = oio::MultipartCopier<S3Copier>;

pub fn new_s3_copier(
    core: Arc<S3Core>,
    from: &str,
    to: &str,
    args: OpCopy,
    opts: OpCopier,
) -> Result<S3Copiers> {
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
        S3Copier {
            core,
            from: from.to_string(),
            to: to.to_string(),
            args,
        },
        opts.source_content_length_hint(),
        copy_once_threshold,
        part_size,
        opts.concurrent(),
    ))
}

pub struct S3Copier {
    core: Arc<S3Core>,
    from: String,
    to: String,
    args: OpCopy,
}

impl oio::MultipartCopy for S3Copier {
    async fn source_metadata(&self) -> Result<Metadata> {
        let resp = self
            .core
            .s3_head_object(&self.from, OpStat::default())
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let headers = resp.headers();
                parse_into_metadata(&self.from, headers)
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn copy_once(&self) -> Result<()> {
        let resp = self
            .core
            .s3_copy_object(&self.from, &self.to, &self.args)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.into_body().to_bytes();

                let result: CopyObjectResult =
                    quick_xml::de::from_reader(body.as_ref()).map_err(new_xml_deserialize_error)?;

                if result.etag.is_empty() {
                    return Err(
                        Error::new(ErrorKind::Unexpected, String::from_utf8_lossy(&body))
                            .set_temporary(),
                    );
                }

                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn initiate_copy(&self) -> Result<String> {
        let resp = self.core.s3_initiate_multipart_copy(&self.to).await?;

        match resp.status() {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: InitiateMultipartUploadResult =
                    quick_xml::de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

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

        let req = self
            .core
            .s3_upload_part_copy_request(S3UploadPartCopyRequest {
                from: &self.from,
                to: &self.to,
                upload_id,
                part_number,
                range,
            })?;

        let resp = self.core.send(req).await?;

        match resp.status() {
            StatusCode::OK => {
                let body = resp.into_body().to_bytes();
                let result: CopyObjectResult =
                    quick_xml::de::from_reader(body.as_ref()).map_err(new_xml_deserialize_error)?;

                if result.etag.is_empty() {
                    return Err(
                        Error::new(ErrorKind::Unexpected, String::from_utf8_lossy(&body))
                            .set_temporary(),
                    );
                }

                Ok(oio::MultipartPart {
                    part_number,
                    etag: result.etag,
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
                ..Default::default()
            })
            .collect();

        let resp = self
            .core
            .s3_complete_multipart_copy(&self.to, upload_id, parts, &self.args)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let (parts, body) = resp.into_parts();

                let ret: CompleteMultipartUploadResult =
                    quick_xml::de::from_reader(body.reader()).map_err(new_xml_deserialize_error)?;
                if !ret.code.is_empty() {
                    return Err(from_s3_error(
                        S3Error {
                            code: ret.code,
                            message: ret.message,
                            resource: "".to_string(),
                            request_id: ret.request_id,
                        },
                        parts,
                    ));
                }

                Ok(())
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn abort_copy(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .s3_abort_multipart_copy(&self.to, upload_id)
            .await?;
        match resp.status() {
            StatusCode::NO_CONTENT => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }
}
