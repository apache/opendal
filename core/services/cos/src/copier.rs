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
use http::Request;
use http::StatusCode;
use http::header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE};
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, EXPIRES};

use crate::core::*;
use opendal_core::raw::*;
use opendal_core::*;

// Upload Part Copy accepts 1 MiB through 5 GiB, except for the final part.
// https://www.tencentcloud.com/document/product/436/8287
pub(crate) const COS_COPY_MIN_PART_SIZE: usize = 1024 * 1024;
#[cfg(target_pointer_width = "64")]
pub(crate) const COS_COPY_MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;
#[cfg(not(target_pointer_width = "64"))]
pub(crate) const COS_COPY_MAX_PART_SIZE: usize = usize::MAX;
const COS_MAX_COPY_PARTS: u64 = 10_000;

pub type CosCopiers = oio::MultipartCopier<CosCopier>;

pub struct CosCopier {
    core: Arc<CosCore>,
    ctx: OperationContext,
    from: String,
    to: String,
    args: OpCopy,
    part_size: u64,
    source: OnceLock<CopySource>,
}

struct CopySource {
    metadata: Metadata,
    headers: HeaderMap,
}

impl CosCopier {
    pub fn multipart(
        core: Arc<CosCore>,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> CosCopiers {
        let part_size = args
            .chunk()
            .unwrap_or(COS_COPY_MAX_PART_SIZE)
            .clamp(COS_COPY_MIN_PART_SIZE, COS_COPY_MAX_PART_SIZE) as u64;
        let copy_once_threshold = if args.if_not_exists() {
            // Keep the existing conditional CopyObject path.
            u64::MAX
        } else if args.chunk().is_some() {
            part_size - 1
        } else {
            part_size
        };
        let source_content_length_hint = args.source_content_length_hint();
        let concurrent = args.concurrent();
        oio::MultipartCopier::new(
            ctx.executor().clone(),
            core.capability,
            Self {
                core,
                ctx: ctx.clone(),
                from: from.to_owned(),
                to: to.to_owned(),
                args,
                part_size,
                source: OnceLock::new(),
            },
            source_content_length_hint,
            copy_once_threshold,
            part_size,
            concurrent,
        )
    }

    async fn load_source(&self) -> Result<&CopySource> {
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
            .cos_head_object(&self.ctx, &self.from, &args)
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

impl oio::MultipartCopy for CosCopier {
    async fn source_metadata(&self) -> Result<Metadata> {
        Ok(self.load_source().await?.metadata.clone())
    }

    async fn copy_once(&self) -> Result<Metadata> {
        self.core
            .cos_copy_object(&self.ctx, &self.from, &self.to, &self.args)
            .await
    }

    async fn initiate_copy(&self) -> Result<String> {
        // Even with a length hint, multipart copy needs the source ETag and metadata.
        let source = self.load_source().await?;
        let size = source.metadata.content_length();
        if let Some(hint) = self.args.source_content_length_hint()
            && hint != size
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "multipart copy source length differs from the hint",
            )
            .with_context("hint", hint)
            .with_context("source_size", size));
        }
        if size.div_ceil(self.part_size) > COS_MAX_COPY_PARTS {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "multipart copy exceeds the COS part count limit, increase the chunk size",
            )
            .with_context("source_size", size)
            .with_context("part_size", self.part_size));
        }
        if source.metadata.etag().is_none_or(str::is_empty) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "multipart copy requires a source ETag",
            ));
        }

        let mut req =
            self.core
                .cos_initiate_multipart_upload_request(&self.to, false, Operation::Copy);
        // UploadPartCopy moves bytes only. Set the source metadata when creating the upload.
        for name in [
            CACHE_CONTROL,
            CONTENT_DISPOSITION,
            CONTENT_ENCODING,
            CONTENT_LANGUAGE,
            CONTENT_TYPE,
            EXPIRES,
        ] {
            if let Some(value) = source.headers.get(&name) {
                req = req.header(name, value);
            }
        }
        for (name, value) in &source.headers {
            if name.as_str().starts_with("x-cos-meta-") {
                req = req.header(name, value);
            }
        }
        let source_version = self.args.source_version().or(parse_header_to_str(
            &source.headers,
            constants::X_COS_VERSION_ID,
        )?);
        let tags = self
            .core
            .cos_get_object_tags(&self.ctx, &self.from, source_version)
            .await?;
        if !tags.is_empty() {
            req = req.header("x-cos-tagging", tags);
        }
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;
        let req = self.core.sign(&self.ctx, req).await?;
        let resp = self.core.send(&self.ctx, req).await?;
        if resp.status() != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("InitiateMultipartUpload")),
                resp,
            ));
        }
        let result: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(resp.into_body().reader())
                .map_err(new_xml_deserialize_error)?;
        if result.upload_id.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "COS did not return a multipart copy upload ID",
            ));
        }
        Ok(result.upload_id)
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
            .expect("multipart copy source ETag is loaded before initiating the upload");
        let from = build_absolute_path(&self.core.root, &self.from);
        let to = build_absolute_path(&self.core.root, &self.to);
        let (_, authority) = self
            .core
            .endpoint
            .split_once("://")
            .expect("COS endpoint has a scheme");
        let mut source = format!("{authority}/{}", percent_encode_path(&from));
        if let Some(version) = self.args.source_version() {
            source.push_str("?versionId=");
            source.push_str(&percent_encode_path(version));
        }
        let part_number = part_number + 1;
        let url = format!(
            "{}/{}?partNumber={part_number}&uploadId={}",
            self.core.endpoint,
            percent_encode_path(&to),
            percent_encode_path(upload_id),
        );
        let req = Request::put(url)
            .extension(Operation::Copy)
            .extension(ServiceOperation("UploadPartCopy"))
            .header(CONTENT_LENGTH, 0)
            .header("x-cos-copy-source", source)
            .header("x-cos-copy-source-range", range.to_header())
            .header("x-cos-copy-source-if-match", etag)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let req = self.core.sign(&self.ctx, req).await?;
        let resp = self.core.send(&self.ctx, req).await?;
        let (_, result) = parse_copy_response(
            ErrorContext::new(ServiceOperation("UploadPartCopy")).with_source_if_match(),
            resp,
        )?;
        Ok(oio::MultipartPart {
            part_number,
            etag: result.etag,
            checksum: None,
            size: range.size(),
        })
    }

    async fn complete_copy(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        let parts = parts
            .iter()
            .map(|part| CompleteMultipartUploadRequestPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
            })
            .collect();
        let req = self.core.cos_complete_multipart_upload_request(
            &self.to,
            upload_id,
            parts,
            false,
            Operation::Copy,
        )?;
        let req = self.core.sign(&self.ctx, req).await?;
        // COS starts the response before committing. A lost response can leave a committed
        // object whose upload ID no longer exists.
        let resp = self.core.send(&self.ctx, req).await.map_err(|err| {
            err.with_context("service_operation", "CompleteMultipartUpload")
                .set_permanent()
        })?;
        let (headers, result) = parse_copy_response(
            ErrorContext::new(ServiceOperation("CompleteMultipartUpload")),
            resp,
        )
        .map_err(Error::set_permanent)?;

        let mut metadata = MetadataBuilder::unknown();
        metadata.etag(result.etag);
        if let Some(version) = parse_header_to_str(&headers, constants::X_COS_VERSION_ID)?
            && version != "null"
        {
            metadata.version(version);
        }
        Ok(metadata.build())
    }

    async fn abort_copy(&self, upload_id: &str) -> Result<()> {
        let resp = self
            .core
            .cos_abort_multipart_upload(&self.ctx, &self.to, upload_id)
            .await?;
        match resp.status() {
            StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("AbortMultipartUpload")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use http::Response;
    use oio::MultipartCopy;

    use super::*;

    struct LostCompletionResponse;

    impl HttpTransport for LostCompletionResponse {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            assert_eq!(
                req.extensions().get::<ServiceOperation>().unwrap().0,
                "CompleteMultipartUpload"
            );
            Err(Error::new(ErrorKind::Unexpected, "synthetic connection loss").set_temporary())
        }
    }

    #[tokio::test]
    async fn copy_completion_does_not_retry_a_lost_response() {
        let core = Arc::new(CosCore {
            info: ServiceInfo::new(crate::COS_SCHEME, "/", "test"),
            capability: Capability::default(),
            bucket: "test".to_owned(),
            root: "/".to_owned(),
            endpoint: "https://cos.example.com".to_owned(),
            sign_ctx: opendal_core::raw::reqsign_context(),
            credential_provider: Arc::new(reqsign_tencent_cos::StaticCredentialProvider::new(
                "test", "test",
            )),
            request_signer: Arc::new(reqsign_tencent_cos::RequestSigner::new()),
            signers: HttpTransportCache::new(),
        });
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(LostCompletionResponse));
        let copier = CosCopier {
            core,
            ctx,
            from: "source".to_owned(),
            to: "target".to_owned(),
            args: OpCopy::default(),
            part_size: COS_COPY_MIN_PART_SIZE as u64,
            source: OnceLock::new(),
        };
        let parts = [oio::MultipartPart {
            part_number: 1,
            etag: "\"copied\"".to_owned(),
            checksum: None,
            size: Some(1),
        }];
        let error = copier
            .complete_copy("test-upload", &parts)
            .await
            .unwrap_err();
        assert_eq!(error.message(), "synthetic connection loss");
        assert!(error.to_string().contains("CompleteMultipartUpload"));
        assert!(!error.is_temporary());
    }
}
