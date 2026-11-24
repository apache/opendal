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

use super::core::StartLargeFileResponse;
use super::core::UploadPartResponse;
use super::core::UploadResponse;
use super::core::parse_error;
use super::core::{B2Core, ErrorContext};
use opendal_core::raw::*;
use opendal_core::*;

pub type B2Writers = oio::MultipartWriter<B2Writer>;

pub struct B2Writer {
    core: Arc<B2Core>,
    ctx: OperationContext,

    op: OpWrite,
    path: String,
}

impl B2Writer {
    pub fn new(core: Arc<B2Core>, ctx: OperationContext, path: &str, op: OpWrite) -> Self {
        B2Writer {
            core,
            ctx,
            path: path.to_string(),
            op,
        }
    }

    pub fn parse_body_into_meta(path: &str, resp: UploadResponse) -> Metadata {
        let mut meta = if path.ends_with('/') {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::file(resp.content_length)
        };

        if let Some(md5) = resp.content_md5 {
            meta.content_md5(&md5);
        }

        if let Some(content_type) = resp.content_type {
            meta.content_type(&content_type);
        }

        meta.build()
    }
}

impl oio::MultipartWrite for B2Writer {
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .upload_file(&self.ctx, &self.path, Some(size), &self.op, body)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: UploadResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                let meta = Self::parse_body_into_meta(&self.path, result);

                Ok(meta)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadFile")),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        let resp = self
            .core
            .start_large_file(&self.ctx, &self.path, &self.op)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: StartLargeFileResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(result.file_id)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("StartLargeFile")),
                resp,
            )),
        }
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<oio::MultipartPart> {
        // B2 requires part number must between [1..=10000]
        let part_number = part_number + 1;

        let resp = self
            .core
            .upload_part(&self.ctx, upload_id, part_number, size, body)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: UploadPartResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(oio::MultipartPart {
                    etag: result.content_sha1,
                    part_number,
                    checksum: None,
                    size: None,
                })
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadPart")),
                resp,
            )),
        }
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        let part_sha1_array = parts
            .iter()
            .map(|p| {
                p.etag
                    .strip_prefix("unverified:")
                    .unwrap_or(&p.etag)
                    .to_string()
            })
            .collect();

        let resp = self
            .core
            .finish_large_file(&self.ctx, upload_id, part_sha1_array)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let result: UploadResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                let meta = Self::parse_body_into_meta(&self.path, result);

                Ok(meta)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("FinishLargeFile")),
                resp,
            )),
        }
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        let resp = self.core.cancel_large_file(&self.ctx, upload_id).await?;
        match resp.status() {
            // b2 returns code 200 if abort succeeds.
            StatusCode::OK => Ok(()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CancelLargeFile")),
                resp,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::stream;
    use http::Request;
    use http::Response;
    use opendal_core::raw::oio::MultipartWrite as _;

    use super::*;
    use crate::core::tests::test_core;

    struct FinishCaptureTransport {
        bodies: Arc<Mutex<Vec<String>>>,
    }

    impl HttpTransport for FinishCaptureTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = String::from_utf8_lossy(&req.into_body().to_bytes()).into_owned();
            self.bodies.lock().unwrap().push(body);

            let resp = Buffer::from(
                r#"{"contentLength":10485760,"contentMd5":"deadbeef","contentType":"application/octet-stream"}"#,
            );
            let len = resp.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(resp)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn test_complete_part_preserves_verified_and_strips_unverified_sha1() {
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let core = test_core("/");
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            FinishCaptureTransport {
                bodies: bodies.clone(),
            },
        ));
        let writer = B2Writer::new(core, ctx, "large.bin", OpWrite::new());

        let parts = vec![
            oio::MultipartPart {
                part_number: 1,
                etag: "unverified:2aae6c35c94fcfb415dbe95f408b9ce91ee846ed".to_string(),
                checksum: None,
                size: None,
            },
            oio::MultipartPart {
                part_number: 2,
                etag: "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3".to_string(),
                checksum: None,
                size: None,
            },
        ];

        let meta = writer.complete_part("4_zupload", &parts).await.unwrap();
        assert_eq!(meta.content_length(), 10485760);

        let recorded = bodies.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&recorded[0]).unwrap();
        assert_eq!(parsed["fileId"], "4_zupload");
        assert_eq!(
            parsed["partSha1Array"],
            serde_json::json!([
                "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"
            ])
        );
    }
}
