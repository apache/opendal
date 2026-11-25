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

use super::core::SloManifestEntry;
use super::core::parse_error;
use super::core::{ErrorContext, SwiftCore};
use opendal_core::raw::*;
use opendal_core::*;

pub struct SwiftWriter {
    core: Arc<SwiftCore>,
    ctx: OperationContext,
    op: OpWrite,
    path: String,
}

impl SwiftWriter {
    pub fn new(core: Arc<SwiftCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        SwiftWriter {
            core,
            ctx,
            op,
            path,
        }
    }

    fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
        let mut metadata = MetadataBuilder::unknown();

        if let Some(etag) = parse_etag(headers)? {
            metadata.etag(etag);
        }

        if let Some(last_modified) = parse_last_modified(headers)? {
            metadata.last_modified(last_modified);
        }

        Ok(metadata.build())
    }
}

impl oio::MultipartWrite for SwiftWriter {
    async fn write_once(&self, _size: u64, bs: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .swift_create_object(&self.ctx, &self.path, bs.len() as u64, &self.op, bs)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                let metadata = SwiftWriter::parse_metadata(resp.headers())?;
                Ok(metadata)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("CreateObject")),
                resp,
            )),
        }
    }

    async fn initiate_part(&self) -> Result<String> {
        // Swift SLO doesn't need a server-side initiate call.
        // Generate a local UUID as the upload ID to namespace the segments.
        Ok(uuid::Uuid::new_v4().to_string())
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<oio::MultipartPart> {
        let resp = self
            .core
            .swift_put_segment(&self.ctx, &self.path, upload_id, part_number, size, body)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                let etag = parse_etag(resp.headers())?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "ETag not present in segment upload response",
                        )
                    })?
                    .to_string();

                Ok(oio::MultipartPart {
                    part_number,
                    etag,
                    checksum: None,
                    size: Some(size),
                })
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadSegment")),
                resp,
            )),
        }
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        let manifest: Vec<SloManifestEntry> = parts
            .iter()
            .map(|part| {
                let segment = self
                    .core
                    .slo_segment_path(&self.path, upload_id, part.part_number);
                SloManifestEntry {
                    path: format!("{}/{}", self.core.container, segment),
                    etag: part.etag.trim_matches('"').to_string(),
                    size_bytes: part.size.unwrap_or(0),
                }
            })
            .collect();

        let resp = self
            .core
            .swift_put_slo_manifest(&self.ctx, &self.path, &manifest, &self.op)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                let metadata = SwiftWriter::parse_metadata(resp.headers())?;
                Ok(metadata)
            }
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("UploadManifest")),
                resp,
            )),
        }
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        self.core
            .swift_delete_slo(&self.ctx, &self.path, upload_id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use futures::stream;
    use http::HeaderMap;
    use http::Response;
    use http::header;

    use super::*;
    use crate::core::TempUrlHashAlgorithm;
    use opendal_core::raw::oio::MultipartWrite;

    #[derive(Default)]
    struct RecordingTransport {
        requests: Mutex<Vec<(String, HeaderMap)>>,
    }

    impl HttpTransport for RecordingTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests
                .lock()
                .unwrap()
                .push((req.uri().to_string(), req.headers().clone()));
            Response::builder()
                .status(StatusCode::CREATED)
                .header(header::ETAG, "\"manifest-etag\"")
                .body(HttpBody::new(stream::empty(), Some(0)))
                .map_err(new_request_build_error)
        }
    }

    struct ArcTransport(Arc<RecordingTransport>);

    impl HttpTransport for ArcTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            self.0.fetch(req).await
        }
    }

    fn build_writer(op: OpWrite) -> (SwiftWriter, Arc<RecordingTransport>) {
        let core = Arc::new(SwiftCore {
            info: ServiceInfo::new(crate::SWIFT_SCHEME, "/", "container"),
            capability: Capability {
                write: true,
                write_can_multi: true,
                write_with_content_type: true,
                write_with_content_disposition: true,
                write_with_content_encoding: true,
                write_with_cache_control: true,
                write_with_user_metadata: true,
                ..Default::default()
            },
            root: "/".to_string(),
            endpoint: "http://127.0.0.1:8080/v1/AUTH_test".to_string(),
            container: "container".to_string(),
            token: "test-token".to_string(),
            temp_url_key: String::new(),
            temp_url_hash_algorithm: TempUrlHashAlgorithm::Sha256,
        });
        let transport = Arc::new(RecordingTransport::default());
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(ArcTransport(transport.clone())));
        let writer = SwiftWriter::new(core, ctx, op, "report.csv".to_string());
        (writer, transport)
    }

    #[tokio::test]
    async fn complete_part_forwards_object_metadata_headers_without_overwriting_content_type() {
        let cap = Capability {
            write: true,
            write_can_multi: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: true,
            write_with_cache_control: true,
            write_with_user_metadata: true,
            ..Default::default()
        };
        let (op, _) = OpWrite::from_options(
            &cap,
            options::WriteOptions {
                content_type: Some("text/csv".to_string()),
                content_disposition: Some("attachment; filename=\"report.csv\"".to_string()),
                content_encoding: Some("gzip".to_string()),
                cache_control: Some("max-age=60".to_string()),
                user_metadata: Some(HashMap::from([(
                    "author".to_string(),
                    "opendal".to_string(),
                )])),
                ..Default::default()
            },
        )
        .unwrap();
        let (writer, transport) = build_writer(op);

        writer
            .complete_part(
                "upload-1",
                &[oio::MultipartPart {
                    part_number: 1,
                    etag: "\"abc123\"".to_string(),
                    checksum: None,
                    size: Some(12),
                }],
            )
            .await
            .unwrap();

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .0
                .ends_with("/container/report.csv?multipart-manifest=put")
        );

        let headers = &requests[0].1;
        assert_eq!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/csv")
        );
        assert_eq!(
            headers
                .get(header::CONTENT_DISPOSITION)
                .and_then(|v| v.to_str().ok()),
            Some("attachment; filename=\"report.csv\"")
        );
        assert_eq!(
            headers
                .get(header::CONTENT_ENCODING)
                .and_then(|v| v.to_str().ok()),
            Some("gzip")
        );
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("max-age=60")
        );
        assert_eq!(
            headers
                .get("x-object-meta-author")
                .and_then(|v| v.to_str().ok()),
            Some("opendal")
        );
    }

    #[tokio::test]
    async fn complete_part_omits_content_type_when_unset() {
        let (writer, transport) = build_writer(OpWrite::default());

        writer
            .complete_part(
                "upload-2",
                &[oio::MultipartPart {
                    part_number: 1,
                    etag: "\"abc123\"".to_string(),
                    checksum: None,
                    size: Some(12),
                }],
            )
            .await
            .unwrap();

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].1.contains_key(header::CONTENT_TYPE));
    }
}
