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

use super::backend::*;
use super::core::{ErrorContext, parse_error};
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct SwiftReader {
    backend: SwiftBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl SwiftReader {
    pub(super) fn new(
        backend: SwiftBackend,
        ctx: OperationContext,
        path: &str,
        args: OpRead,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for SwiftReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .swift_read(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let headers = resp.headers();
                let mut meta = parse_into_metadata(path, headers)?.into_builder();
                let user_meta = parse_prefixed_headers(headers, "x-object-meta-");
                if !user_meta.is_empty() {
                    meta.user_metadata(user_meta);
                }
                (RpRead::new(meta.build()), resp.into_body())
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("GetObject")),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::stream;

    use super::*;
    use crate::core::{SwiftCore, TempUrlHashAlgorithm};
    use opendal_core::raw::oio::StreamRead;

    struct StaticReadTransport {
        status: StatusCode,
    }

    impl HttpTransport for StaticReadTransport {
        async fn fetch(&self, _req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let payload = Buffer::from("hello");
            let mut builder = Response::builder()
                .status(self.status)
                .header(http::header::CONTENT_LENGTH, payload.len())
                .header(http::header::CONTENT_TYPE, "text/plain")
                .header("x-object-meta-author", "opendal")
                .header("x-object-meta-env", "test");
            if self.status == StatusCode::PARTIAL_CONTENT {
                builder = builder.header(http::header::CONTENT_RANGE, "bytes 0-4/10");
            }
            builder
                .body(HttpBody::new(
                    stream::iter([Ok(payload)]),
                    Some("hello".len() as u64),
                ))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn open_preserves_object_user_metadata() {
        for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
            let core = Arc::new(SwiftCore {
                info: ServiceInfo::new(crate::SWIFT_SCHEME, "/", "container"),
                capability: Capability {
                    read: true,
                    ..Default::default()
                },
                root: "/".to_string(),
                endpoint: "http://127.0.0.1:8080/v1/AUTH_test".to_string(),
                container: "container".to_string(),
                token: "test-token".to_string(),
                temp_url_key: String::new(),
                temp_url_hash_algorithm: TempUrlHashAlgorithm::Sha256,
            });
            let backend = SwiftBackend { core };
            let ctx = OperationContext::default()
                .with_http_transport(HttpTransporter::new(StaticReadTransport { status }));
            let reader = SwiftReader::new(backend, ctx, "hello.txt", OpRead::default());

            let (rp, _stream) = reader.open(BytesRange::from(..)).await.unwrap();
            let user_meta = rp
                .metadata()
                .expect("read metadata must be populated")
                .user_metadata()
                .expect("user metadata must be populated");
            assert_eq!(user_meta.get("author"), Some("opendal"));
            assert_eq!(user_meta.get("env"), Some("test"));
        }
    }
}
