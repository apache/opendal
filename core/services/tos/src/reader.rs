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
use crate::core::{ErrorContext, parse_error, tos_parse_into_metadata};
use http::Response;
use http::StatusCode;
use opendal_core::BytesRange;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::*;

/// Reader returned by this backend.
pub struct TosReader {
    backend: TosBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl TosReader {
    pub(super) fn new(
        backend: TosBackend,
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

impl oio::StreamRead for TosReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .tos_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(tos_parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
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
    use std::time::Duration;

    use futures::stream;
    use opendal_core::raw::oio::StreamRead;
    use opendal_core::{Buffer, Builder, HttpBody, HttpTransport, HttpTransporter};

    use super::*;

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
                .header(http::header::ETAG, "\"abc123\"")
                .header("x-tos-version-id", "ver-42")
                .header("x-tos-meta-author", "opendal")
                .header("x-tos-meta-env", "test");
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

    fn build_backend() -> TosBackend {
        let backend = crate::Tos::default()
            .bucket("test-bucket")
            .skip_signature()
            .build()
            .expect("build must succeed");
        (&backend as &dyn std::any::Any)
            .downcast_ref::<TosBackend>()
            .expect("build must return TosBackend")
            .clone()
    }

    #[tokio::test]
    async fn test_open_preserves_tos_metadata() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
                let backend = build_backend();
                let ctx = OperationContext::new()
                    .with_http_transport(HttpTransporter::new(StaticReadTransport { status }));
                let reader = TosReader::new(backend, ctx, "hello.txt", OpRead::default());

                let (rp, _stream) = reader.open(BytesRange::from(..)).await.unwrap();
                let meta = rp.metadata().expect("read metadata must be populated");
                assert_eq!(meta.etag(), Some("abc123"));
                assert_eq!(meta.version(), Some("ver-42"));
                let user_meta = meta
                    .user_metadata()
                    .expect("user metadata must be populated");
                assert_eq!(user_meta.get("author"), Some("opendal"));
                assert_eq!(user_meta.get("env"), Some("test"));
            }
        })
        .await
        .expect("test timed out");
    }
}
