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
pub struct OssReader {
    backend: OssBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl OssReader {
    pub(super) fn new(
        backend: OssBackend,
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

impl oio::StreamRead for OssReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .oss_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(backend.core.parse_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("GetObject"))
                        .with_caller_condition(args.is_conditional()),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use http::header::CONTENT_LENGTH;
    use http::header::CONTENT_RANGE;
    use http::header::ETAG;
    use opendal_core::raw::oio::StreamRead;

    use super::*;
    use crate::core::constants::X_OSS_VERSION_ID;

    struct ReaderTestTransport {
        status: StatusCode,
        body: &'static [u8],
        content_range: Option<&'static str>,
    }

    impl HttpTransport for ReaderTestTransport {
        async fn fetch(&self, _: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let mut builder = Response::builder()
                .status(self.status)
                .header(CONTENT_LENGTH, self.body.len())
                .header(ETAG, "\"etag-1\"")
                .header(X_OSS_VERSION_ID, "v-42")
                .header("x-oss-meta-author", "alice")
                .header("x-oss-meta-env", "prod");
            if let Some(cr) = self.content_range {
                builder = builder.header(CONTENT_RANGE, cr);
            }
            let buf = Buffer::from(self.body);
            let len = buf.len() as u64;
            builder
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn test_backend() -> OssBackend {
        let service = OssBuilder::default()
            .bucket("example")
            .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
            .skip_signature()
            .disable_config_load()
            .build()
            .expect("OSS backend must build");
        (&service as &dyn std::any::Any)
            .downcast_ref::<OssBackend>()
            .expect("builder must produce OssBackend")
            .clone()
    }

    #[tokio::test]
    async fn reader_open_extracts_user_metadata_and_version_id() {
        for (status, range, content_range) in [
            (StatusCode::OK, BytesRange::from(..), None),
            (
                StatusCode::PARTIAL_CONTENT,
                BytesRange::new(0, Some(5)),
                Some("bytes 0-4/5"),
            ),
        ] {
            let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
                ReaderTestTransport {
                    status,
                    body: b"hello",
                    content_range,
                },
            ));
            let reader = OssReader::new(test_backend(), ctx, "hello.txt", OpRead::new());
            let (rp, _) = reader.open(range).await.unwrap();
            let meta = rp.into_metadata().expect("reader must return metadata");
            assert_eq!(meta.etag(), Some("\"etag-1\""));
            assert_eq!(meta.version(), Some("v-42"));
            let user_meta = meta
                .user_metadata()
                .expect("x-oss-meta-* headers must be parsed");
            assert_eq!(user_meta.get("author"), Some("alice"));
            assert_eq!(user_meta.get("env"), Some("prod"));
        }
    }

    #[tokio::test]
    async fn reader_open_maps_unauthorized_to_permission_denied() {
        let body = br#"<Error><Code>AccessDenied</Code><Message>Unauthorized</Message></Error>"#;
        let op = Operator::new(
            OssBuilder::default()
                .bucket("example")
                .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
                .skip_signature()
                .disable_config_load(),
        )
        .unwrap()
        .with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(
                ReaderTestTransport {
                    status: StatusCode::UNAUTHORIZED,
                    body,
                    content_range: None,
                },
            )),
        );

        let err = op
            .read("secret.txt")
            .await
            .expect_err("401 Unauthorized must fail read");
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(!err.is_temporary());
    }
}
