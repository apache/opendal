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
pub struct ObsReader {
    backend: ObsBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl ObsReader {
    pub(super) fn new(
        backend: ObsBackend,
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

impl oio::StreamRead for ObsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .obs_get_object(&self.ctx, path, range, &args)
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

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use http::header::CONTENT_LENGTH;
    use http::header::CONTENT_RANGE;
    use http::header::ETAG;
    use opendal_core::raw::oio::StreamRead;

    use super::*;
    use crate::core::constants::X_OBS_VERSION_ID;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    struct ReaderTestTransport {
        status: StatusCode,
        body: &'static [u8],
        content_range: Option<&'static str>,
    }

    impl HttpTransport for ReaderTestTransport {
        async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
            let buf = Buffer::from(Bytes::from_static(self.body));
            let size = buf.len() as u64;
            let mut builder = Response::builder()
                .status(self.status)
                .header(CONTENT_LENGTH, size)
                .header(ETAG, "\"etag-1\"")
                .header(X_OBS_VERSION_ID, "ver-42")
                .header("x-obs-meta-author", "alice")
                .header("x-obs-meta-env", "prod");
            if let Some(cr) = self.content_range {
                builder = builder.header(CONTENT_RANGE, cr);
            }
            Ok(builder
                .body(HttpBody::new(stream::iter(vec![Ok(buf)]), Some(size)))
                .expect("mock response must build"))
        }
    }

    fn test_reader(transport: ReaderTestTransport) -> ObsReader {
        let service = ObsBuilder::default()
            .bucket("example-bucket")
            .endpoint("https://obs.cn-north-4.myhuaweicloud.com")
            .skip_signature()
            .disable_config_load()
            .build()
            .expect("obs backend must build");
        let backend = (&service as &dyn std::any::Any)
            .downcast_ref::<ObsBackend>()
            .expect("builder must produce ObsBackend")
            .clone();
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        ObsReader::new(backend, ctx, "dir/file.txt", OpRead::default())
    }

    #[tokio::test]
    async fn open_preserves_user_metadata_and_version_id() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            for (status, content_range, range) in [
                (StatusCode::OK, None, BytesRange::from(..)),
                (
                    StatusCode::PARTIAL_CONTENT,
                    Some("bytes 0-3/10"),
                    BytesRange::new(0, Some(4)),
                ),
            ] {
                let reader = test_reader(ReaderTestTransport {
                    status,
                    body: b"data",
                    content_range,
                });

                let (rp, _stream) = reader.open(range).await.expect("open must succeed");
                let meta = rp.into_metadata().expect("reader must return metadata");

                assert_eq!(meta.etag(), Some("\"etag-1\""));
                assert_eq!(meta.version(), Some("ver-42"));
                let user_meta = meta
                    .user_metadata()
                    .expect("x-obs-meta-* headers must be parsed");
                assert_eq!(user_meta.get("author"), Some("alice"));
                assert_eq!(user_meta.get("env"), Some("prod"));
            }
        })
        .await
        .expect("test must not time out");
    }

    #[tokio::test]
    async fn open_maps_416_and_401_errors() {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let err_416 = test_reader(ReaderTestTransport {
                status: StatusCode::RANGE_NOT_SATISFIABLE,
                body: b"<Error><Code>InvalidRange</Code><Message>bad range</Message></Error>",
                content_range: None,
            })
            .open(BytesRange::new(100, Some(10)))
            .await
            .err()
            .expect("416 must fail");
            assert_eq!(err_416.kind(), ErrorKind::RangeNotSatisfied);
            assert!(!err_416.is_temporary());

            let err_401 = test_reader(ReaderTestTransport {
                status: StatusCode::UNAUTHORIZED,
                body: b"<Error><Code>InvalidAccessKeyId</Code><Message>unauthorized</Message></Error>",
                content_range: None,
            })
            .open(BytesRange::from(..))
            .await
            .err()
            .expect("401 must fail");
            assert_eq!(err_401.kind(), ErrorKind::PermissionDenied);
            assert!(!err_401.is_temporary());
        })
        .await
        .expect("test must not time out");
    }
}
