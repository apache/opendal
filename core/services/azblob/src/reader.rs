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
use super::core::ErrorContext;
use super::core::parse_blob_metadata;
use super::core::parse_error;
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct AzblobReader {
    backend: AzblobBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl AzblobReader {
    pub(super) fn new(
        backend: AzblobBackend,
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

impl oio::StreamRead for AzblobReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let error_ctx = ErrorContext::new(ServiceOperation("GetBlob"))
            .with_caller_condition(args.is_conditional());
        let resp = backend
            .core
            .azblob_get_blob(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_blob_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(error_ctx, Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use opendal_core::raw::oio::StreamRead;

    use super::*;

    #[derive(Clone)]
    struct StaticGetTransport;

    impl HttpTransport for StaticGetTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Bytes::from_static(b"hello");
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", "5")
                .header("etag", "\"0x8D123\"")
                .header("x-ms-version-id", "2024-01-01T00:00:00.0000000Z")
                .header("x-ms-meta-author", "alice")
                .header("x-ms-meta-env", "prod")
                .body(HttpBody::new(
                    futures::stream::iter(vec![Ok(Buffer::from(body))]),
                    Some(5),
                ))
                .expect("mock response must build"))
        }
    }

    #[tokio::test]
    async fn reader_open_extracts_version_id_and_user_metadata() {
        let backend = AzblobBuilder::default()
            .container("test")
            .endpoint("https://myaccount.blob.core.windows.net")
            .skip_signature()
            .build_backend()
            .expect("azblob backend must build");
        let ctx =
            OperationContext::new().with_http_transport(HttpTransporter::new(StaticGetTransport));
        let reader = AzblobReader::new(backend, ctx, "a.txt", OpRead::default());

        let (rp, _stream) = reader
            .open(BytesRange::from(..))
            .await
            .expect("reader open must succeed");
        let meta = rp.metadata().expect("metadata must be present");

        assert_eq!(meta.version(), Some("2024-01-01T00:00:00.0000000Z"));
        assert_eq!(meta.etag(), Some("\"0x8D123\""));
        let user_meta = meta.user_metadata().expect("user metadata must be parsed");
        assert_eq!(user_meta.get("author"), Some("alice"));
        assert_eq!(user_meta.get("env"), Some("prod"));
    }
}
