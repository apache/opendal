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

use std::collections::HashMap;

use super::backend::*;
use super::core::{ErrorContext, constants, parse_error};
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct B2Reader {
    backend: B2Backend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl B2Reader {
    pub(super) fn new(backend: B2Backend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for B2Reader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .download_file_by_name(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let mut meta = parse_into_metadata(path, resp.headers())?.into_builder();
                let user_meta: HashMap<String, String> =
                    parse_prefixed_headers(resp.headers(), constants::X_BZ_INFO_PREFIX)
                        .into_iter()
                        .map(|(k, v)| (k, percent_decode_path(&v)))
                        .collect();
                if !user_meta.is_empty() {
                    meta.user_metadata(user_meta);
                }
                (RpRead::new(meta.build()), resp.into_body())
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("DownloadFileByName")),
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
    use http::Request;
    use http::header::CONTENT_LENGTH;
    use http::header::CONTENT_RANGE;
    use opendal_core::raw::oio::StreamRead as _;

    use super::*;
    use crate::core::tests::test_core;

    struct ReaderTestTransport {
        status: StatusCode,
        body: &'static [u8],
        content_range: Option<&'static str>,
        with_user_meta: bool,
    }

    impl HttpTransport for ReaderTestTransport {
        async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
            let mut builder = Response::builder()
                .status(self.status)
                .header(CONTENT_LENGTH, self.body.len());
            if let Some(cr) = self.content_range {
                builder = builder.header(CONTENT_RANGE, cr);
            }
            if self.with_user_meta {
                builder = builder
                    .header("X-Bz-Info-Author", "Alice%20Smith")
                    .header("x-bz-info-env", "prod%2F1");
            }
            let buf = Buffer::from(self.body);
            let len = buf.len() as u64;
            builder
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn test_reader_open_parses_x_bz_info_user_metadata() {
        let backend = B2Backend {
            core: test_core("/"),
        };

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
                    with_user_meta: true,
                },
            ));
            let reader = B2Reader::new(backend.clone(), ctx, "hello.txt", OpRead::new());
            let (rp, _) = reader.open(range).await.unwrap();
            let meta = rp.into_metadata().expect("reader must return metadata");
            let user_meta = meta
                .user_metadata()
                .expect("X-Bz-Info-* headers must be parsed");
            assert_eq!(user_meta.get("author"), Some("Alice Smith"));
            assert_eq!(user_meta.get("env"), Some("prod/1"));
        }

        // Responses without X-Bz-Info-* headers leave user_metadata as None.
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            ReaderTestTransport {
                status: StatusCode::OK,
                body: b"hello",
                content_range: None,
                with_user_meta: false,
            },
        ));
        let reader = B2Reader::new(backend, ctx, "hello.txt", OpRead::new());
        let (rp, _) = reader.open(BytesRange::from(..)).await.unwrap();
        let meta = rp.into_metadata().expect("reader must return metadata");
        assert!(meta.user_metadata().is_none());
    }
}
