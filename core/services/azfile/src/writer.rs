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

use super::core::parse_error;
use super::core::{AzfileCore, ErrorContext};
use opendal_core::raw::*;
use opendal_core::*;

pub type AzfileWriters = TwoWays<oio::OneShotWriter<AzfileWriter>, oio::AppendWriter<AzfileWriter>>;

pub struct AzfileWriter {
    core: Arc<AzfileCore>,
    ctx: OperationContext,
    op: OpWrite,
    path: String,
}

impl AzfileWriter {
    pub fn new(core: Arc<AzfileCore>, ctx: OperationContext, op: OpWrite, path: String) -> Self {
        AzfileWriter {
            core,
            ctx,
            op,
            path,
        }
    }

    async fn ensure_parent_dir_exists(&self) -> Result<()> {
        self.core
            .ensure_parent_dir_exists(&self.ctx, &self.path)
            .await
    }

    fn parse_metadata(headers: &http::HeaderMap) -> Result<Metadata> {
        let mut metadata = MetadataBuilder::unknown();

        if let Some(last_modified) = parse_last_modified(headers)? {
            metadata.last_modified(last_modified);
        }
        let etag = parse_etag(headers)?;
        if let Some(etag) = etag {
            metadata.etag(etag);
        }

        Ok(metadata.build())
    }
}

impl oio::OneShotWrite for AzfileWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        self.ensure_parent_dir_exists().await?;

        let size = bs.len();
        let resp = self
            .core
            .azfile_create_file(&self.ctx, &self.path, size, &self.op)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::CREATED => {}
            _ => {
                return Err(
                    parse_error(ErrorContext::new(ServiceOperation("CreateFile")), resp)
                        .with_operation("Backend::azfile_create_file"),
                );
            }
        }

        if size == 0 {
            let mut meta = AzfileWriter::parse_metadata(resp.headers())?.into_builder();
            meta.set_file(0);
            return Ok(meta.build());
        }

        let resp = self
            .core
            .azfile_update(&self.ctx, &self.path, size as u64, 0, bs)
            .await?;
        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::CREATED => {
                let mut meta = AzfileWriter::parse_metadata(resp.headers())?.into_builder();
                meta.set_file(size as u64);
                Ok(meta.build())
            }
            _ => Err(
                parse_error(ErrorContext::new(ServiceOperation("PutRange")), resp)
                    .with_operation("Backend::azfile_update"),
            ),
        }
    }
}

impl oio::AppendWrite for AzfileWriter {
    async fn offset(&self) -> Result<u64> {
        self.ensure_parent_dir_exists().await?;

        let resp = self
            .core
            .azfile_get_file_properties(&self.ctx, &self.path)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(parse_content_length(resp.headers())?.unwrap_or_default()),
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetFileProperties")),
                resp,
            )),
        }
    }

    async fn append(&self, offset: u64, size: u64, body: Buffer) -> Result<Metadata> {
        let resp = self
            .core
            .azfile_update(&self.ctx, &self.path, size, offset, body)
            .await?;

        let status = resp.status();
        let mut meta = AzfileWriter::parse_metadata(resp.headers())?.into_builder();
        meta.set_file(offset + size);
        match status {
            StatusCode::OK | StatusCode::CREATED => Ok(meta.build()),
            _ => Err(
                parse_error(ErrorContext::new(ServiceOperation("PutRange")), resp)
                    .with_operation("Backend::azfile_update"),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use http::{HeaderMap, Method, Request, Response, StatusCode};

    use super::*;
    use crate::Azfile;

    #[derive(Debug)]
    struct RecordedRequest {
        method: Method,
        uri: String,
        headers: HeaderMap,
        body: Bytes,
    }

    #[derive(Clone)]
    struct ScriptedTransport {
        responses: Arc<Mutex<VecDeque<Response<Bytes>>>>,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<Response<Bytes>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for ScriptedTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let (parts, body) = req.into_parts();
            self.requests.lock().unwrap().push(RecordedRequest {
                method: parts.method,
                uri: parts.uri.to_string(),
                headers: parts.headers,
                body: body.to_bytes(),
            });

            let resp = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected HTTP request");
            let (resp_parts, resp_body) = resp.into_parts();
            let size = resp_body.len() as u64;
            let stream_body = if resp_body.is_empty() {
                HttpBody::new(futures::stream::iter(std::iter::empty()), Some(0))
            } else {
                HttpBody::new(
                    futures::stream::iter(vec![Ok(Buffer::from(resp_body))]),
                    Some(size),
                )
            };
            Ok(Response::from_parts(resp_parts, stream_body))
        }
    }

    fn test_operator(transport: ScriptedTransport) -> Operator {
        Operator::new(
            Azfile::default()
                .endpoint("https://acc.file.core.windows.net")
                .share_name("share")
                .skip_signature(),
        )
        .expect("azfile operator must build")
        .with_context(OperationContext::new().with_http_transport(HttpTransporter::new(transport)))
    }

    #[tokio::test]
    async fn write_empty_file_skips_put_range_and_returns_create_metadata() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let transport = ScriptedTransport::new(vec![
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Bytes::new())
                    .unwrap(),
                Response::builder()
                    .status(StatusCode::CREATED)
                    .header("etag", "\"0x8DB1234\"")
                    .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                    .body(Bytes::new())
                    .unwrap(),
            ]);
            let requests = transport.requests.clone();
            let op = test_operator(transport);

            assert!(!op.info().capability().write_can_empty);

            let meta = op
                .write("empty.txt", "")
                .await
                .expect("0-byte write must succeed");
            assert_eq!(meta.mode(), EntryMode::FILE);
            assert_eq!(meta.content_length(), 0);
            assert_eq!(meta.etag(), Some("\"0x8DB1234\""));
            assert!(meta.last_modified().is_some());

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 2);
            assert_eq!(recorded[0].method, Method::HEAD);
            assert_eq!(
                recorded[0].uri,
                "https://acc.file.core.windows.net/share/?restype=directory"
            );
            assert_eq!(recorded[1].method, Method::PUT);
            assert_eq!(
                recorded[1].uri,
                "https://acc.file.core.windows.net/share/empty.txt"
            );
            assert_eq!(recorded[1].headers["x-ms-content-length"], "0");
            assert_eq!(recorded[1].headers["x-ms-type"], "file");
            assert_eq!(recorded[1].headers["content-length"], "0");
            assert!(recorded[1].body.is_empty());
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn write_non_empty_file_sends_create_and_put_range() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let transport = ScriptedTransport::new(vec![
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Bytes::new())
                    .unwrap(),
                Response::builder()
                    .status(StatusCode::CREATED)
                    .header("etag", "\"create-etag\"")
                    .body(Bytes::new())
                    .unwrap(),
                Response::builder()
                    .status(StatusCode::CREATED)
                    .header("etag", "\"put-range-etag\"")
                    .body(Bytes::new())
                    .unwrap(),
            ]);
            let requests = transport.requests.clone();
            let op = test_operator(transport);

            let meta = op
                .write("hello.txt", "hello")
                .await
                .expect("non-empty write must succeed");
            assert_eq!(meta.mode(), EntryMode::FILE);
            assert_eq!(meta.content_length(), 5);
            assert_eq!(meta.etag(), Some("\"put-range-etag\""));

            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 3);
            assert_eq!(recorded[1].method, Method::PUT);
            assert_eq!(
                recorded[1].uri,
                "https://acc.file.core.windows.net/share/hello.txt"
            );
            assert_eq!(recorded[1].headers["x-ms-content-length"], "5");

            assert_eq!(recorded[2].method, Method::PUT);
            assert_eq!(
                recorded[2].uri,
                "https://acc.file.core.windows.net/share/hello.txt?comp=range"
            );
            assert_eq!(recorded[2].headers["x-ms-write"], "update");
            assert_eq!(recorded[2].headers["content-length"], "5");
            assert_eq!(recorded[2].headers["range"], "bytes=0-4");
            assert_eq!(recorded[2].body.as_ref(), b"hello");
        })
        .await
        .expect("test timed out");
    }

    #[tokio::test]
    async fn write_empty_file_propagates_create_file_error() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let transport = ScriptedTransport::new(vec![
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Bytes::new())
                    .unwrap(),
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("x-ms-error-code", "InvalidAuthenticationInfo")
                    .body(Bytes::new())
                    .unwrap(),
            ]);
            let requests = transport.requests.clone();
            let op = test_operator(transport);

            let err = op
                .write("empty.txt", "")
                .await
                .expect_err("failed CreateFile must return error");
            assert_eq!(err.kind(), ErrorKind::PermissionDenied);
            assert!(!err.is_temporary());
            assert_eq!(requests.lock().unwrap().len(), 2);
        })
        .await
        .expect("test timed out");
    }
}
