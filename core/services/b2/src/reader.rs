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
use http::HeaderMap;
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
        let version = self.args.version();
        let (resp, operation) = if let Some(file_id) = version {
            let key = build_absolute_path(&backend.core.root, path);
            // Validate with the API that supplies both the bucket and the full key.
            // Each open, including a retry, uses its own authorization check.
            backend
                .core
                .get_data_version(&self.ctx, &key, file_id, Operation::Read)
                .await?;
            (
                backend
                    .core
                    .download_file_by_id(&self.ctx, file_id, range)
                    .await?,
                ServiceOperation("DownloadFileById"),
            )
        } else {
            (
                backend
                    .core
                    .download_file_by_name(&self.ctx, path, range, &self.args)
                    .await?,
                ServiceOperation("DownloadFileByName"),
            )
        };

        let status = resp.status();
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let headers = resp.headers();
                if let Some(file_id) = version {
                    let mut ids = headers.get_all("x-bz-file-id").iter();
                    if ids
                        .next()
                        .is_none_or(|id| id.as_bytes() != file_id.as_bytes())
                        || ids.next().is_some()
                    {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "B2 download did not return a single matching file ID",
                        ));
                    }
                }
                let metadata = parse_into_metadata(path, headers)?;
                if version.is_some() {
                    check_download_range(status, headers, range, metadata.content_length())?;
                }
                (RpRead::new(metadata), resp.into_body())
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(operation),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

fn check_download_range(
    status: StatusCode,
    headers: &HeaderMap,
    requested: BytesRange,
    length: u64,
) -> Result<()> {
    let expected = match requested {
        BytesRange::Range { offset, size } => {
            if !requested.is_full() && offset >= length {
                return Err(Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "range starts at or beyond the selected version's end",
                ));
            }
            offset..size.map_or(length, |size| offset.saturating_add(size).min(length))
        }
        BytesRange::Suffix { size } => length.saturating_sub(size)..length,
    };
    let content_range = parse_content_range(headers)?;
    let valid = match status {
        StatusCode::OK => content_range.is_none() && expected == (0..length),
        StatusCode::PARTIAL_CONTENT => {
            !requested.is_full()
                && !expected.is_empty()
                && content_range.and_then(|range| range.range()).as_ref() == Some(&expected)
                && parse_content_length(headers)?
                    .is_none_or(|size| size == expected.end - expected.start)
        }
        _ => unreachable!("range validation applies to successful downloads"),
    };
    if !valid {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "B2 download did not return the requested byte range",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream;
    use http::Request;
    use opendal_core::options::ReadOptions;

    use super::*;
    use crate::core::tests::test_core;

    struct Download {
        status: StatusCode,
        body: &'static str,
        content_range: Option<&'static str>,
        ids: &'static [&'static str],
    }

    type RecordedRequest = (ServiceOperation, Option<String>);

    struct Transport {
        identity: &'static str,
        response: Download,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl HttpTransport for Transport {
        async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
            let operation = if request.uri().path() == "/b2api/v2/b2_get_download_authorization" {
                ServiceOperation("GetDownloadAuthorization")
            } else {
                *request.extensions().get::<ServiceOperation>().unwrap()
            };
            let range = request
                .headers()
                .get("range")
                .map(|v| v.to_str().unwrap().to_string());
            self.requests.lock().unwrap().push((operation, range));
            let mut response = Response::builder();
            let body = match operation.0 {
                "GetFileInfo" => {
                    assert_eq!(
                        request.extensions().get::<Operation>(),
                        Some(&Operation::Read)
                    );
                    assert_eq!(
                        request.body().to_bytes().as_ref(),
                        br#"{"fileId":"old+id"}"#
                    );
                    self.identity
                }
                "DownloadFileById" => {
                    assert_eq!(
                        request.extensions().get::<Operation>(),
                        Some(&Operation::Read)
                    );
                    assert_eq!(request.uri().path(), "/b2api/v4/b2_download_file_by_id");
                    assert_eq!(request.uri().query(), Some("fileId=old%2Bid"));
                    response = response
                        .status(self.response.status)
                        .header("content-type", "text/plain");
                    for id in self.response.ids {
                        response = response.header("x-bz-file-id", *id);
                    }
                    if let Some(range) = self.response.content_range {
                        response = response.header("content-range", range);
                    }
                    self.response.body
                }
                "DownloadFileByName" => {
                    assert_eq!(
                        request.extensions().get::<Operation>(),
                        Some(&Operation::Read)
                    );
                    assert!(request.uri().path().ends_with("/root/file"));
                    response = response.header("content-type", "application/json");
                    "new content"
                }
                "GetDownloadAuthorization" => {
                    let params: serde_json::Value =
                        serde_json::from_slice(&request.body().to_bytes()).unwrap();
                    assert_eq!(params["fileNamePrefix"], "root/file");
                    assert_eq!(params["b2ContentType"], "text/plain");
                    r#"{"authorizationToken":"test-download"}"#
                }
                _ => panic!("unexpected service operation"),
            };
            let len = body.len() as u64;
            response
                .header("content-length", len)
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(body))]),
                    Some(len),
                ))
                .map_err(new_request_build_error)
        }
    }

    const IDENTITY: &str =
        r#"{"bucketId":"bucket-id-1","fileName":"root/file","fileId":"old+id","action":"upload"}"#;

    fn operator(transport: Transport) -> Operator {
        let mut core = test_core("/root/");
        let capability = &mut Arc::get_mut(&mut core).unwrap().capability;
        capability.read = true;
        capability.read_with_version = true;
        capability.read_with_suffix = true;
        capability.read_with_override_content_type = true;
        capability.presign = true;
        capability.presign_read = true;
        let context = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        Operator::from_parts(context, Arc::new(B2Backend { core }))
    }

    #[tokio::test]
    async fn selected_reader_pins_identity_and_validates_each_range() -> Result<()> {
        for (range, header, status, body, response_range, error) in [
            (
                BytesRange::default(),
                None,
                StatusCode::OK,
                "abcde",
                None,
                None,
            ),
            (
                (1..3).into(),
                Some("bytes=1-2"),
                StatusCode::PARTIAL_CONTENT,
                "bc",
                Some("bytes 1-2/5"),
                None,
            ),
            (
                BytesRange::suffix(2),
                Some("bytes=-2"),
                StatusCode::PARTIAL_CONTENT,
                "de",
                Some("bytes 3-4/5"),
                None,
            ),
            (
                BytesRange::suffix(8),
                Some("bytes=-8"),
                StatusCode::OK,
                "abcde",
                None,
                None,
            ),
            (
                (1..3).into(),
                Some("bytes=1-2"),
                StatusCode::OK,
                "abcde",
                None,
                Some(ErrorKind::Unexpected),
            ),
            (
                (1..3).into(),
                Some("bytes=1-2"),
                StatusCode::PARTIAL_CONTENT,
                "ab",
                Some("bytes 0-1/5"),
                Some(ErrorKind::Unexpected),
            ),
            (
                (5..6).into(),
                Some("bytes=5-5"),
                StatusCode::RANGE_NOT_SATISFIABLE,
                "{}",
                None,
                Some(ErrorKind::RangeNotSatisfied),
            ),
            (
                (5..6).into(),
                Some("bytes=5-5"),
                StatusCode::OK,
                "abcde",
                None,
                Some(ErrorKind::RangeNotSatisfied),
            ),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let op = operator(Transport {
                identity: IDENTITY,
                response: Download {
                    status,
                    body,
                    content_range: response_range,
                    ids: &["old+id"],
                },
                requests: requests.clone(),
            });
            let reader = op.reader_with("file").version("old+id").await?;
            assert!(requests.lock().unwrap().is_empty());
            for _ in 0..2 {
                let read = reader.read(range).await;
                if let Some(error) = error {
                    assert_eq!(read.unwrap_err().kind(), error);
                } else {
                    assert_eq!(read?.to_bytes().as_ref(), body.as_bytes());
                    let meta = reader.metadata().unwrap();
                    assert_eq!(meta.content_length(), 5);
                    assert_eq!(meta.content_type(), Some("text/plain"));
                    assert!(
                        meta.content_md5().is_none()
                            && meta.etag().is_none()
                            && meta.version().is_none()
                    );
                }
            }
            let requests = requests.lock().unwrap();
            assert_eq!(
                requests
                    .iter()
                    .map(|(op, range)| (op.0, range.as_deref()))
                    .collect::<Vec<_>>(),
                [
                    ("GetFileInfo", None),
                    ("DownloadFileById", header),
                    ("GetFileInfo", None),
                    ("DownloadFileById", header),
                ]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn selected_reads_reject_wrong_paths_and_unproven_downloads() -> Result<()> {
        for (path, ids, kind, count) in [
            ("other", &["old+id"][..], ErrorKind::NotFound, 1),
            ("file", &[][..], ErrorKind::Unexpected, 2),
            ("file", &["other"][..], ErrorKind::Unexpected, 2),
            ("file", &["old+id", "old+id"][..], ErrorKind::Unexpected, 2),
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let op = operator(Transport {
                identity: IDENTITY,
                response: Download {
                    status: StatusCode::OK,
                    body: "abcde",
                    content_range: None,
                    ids,
                },
                requests: requests.clone(),
            });
            assert_eq!(
                op.read_with(path)
                    .version("old+id")
                    .await
                    .unwrap_err()
                    .kind(),
                kind
            );
            assert_eq!(requests.lock().unwrap().len(), count);
        }
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_reads_and_presign_remain_by_name() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let op = operator(Transport {
            identity: IDENTITY,
            response: Download {
                status: StatusCode::OK,
                body: "",
                content_range: None,
                ids: &[],
            },
            requests: requests.clone(),
        });
        let expire = Duration::from_secs(60);
        let options = ReadOptions {
            range: (1..3).into(),
            override_content_type: Some("text/plain".into()),
            ..Default::default()
        };
        let selected = ReadOptions {
            version: Some("old+id".into()),
            ..options.clone()
        };
        assert_eq!(
            op.read_options("file", selected).await.unwrap_err().kind(),
            ErrorKind::Unsupported
        );
        let selected = ReadOptions {
            version: Some("old+id".into()),
            ..Default::default()
        };
        assert_eq!(
            op.presign_read_options("file", expire, selected)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(op.read("file").await?.to_bytes().as_ref(), b"new content");
        let signed = op.presign_read_options("file", expire, options).await?;
        assert!(
            signed.uri().path().starts_with("/file/")
                && signed.uri().path().ends_with("/root/file")
        );
        assert!(
            signed
                .uri()
                .query()
                .unwrap()
                .contains("b2ContentType=text/plain")
        );
        assert_eq!(signed.header()["range"], "bytes=1-2");
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .map(|(op, _)| op.0)
                .collect::<Vec<_>>(),
            ["DownloadFileByName", "GetDownloadAuthorization"]
        );
        Ok(())
    }
}
