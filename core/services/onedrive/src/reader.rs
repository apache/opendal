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
use super::core::parse_error;
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct OnedriveReader {
    backend: OnedriveBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl OnedriveReader {
    pub(super) fn new(
        backend: OnedriveBackend,
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

impl oio::StreamRead for OnedriveReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let range = match range {
            BytesRange::Suffix { size } => {
                let content_length = backend
                    .core
                    .onedrive_stat(&self.ctx, path, OpStat::default())
                    .await?
                    .content_length();
                BytesRange::new(content_length.saturating_sub(size), None)
            }
            range => range,
        };
        let response = backend
            .core
            .onedrive_get_content(&self.ctx, path, range, &args)
            .await?;
        let (rp, stream) = match response.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, response.headers())?),
                response.into_body(),
            ),
            _ => {
                let (part, mut body) = response.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::stream;
    use http::Request;
    use http::Response;
    use http::StatusCode;
    use http::header;

    use super::*;
    use crate::core::OneDriveCore;
    use crate::core::OneDriveSigner;

    const FILE_STAT_RESPONSE: &str = r#"{"id":"1","name":"file.txt","lastModifiedDateTime":"2026-01-01T00:00:00Z","eTag":"aTag","size":1024,"parentReference":{"path":"/drive/root:","driveId":"d","id":"p"},"file":{"mimeType":"text/plain"}}"#;

    type RecordedRequest = (String, Option<String>);
    type RecordedRequests = Arc<Mutex<Vec<RecordedRequest>>>;

    #[derive(Clone)]
    struct MockHttpTransport {
        requests: RecordedRequests,
    }

    impl HttpTransport for MockHttpTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let uri = req.uri().to_string();
            let range = req
                .headers()
                .get(header::RANGE)
                .map(|value| value.to_str().unwrap().to_string());
            self.requests.lock().unwrap().push((uri.clone(), range));

            let (status, body) = if uri.ends_with(":/content") {
                match self.requests.lock().unwrap().last().unwrap().1.as_deref() {
                    Some("bytes=-2048") => (StatusCode::RANGE_NOT_SATISFIABLE, ""),
                    Some("bytes=0-") => (StatusCode::OK, "content"),
                    _ => (StatusCode::BAD_REQUEST, ""),
                }
            } else {
                (StatusCode::OK, FILE_STAT_RESPONSE)
            };

            let bytes = Bytes::from_static(body.as_bytes());
            let size = bytes.len() as u64;
            Ok(Response::builder()
                .status(status)
                .header(header::CONTENT_LENGTH, size)
                .body(HttpBody::new(
                    stream::iter(vec![Ok(Buffer::from(bytes))]),
                    Some(size),
                ))
                .unwrap())
        }
    }

    fn test_backend() -> OnedriveBackend {
        let mut signer = OneDriveSigner::new();
        signer.access_token = "token".to_string();
        signer.expires_in = Timestamp::MAX;

        OnedriveBackend {
            core: Arc::new(OneDriveCore {
                info: ServiceInfo::new("onedrive", "/", ""),
                capability: Capability::default(),
                root: "/".to_string(),
                signer: Arc::new(asyncband::mutex::Mutex::new(signer)),
            }),
        }
    }

    #[tokio::test]
    async fn read_suffix_larger_than_file_requests_full_content() {
        let transport = MockHttpTransport {
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let requests = transport.requests.clone();
        let ctx = OperationContext::new().with_http_transport(HttpTransporter::new(transport));
        let backend = test_backend();

        let reader = Service::read(&backend, &ctx, "file.txt", OpRead::default()).unwrap();
        oio::Read::open(&reader, BytesRange::suffix(2048))
            .await
            .unwrap();

        assert_eq!(
            *requests.lock().unwrap(),
            vec![
                (format!("{}:/file.txt", OneDriveCore::DRIVE_ROOT_URL), None,),
                (
                    format!("{}:/file.txt:/content", OneDriveCore::DRIVE_ROOT_URL),
                    Some("bytes=0-".to_string()),
                ),
            ]
        );
    }
}
