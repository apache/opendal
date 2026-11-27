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
use super::core::constants::{X_GOOG_GENERATION, X_GOOG_HASH};
use super::core::{parse_crc32c, parse_error};
use http::StatusCode;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE};
use http::{HeaderMap, Response};
use opendal_core::raw::*;
use opendal_core::*;

const STORED_CONTENT_ENCODING: &str = "x-goog-stored-content-encoding";
const STORED_CONTENT_LENGTH: &str = "x-goog-stored-content-length";

/// Reader returned by this backend.
pub struct GcsReader {
    backend: GcsBackend,
    // StreamReader opens requests lazily, so keep the per-operation context
    // available for later range requests.
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl GcsReader {
    pub(super) fn new(
        backend: GcsBackend,
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

impl oio::StreamRead for GcsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let error_ctx = ErrorContext::new(ServiceOperation("GetObject"))
            .with_caller_condition(args.is_conditional());
        let mut resp = backend
            .core
            .gcs_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let headers = resp.headers_mut();
                // A chunked full response can omit Content-Length. Use the
                // stored length only when GCS sends the stored gzip representation.
                let check_stored_length = if status == StatusCode::OK
                    && range.is_full()
                    && !headers.contains_key(CONTENT_LENGTH)
                    && !headers.contains_key(CONTENT_RANGE)
                    && headers
                        .get(CONTENT_ENCODING)
                        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"gzip"))
                    && headers
                        .get(STORED_CONTENT_ENCODING)
                        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"gzip"))
                    && let Some(length) = headers.get(STORED_CONTENT_LENGTH).cloned()
                {
                    headers.insert(CONTENT_LENGTH, length);
                    true
                } else {
                    false
                };
                let mut metadata = parse_into_metadata(path, headers)?.into_builder();
                // GCS sends the JSON API etag unquoted, as stat and list report
                // it, and matches only that form in preconditions. Some
                // emulators quote it.
                if let Some(etag) = parse_etag(headers)? {
                    let etag = etag
                        .strip_prefix('"')
                        .and_then(|etag| etag.strip_suffix('"'))
                        .unwrap_or(etag);
                    metadata.etag(etag);
                }
                if let Some(generation) = parse_header_to_str(headers, X_GOOG_GENERATION)? {
                    metadata.version(generation);
                }
                if let Some(checksum) = stored_checksum(status, headers) {
                    metadata.checksum(checksum);
                }
                let metadata = metadata.build();
                let body = resp.into_body();
                let stream: Box<dyn oio::ReadStreamDyn> = if check_stored_length {
                    Box::new(StoredGzipStream {
                        body,
                        remaining: metadata.content_length(),
                    })
                } else {
                    Box::new(body)
                };
                Ok((RpRead::new(metadata), stream))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(error_ctx, Response::from_parts(part, buf)))
            }
        }
    }
}

fn stored_checksum(status: StatusCode, headers: &HeaderMap) -> Option<Checksum> {
    match status {
        StatusCode::OK if !headers.contains_key(CONTENT_RANGE) => {}
        StatusCode::PARTIAL_CONTENT => {
            let range = parse_content_range(headers).ok()??;
            if range.range()? != (0..range.size()?) {
                return None;
            }
        }
        _ => return None,
    }

    let stored = headers
        .get(STORED_CONTENT_ENCODING)
        .map_or(b"identity".as_slice(), |h| h.as_bytes());
    let received = headers
        .get(CONTENT_ENCODING)
        .map_or(b"identity".as_slice(), |h| h.as_bytes());
    // GCS can return the stored CRC32C after decompressing the object in transit.
    if !stored.eq_ignore_ascii_case(received) {
        return None;
    }

    headers
        .get_all(X_GOOG_HASH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .find_map(|hash| hash.trim().strip_prefix("crc32c=").and_then(parse_crc32c))
}

struct StoredGzipStream {
    body: HttpBody,
    remaining: u64,
}

impl oio::ReadStream for StoredGzipStream {
    async fn read(&mut self) -> Result<Buffer> {
        let chunk = oio::ReadStream::read(&mut self.body).await?;
        if chunk.is_empty() && self.remaining != 0 {
            return Err(
                Error::new(ErrorKind::Unexpected, "stored gzip response ended early")
                    .with_context("remaining", self.remaining)
                    .set_temporary(),
            );
        }
        self.remaining = self
            .remaining
            .checked_sub(chunk.len() as u64)
            .ok_or_else(|| {
                // Resuming at EOF cannot repair excess data.
                Error::new(
                    ErrorKind::Unexpected,
                    "stored gzip response exceeded its length",
                )
                .with_context("remaining", self.remaining)
                .with_context("chunk_size", chunk.len())
                .set_persistent()
            })?;
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::stream;
    use http::header::CONTENT_RANGE;
    use http::header::ETAG;
    use opendal_core::raw::oio::StreamRead;

    use reqsign_core::ProvideCredentialChain;
    use reqsign_google::RequestSigner;
    use reqsign_google::TokenCredentialProvider;

    use super::super::core::GcsCore;
    use super::*;

    #[test]
    fn test_crc32c_requires_complete_stored_representation() {
        let mut headers = HeaderMap::new();
        headers.insert(X_GOOG_HASH, "crc32c=5847cw==".parse().unwrap());
        headers.insert(STORED_CONTENT_ENCODING, "gzip".parse().unwrap());
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let expected = Checksum::new(ChecksumAlgo::Crc32c, 0xe7ce_3b73_u32.to_be_bytes());
        assert_eq!(
            stored_checksum(StatusCode::OK, &headers),
            Some(expected.clone())
        );

        headers.insert(CONTENT_RANGE, "bytes 0-63/64".parse().unwrap());
        assert_eq!(
            stored_checksum(StatusCode::PARTIAL_CONTENT, &headers),
            Some(expected)
        );

        headers.insert(CONTENT_RANGE, "bytes 5-12/64".parse().unwrap());
        assert_eq!(stored_checksum(StatusCode::PARTIAL_CONTENT, &headers), None);

        headers.remove(CONTENT_RANGE);
        headers.remove(CONTENT_ENCODING);
        assert_eq!(stored_checksum(StatusCode::OK, &headers), None);
    }

    struct EtagTransport(&'static str);

    impl HttpTransport for EtagTransport {
        async fn fetch(&self, _: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let body = Buffer::from("data");
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(CONTENT_RANGE, "bytes 0-3/4")
                .header(ETAG, self.0)
                .header(X_GOOG_GENERATION, "7")
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(4)))
                .map_err(new_request_build_error)
        }
    }

    fn reader_with(transport: impl HttpTransport) -> GcsReader {
        let backend = GcsBackend {
            core: Arc::new(GcsCore {
                info: ServiceInfo::new("gcs", "/", "test-bucket"),
                capability: Capability::default(),
                endpoint: "https://storage.googleapis.com".to_string(),
                bucket: "test-bucket".to_string(),
                root: "/".to_string(),
                sign_ctx: opendal_core::raw::reqsign_context(),
                credential_provider: Arc::new(
                    ProvideCredentialChain::new().push(TokenCredentialProvider::new("test-token")),
                ),
                request_signer: Arc::new(RequestSigner::new("storage")),
                signers: HttpTransportCache::new(),
                predefined_acl: None,
                default_storage_class: None,
                skip_signature: true,
            }),
        };
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(transport));
        GcsReader::new(backend, ctx, "object", OpRead::new())
    }

    async fn read_etag(header: &'static str) -> Option<String> {
        let reader = reader_with(EtagTransport(header));
        let (rp, _) = reader.open(BytesRange::new(0, Some(4))).await.unwrap();
        rp.into_metadata()
            .and_then(|meta| meta.etag().map(str::to_string))
    }

    struct ChunkedGzipObject {
        content_encoding: Option<&'static str>,
        body: &'static str,
    }

    impl HttpTransport for ChunkedGzipObject {
        async fn fetch(&self, _: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .header(STORED_CONTENT_ENCODING, "gzip")
                .header(STORED_CONTENT_LENGTH, "4");
            if let Some(encoding) = self.content_encoding {
                response = response.header(CONTENT_ENCODING, encoding);
            }
            response
                .body(HttpBody::new(
                    stream::iter([Ok(Buffer::from(self.body))]),
                    None,
                ))
                .map_err(new_request_build_error)
        }
    }

    #[tokio::test]
    async fn test_chunked_full_stored_gzip_uses_and_checks_stored_length() -> Result<()> {
        let reader = reader_with(ChunkedGzipObject {
            content_encoding: Some("gzip"),
            body: "data",
        });
        let (rp, mut body) = reader.open(BytesRange::default()).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 4);
        assert_eq!(body.read_all_dyn().await?.to_bytes(), "data");

        for (body, expected_error, retryable) in [
            ("abc", "stored gzip response ended early", true),
            ("abcde", "stored gzip response exceeded its length", false),
        ] {
            let reader = reader_with(ChunkedGzipObject {
                content_encoding: Some("gzip"),
                body,
            });
            let (_, mut stream) = reader.open(BytesRange::default()).await?;
            let error = stream.read_all_dyn().await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unexpected);
            assert_eq!(error.message(), expected_error);
            assert_eq!(error.is_temporary(), retryable);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_transcoded_full_read_does_not_use_stored_length() {
        let reader = reader_with(ChunkedGzipObject {
            content_encoding: None,
            body: "decoded",
        });
        let Err(error) = reader.open(BytesRange::default()).await else {
            panic!("the stored length does not describe the transcoded body");
        };
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert_eq!(
            error.message(),
            "HTTP response does not contain file content length"
        );
    }

    #[tokio::test]
    async fn test_read_etag_matches_json_api_form() {
        // GCS sends the JSON API resource etag unquoted, as stat and list report it.
        assert_eq!(
            read_etag("CKWasoTgyPkCEAE=").await.as_deref(),
            Some("CKWasoTgyPkCEAE=")
        );
        // Emulators such as fake-gcs-server quote it.
        assert_eq!(
            read_etag("\"CKWasoTgyPkCEAE=\"").await.as_deref(),
            Some("CKWasoTgyPkCEAE=")
        );
    }
}
