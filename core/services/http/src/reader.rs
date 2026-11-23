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
pub struct HttpReader {
    backend: HttpBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl HttpReader {
    pub(super) fn new(
        backend: HttpBackend,
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

impl oio::StreamRead for HttpReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend.core.http_get(&self.ctx, path, range, &args).await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK if range.is_full() => (
                read_metadata(path, status, resp.headers())?,
                resp.into_body(),
            ),
            StatusCode::OK => {
                // RFC 9110 section 14.2: a server that ignores Range responds
                // with 200 OK and the complete representation starting at byte
                // 0. Drop the body without draining it so a large object is not
                // buffered into memory.
                let (part, _body) = resp.into_parts();
                let err = Error::new(
                    ErrorKind::Unexpected,
                    "http server returned 200 OK for ranged read instead of 206 Partial Content",
                )
                .with_context("service_operation", "Get")
                .with_context("range", range.to_header());
                return Err(with_error_response_context(err, part));
            }
            StatusCode::PARTIAL_CONTENT => (
                read_metadata(path, status, resp.headers())?,
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("Get")),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

/// Build the read reply from a successful response's headers.
///
/// `Content-Length` is optional in HTTP: an HTTP/1.1 response can be framed
/// with `Transfer-Encoding: chunked` (RFC 9112 section 6.1) and an HTTP/2
/// response is framed by the end of the stream (RFC 9113 section 8.1.1), so
/// neither carries one. RFC 9110 section 8.6 only says a server *should* send
/// it when the length is known in advance.
///
/// `parse_into_metadata` requires a length for a file, because `Metadata` for a
/// file always carries its complete content length. That requirement is correct
/// for `stat`, but a read does not need it: `RpRead` holds an
/// `Option<Metadata>` precisely so a service can decline to report metadata
/// that the read did not observe natively. So when the response states no
/// length, report no metadata and let the body stream.
///
/// A caller that explicitly asks for `Reader::metadata()` then gets
/// `ErrorKind::Unsupported`, which is the documented answer for a service that
/// does not return metadata on read. Before this, the read itself failed.
fn read_metadata(path: &str, status: StatusCode, headers: &http::HeaderMap) -> Result<RpRead> {
    // On 206 Partial Content, Content-Length is the length of the returned
    // range, not the full object size. Only Content-Range carries the full
    // object size, and only when its complete-length is known (not `*`).
    // A malformed header still errors here rather than being silently treated
    // as absent.
    let content_length = match parse_content_range(headers)? {
        Some(value) => value.size(),
        None if status == StatusCode::PARTIAL_CONTENT => None,
        None => parse_content_length(headers)?,
    };

    match content_length {
        Some(_) => Ok(RpRead::new(parse_into_metadata(path, headers)?)),
        None => Ok(RpRead::default()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use futures::StreamExt;
    use http::HeaderMap;
    use http::HeaderValue;
    use http::Request;

    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                http::header::HeaderName::from_bytes(name.as_bytes()).expect("name must be valid"),
                HeaderValue::from_str(value).expect("value must be valid"),
            );
        }
        headers
    }

    #[test]
    fn test_read_metadata_with_content_length() {
        let rp = read_metadata(
            "file.txt",
            StatusCode::OK,
            &headers(&[("content-length", "12")]),
        )
        .expect("read must be accepted");

        let meta = rp.metadata().expect("metadata must be reported");
        assert_eq!(meta.content_length(), 12);
    }

    #[test]
    fn test_read_metadata_from_content_range() {
        // A ranged read states the full size in Content-Range, not
        // Content-Length, and that is still a length.
        let rp = read_metadata(
            "file.txt",
            StatusCode::PARTIAL_CONTENT,
            &headers(&[("content-length", "4"), ("content-range", "bytes 0-3/12")]),
        )
        .expect("read must be accepted");

        let meta = rp.metadata().expect("metadata must be reported");
        assert_eq!(meta.content_length(), 12);
    }

    #[test]
    fn test_read_metadata_partial_content_unknown_total_size_reports_none() {
        // On 206 with `Content-Range: bytes x-y/*`, Content-Length is only the
        // range length, not the full object size.
        let rp = read_metadata(
            "file.txt",
            StatusCode::PARTIAL_CONTENT,
            &headers(&[
                ("content-length", "4"),
                ("content-range", "bytes 0-3/*"),
                ("etag", "\"e1\""),
            ]),
        )
        .expect("read with unknown full length must be accepted");

        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_read_metadata_partial_content_without_content_range_reports_none() {
        let rp = read_metadata(
            "file.txt",
            StatusCode::PARTIAL_CONTENT,
            &headers(&[("content-length", "4")]),
        )
        .expect("206 without Content-Range must be accepted without mistaking range length for object size");

        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_read_metadata_without_content_length_reports_none() {
        // A chunked HTTP/1.1 response, and the shape an HTTP/2 response takes:
        // no length anywhere. The read must still be accepted.
        let rp = read_metadata(
            "file.txt",
            StatusCode::OK,
            &headers(&[("transfer-encoding", "chunked")]),
        )
        .expect("a response with no length must still be read");

        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_read_metadata_without_any_headers_reports_none() {
        let rp = read_metadata("file.txt", StatusCode::OK, &HeaderMap::new())
            .expect("a response with no headers must still be read");

        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_read_metadata_rejects_a_malformed_content_length() {
        // Absent is not the same as unparsable: a server that sends garbage
        // should still be reported rather than treated as length-free.
        let err = read_metadata(
            "file.txt",
            StatusCode::OK,
            &headers(&[("content-length", "not-a-number")]),
        )
        .expect_err("a malformed length must be an error");

        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[derive(Debug)]
    struct AlwaysOkTransport {
        content: &'static [u8],
        include_content_length: bool,
        body_polled: Arc<AtomicBool>,
    }

    impl HttpTransport for AlwaysOkTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let body_polled = self.body_polled.clone();
            let content = self.content;
            let stream = futures::stream::iter([Ok(Buffer::from(content))]).inspect(move |_| {
                body_polled.store(true, Ordering::Relaxed);
            });
            let mut builder = Response::builder().status(StatusCode::OK);
            let body_size = if self.include_content_length {
                builder = builder.header(http::header::CONTENT_LENGTH, content.len().to_string());
                Some(content.len() as u64)
            } else {
                builder = builder.header(http::header::TRANSFER_ENCODING, "chunked");
                None
            };
            Ok(builder
                .body(HttpBody::new(stream, body_size))
                .expect("response must build"))
        }
    }

    #[derive(Debug)]
    struct RangeAwareTransport {
        content: &'static [u8],
    }

    impl HttpTransport for RangeAwareTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let total = self.content.len();
            if let Some(raw_range) = req.headers().get(http::header::RANGE) {
                let range: BytesRange = raw_range
                    .to_str()
                    .expect("range header must be valid str")
                    .parse()
                    .expect("range header must parse");
                let slice_range = range
                    .to_content_range(total)
                    .expect("test range must be satisfiable");
                let slice = &self.content[slice_range.clone()];
                let content_range = format!(
                    "bytes {}-{}/{}",
                    slice_range.start,
                    slice_range.end - 1,
                    total
                );
                Ok(Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(http::header::CONTENT_LENGTH, slice.len().to_string())
                    .header(http::header::CONTENT_RANGE, content_range)
                    .body(HttpBody::new(
                        futures::stream::iter([Ok(Buffer::from(slice))]),
                        Some(slice.len() as u64),
                    ))
                    .expect("206 response must build"))
            } else {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(http::header::CONTENT_LENGTH, total.to_string())
                    .body(HttpBody::new(
                        futures::stream::iter([Ok(Buffer::from(self.content))]),
                        Some(total as u64),
                    ))
                    .expect("200 response must build"))
            }
        }
    }

    #[derive(Debug)]
    struct CountingTransport {
        requests: Arc<AtomicUsize>,
    }

    impl HttpTransport for CountingTransport {
        async fn fetch(&self, _req: Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests.fetch_add(1, Ordering::Relaxed);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_LENGTH, "10")
                .body(HttpBody::new(
                    futures::stream::iter([Ok(Buffer::from("0123456789"))]),
                    Some(10),
                ))
                .expect("response must build"))
        }
    }

    #[tokio::test]
    async fn test_ranged_read_rejects_200_ok() -> Result<()> {
        let ranged_cases = [
            BytesRange::from(4..),
            BytesRange::from(4..8),
            BytesRange::from(0..4),
            BytesRange::from(0..10),
            BytesRange::suffix(4),
        ];

        for include_content_length in [true, false] {
            let body_polled = Arc::new(AtomicBool::new(false));
            let op =
                Operator::new(HttpBuilder::default().endpoint("https://example.com"))?
                    .with_context(OperationContext::new().with_http_transport(
                        HttpTransporter::new(AlwaysOkTransport {
                            content: b"0123456789",
                            include_content_length,
                            body_polled: body_polled.clone(),
                        }),
                    ));

            for range in ranged_cases {
                body_polled.store(false, Ordering::Relaxed);
                let err = op
                    .read_with("file.txt")
                    .range(range)
                    .await
                    .expect_err(&format!(
                        "ranged read {range:?} (content_length={include_content_length}) must reject 200 OK"
                    ));
                assert_eq!(
                    err.kind(),
                    ErrorKind::Unexpected,
                    "{range:?} content_length={include_content_length}"
                );
                assert!(
                    !err.is_temporary(),
                    "{range:?} content_length={include_content_length}"
                );
                assert!(
                    !body_polled.load(Ordering::Relaxed),
                    "rejected 200 OK body must not be drained for {range:?} (content_length={include_content_length})"
                );
            }

            // Full reads (both `..` and `0..`) do not send a Range header and must accept 200 OK.
            let full = op.read("file.txt").await?;
            assert_eq!(full.to_bytes(), b"0123456789".as_slice());

            let from_zero = op.read_with("file.txt").range(0..).await?;
            assert_eq!(from_zero.to_bytes(), b"0123456789".as_slice());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_ranged_read_accepts_206_partial_content() -> Result<()> {
        let op = Operator::new(HttpBuilder::default().endpoint("https://example.com"))?
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(
                    RangeAwareTransport {
                        content: b"0123456789",
                    },
                )),
            );

        let cases: &[(BytesRange, &[u8])] = &[
            (BytesRange::from(..), b"0123456789"),
            (BytesRange::from(0..), b"0123456789"),
            (BytesRange::from(0..4), b"0123"),
            (BytesRange::from(4..), b"456789"),
            (BytesRange::from(4..8), b"4567"),
            (BytesRange::suffix(4), b"6789"),
        ];

        for &(range, expected) in cases {
            let buf = op.read_with("file.txt").range(range).await?;
            assert_eq!(buf.to_bytes(), expected, "{range:?}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_overflowing_range_never_sends_request() -> Result<()> {
        let requests = Arc::new(AtomicUsize::new(0));
        let op = Operator::new(HttpBuilder::default().endpoint("https://example.com"))?
            .with_context(
                OperationContext::new().with_http_transport(HttpTransporter::new(
                    CountingTransport {
                        requests: requests.clone(),
                    },
                )),
            );

        for range in [
            BytesRange::new(u64::MAX, Some(2)),
            BytesRange::new(2, Some(u64::MAX)),
        ] {
            let err = op
                .read_with("file.txt")
                .range(range)
                .await
                .expect_err(&format!("overflowing range {range:?} must fail"));
            assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied, "{range:?}");
            assert!(!err.is_temporary(), "{range:?}");
        }

        assert_eq!(
            requests.load(Ordering::Relaxed),
            0,
            "overflowing ranges must fail before sending an HTTP request"
        );

        for range in [
            BytesRange::new(u64::MAX, Some(2)),
            BytesRange::new(0, Some(0)),
        ] {
            let err = op
                .presign_read_options(
                    "file.txt",
                    Duration::from_secs(60),
                    options::ReadOptions {
                        range,
                        ..Default::default()
                    },
                )
                .await
                .expect_err(&format!(
                    "presign with unrepresentable range {range:?} must fail"
                ));
            assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied, "{range:?}");
        }

        Ok(())
    }
}
