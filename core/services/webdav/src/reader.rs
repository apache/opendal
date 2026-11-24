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
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct WebdavReader {
    backend: WebdavBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl WebdavReader {
    pub(super) fn new(
        backend: WebdavBackend,
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

/// Build the read reply from response headers.
///
/// Servers may omit `Content-Length` (for example with chunked transfer
/// encoding). The metadata is optional in `RpRead`, so return an empty reply
/// instead of failing the read when the object size is unknown.
fn parse_read_reply(path: &str, status: StatusCode, headers: &http::HeaderMap) -> Result<RpRead> {
    // On 206 Partial Content, Content-Length is the length of the returned
    // range, not the full object size. Only Content-Range carries the full
    // object size, and only when its complete-length is known (not `*`).
    let size = match parse_content_range(headers)? {
        Some(value) => value.size(),
        None if status == StatusCode::PARTIAL_CONTENT => None,
        None => parse_content_length(headers)?,
    };
    if size.is_none() && !path.ends_with('/') {
        return Ok(RpRead::default());
    }
    Ok(RpRead::new(parse_into_metadata(path, headers)?))
}

/// Returns whether a `200 OK` response carrying the full representation
/// satisfies the requested `range` without dropping or shifting any bytes.
///
/// Some WebDAV servers (such as `dav-server`) respond with `200 OK` instead of
/// `206 Partial Content` when a prefix or suffix range covers the entire
/// resource (for example `bytes=-2048` or `bytes=0-2047` on a 1024-byte file).
/// That response is exact only when `Content-Length` is known and within the
/// requested range; otherwise a `200 OK` response to a ranged read must be
/// rejected to avoid silently returning bytes starting at offset 0.
fn ok_response_matches_range(range: BytesRange, content_length: Option<u64>) -> bool {
    match range {
        BytesRange::Range {
            offset: 0,
            size: None,
        } => true,
        BytesRange::Range {
            offset: 0,
            size: Some(size),
        }
        | BytesRange::Suffix { size } => content_length.is_some_and(|len| len <= size),
        BytesRange::Range { .. } => false,
    }
}

impl oio::StreamRead for WebdavReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .webdav_get(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK => {
                let content_length = parse_content_length(resp.headers())?;
                if !ok_response_matches_range(range, content_length) {
                    // RFC 9110 section 14.2: a server that ignores Range responds
                    // with 200 OK and the complete representation starting at byte
                    // 0. Drop the body without draining it so a large object is not
                    // buffered into memory.
                    let (part, _body) = resp.into_parts();
                    let err = Error::new(
                        ErrorKind::Unexpected,
                        "webdav server returned 200 OK for ranged read instead of 206 Partial Content",
                    )
                    .with_context("service_operation", "Get")
                    .with_context("range", range.to_header());
                    return Err(with_error_response_context(err, part));
                }
                (
                    parse_read_reply(path, status, resp.headers())?,
                    resp.into_body(),
                )
            }
            StatusCode::PARTIAL_CONTENT => (
                parse_read_reply(path, status, resp.headers())?,
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("Get"))
                        .with_if_match(args.if_match().is_some()),
                    Response::from_parts(part, buf),
                ));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use http::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE};

    use super::*;

    #[test]
    fn test_parse_read_reply_without_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "text/plain".parse().unwrap());

        let rp = parse_read_reply("file", StatusCode::OK, &headers).unwrap();
        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_parse_read_reply_with_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "5".parse().unwrap());

        let rp = parse_read_reply("file", StatusCode::OK, &headers).unwrap();
        assert_eq!(rp.metadata().unwrap().content_length(), 5);
    }

    #[test]
    fn test_parse_read_reply_with_content_range() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "2".parse().unwrap());
        headers.insert(CONTENT_RANGE, "bytes 0-1/10".parse().unwrap());

        let rp = parse_read_reply("file", StatusCode::PARTIAL_CONTENT, &headers).unwrap();
        assert_eq!(rp.metadata().unwrap().content_length(), 10);
    }

    #[test]
    fn test_parse_read_reply_partial_content_unknown_total_size() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "4".parse().unwrap());
        headers.insert(CONTENT_RANGE, "bytes 0-3/*".parse().unwrap());

        let rp = parse_read_reply("file", StatusCode::PARTIAL_CONTENT, &headers).unwrap();
        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_parse_read_reply_partial_content_without_content_range() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "4".parse().unwrap());

        let rp = parse_read_reply("file", StatusCode::PARTIAL_CONTENT, &headers).unwrap();
        assert!(rp.metadata().is_none());
    }

    #[test]
    fn test_parse_read_reply_rejects_invalid_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, "abc".parse().unwrap());

        assert!(parse_read_reply("file", StatusCode::OK, &headers).is_err());
    }

    struct Always200Transport {
        body: &'static [u8],
        include_content_length: bool,
        body_polled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl HttpTransport for Always200Transport {
        async fn fetch(&self, _req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let mut builder = Response::builder().status(StatusCode::OK);
            let body_len = if self.include_content_length {
                builder = builder.header(CONTENT_LENGTH, self.body.len());
                Some(self.body.len() as u64)
            } else {
                None
            };
            let data = bytes::Bytes::from_static(self.body);
            let body_polled = self.body_polled.clone();
            let mut yielded = false;
            let body = futures::stream::poll_fn(move |_cx| {
                body_polled.store(true, std::sync::atomic::Ordering::Relaxed);
                if yielded {
                    std::task::Poll::Ready(None)
                } else {
                    yielded = true;
                    std::task::Poll::Ready(Some(Ok(Buffer::from(data.clone()))))
                }
            });
            Ok(builder.body(HttpBody::new(body, body_len)).unwrap())
        }
    }

    struct RangeAwareTransport {
        content: &'static [u8],
    }

    impl HttpTransport for RangeAwareTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let total = self.content.len();
            let (status, slice, content_range) = match req
                .headers()
                .get(http::header::RANGE)
                .and_then(|v| v.to_str().ok())
            {
                None => (StatusCode::OK, self.content, None),
                Some(header) => {
                    let spec = header.strip_prefix("bytes=").expect("bytes= prefix");
                    let (start, end_inclusive) = if let Some(suffix) = spec.strip_prefix('-') {
                        let suffix: usize = suffix.parse().unwrap();
                        (total.saturating_sub(suffix), total - 1)
                    } else {
                        let (s, e) = spec.split_once('-').unwrap();
                        let start: usize = s.parse().unwrap();
                        let end: usize = if e.is_empty() {
                            total - 1
                        } else {
                            e.parse::<usize>().unwrap().min(total - 1)
                        };
                        (start, end)
                    };
                    (
                        StatusCode::PARTIAL_CONTENT,
                        &self.content[start..=end_inclusive],
                        Some(format!("bytes {start}-{end_inclusive}/{total}")),
                    )
                }
            };

            let mut builder = Response::builder()
                .status(status)
                .header(CONTENT_LENGTH, slice.len());
            if let Some(cr) = content_range {
                builder = builder.header(CONTENT_RANGE, cr);
            }
            let body = HttpBody::new(
                futures::stream::iter([Ok(Buffer::from(bytes::Bytes::copy_from_slice(slice)))]),
                Some(slice.len() as u64),
            );
            Ok(builder.body(body).unwrap())
        }
    }

    struct CountingTransport {
        requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl HttpTransport for CountingTransport {
        async fn fetch(&self, _req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            self.requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(futures::stream::empty(), Some(0)))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_ranged_read_rejects_200_ok() -> Result<()> {
        let ranged_cases = [
            BytesRange::from(0..4),
            BytesRange::from(4..),
            BytesRange::from(4..8),
            BytesRange::suffix(4),
        ];

        for include_content_length in [false, true] {
            let body_polled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let op =
                Operator::new(WebdavBuilder::default().endpoint("http://localhost:8080"))?
                    .with_context(OperationContext::new().with_http_transport(
                        HttpTransporter::new(Always200Transport {
                            body: b"0123456789",
                            include_content_length,
                            body_polled: body_polled.clone(),
                        }),
                    ));

            for range in ranged_cases {
                body_polled.store(false, std::sync::atomic::Ordering::Relaxed);
                let err = op
                    .read_with("file.txt")
                    .range(range)
                    .await
                    .expect_err(&format!(
                        "ranged read {range:?} (content_length={include_content_length}) must reject 200 OK"
                    ));
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert!(!err.is_temporary());
                assert!(
                    !body_polled.load(std::sync::atomic::Ordering::Relaxed),
                    "rejected 200 OK body must not be drained for {range:?}"
                );
            }

            let full = op.read("file.txt").await?;
            assert_eq!(full.to_bytes(), b"0123456789".as_slice());

            let from_zero = op.read_with("file.txt").range(0..).await?;
            assert_eq!(from_zero.to_bytes(), b"0123456789".as_slice());

            // Prefix/suffix ranges covering the full representation can accept 200 OK
            // only when Content-Length proves the body fits within the requested range.
            for covering_range in [
                BytesRange::from(0..10),
                BytesRange::suffix(10),
                BytesRange::suffix(20),
            ] {
                body_polled.store(false, std::sync::atomic::Ordering::Relaxed);
                if include_content_length {
                    let buf = op.read_with("file.txt").range(covering_range).await?;
                    assert_eq!(
                        buf.to_bytes(),
                        b"0123456789".as_slice(),
                        "{covering_range:?}"
                    );
                } else {
                    let err = op
                        .read_with("file.txt")
                        .range(covering_range)
                        .await
                        .expect_err(&format!(
                            "chunked 200 OK without Content-Length must reject {covering_range:?}"
                        ));
                    assert_eq!(err.kind(), ErrorKind::Unexpected, "{covering_range:?}");
                    assert!(
                        !body_polled.load(std::sync::atomic::Ordering::Relaxed),
                        "rejected chunked 200 OK body must not be drained for {covering_range:?}"
                    );
                }
            }

            // `.limit(20)` on a 10-byte file opens `0..20` and clamps to EOF when length is known.
            body_polled.store(false, std::sync::atomic::Ordering::Relaxed);
            if include_content_length {
                let buf = op.read_with("file.txt").limit(20).await?;
                assert_eq!(buf.to_bytes(), b"0123456789".as_slice());
            } else {
                let err = op
                    .read_with("file.txt")
                    .limit(20)
                    .await
                    .expect_err("chunked 200 OK without Content-Length must reject limit(20)");
                assert_eq!(err.kind(), ErrorKind::Unexpected);
                assert!(
                    !body_polled.load(std::sync::atomic::Ordering::Relaxed),
                    "rejected chunked 200 OK body must not be drained for limit(20)"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_ranged_read_accepts_206_partial_content() -> Result<()> {
        let op = Operator::new(WebdavBuilder::default().endpoint("http://localhost:8080"))?
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
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let op = Operator::new(WebdavBuilder::default().endpoint("http://localhost:8080"))?
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
            requests.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "overflowing ranges must fail before sending an HTTP request"
        );

        Ok(())
    }
}
