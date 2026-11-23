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

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::*;

struct ScriptedOpen {
    rp: RpRead,
    chunks: VecDeque<Result<Bytes>>,
}

struct ScriptedStream {
    chunks: VecDeque<Result<Bytes>>,
}

impl oio::ReadStream for ScriptedStream {
    async fn read(&mut self) -> Result<Buffer> {
        match self.chunks.pop_front() {
            Some(Ok(bytes)) => Ok(Buffer::from(bytes)),
            Some(Err(err)) => Err(err),
            None => Ok(Buffer::new()),
        }
    }
}

struct ScriptedReader {
    script: Mutex<VecDeque<ScriptedOpen>>,
    opens: Arc<Mutex<Vec<BytesRange>>>,
}

impl ScriptedReader {
    fn new(opens: Arc<Mutex<Vec<BytesRange>>>, script: Vec<ScriptedOpen>) -> Self {
        Self {
            script: Mutex::new(script.into()),
            opens,
        }
    }
}

impl oio::StreamRead for ScriptedReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.opens.lock().unwrap().push(range);
        let next = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected open call");
        Ok((
            next.rp,
            Box::new(ScriptedStream {
                chunks: next.chunks,
            }),
        ))
    }
}

fn transient_err() -> Error {
    Error::new(ErrorKind::Unexpected, "transient stream error").set_temporary()
}

fn retry_reader(
    reader: ScriptedReader,
) -> RetryReader<oio::StreamReader<ScriptedReader>, DefaultRetryInterceptor> {
    RetryReader::new(
        oio::StreamReader::new(reader),
        RetryLayer::new()
            .with_min_delay(Duration::ZERO)
            .with_max_delay(Duration::ZERO)
            .policy,
    )
}

#[tokio::test]
async fn reopen_rejects_changed_identity_and_stays_terminal() -> Result<()> {
    let ts1 = Timestamp::from_second(1_700_000_000).unwrap();
    let ts2 = Timestamp::from_second(1_700_000_010).unwrap();
    let base_meta = || {
        let mut b = MetadataBuilder::file(8);
        b.version("v1").etag("\"e1\"").last_modified(ts1);
        b
    };

    let cases: Vec<(&str, RpRead)> = vec![
        (
            "version changed",
            RpRead::new({
                let mut b = base_meta();
                b.version("v2");
                b.build()
            }),
        ),
        (
            "etag changed",
            RpRead::new({
                let mut b = base_meta();
                b.etag("\"e2\"");
                b.build()
            }),
        ),
        (
            "last_modified changed",
            RpRead::new({
                let mut b = base_meta();
                b.last_modified(ts2);
                b.build()
            }),
        ),
        (
            "content_length grew",
            RpRead::new({
                let mut b = MetadataBuilder::file(12);
                b.version("v1").etag("\"e1\"").last_modified(ts1);
                b.build()
            }),
        ),
        (
            "content_length shrank",
            RpRead::new({
                let mut b = MetadataBuilder::file(6);
                b.version("v1").etag("\"e1\"").last_modified(ts1);
                b.build()
            }),
        ),
        ("metadata disappeared", RpRead::default()),
    ];

    for (label, second_rp) in cases {
        let opens = Arc::new(Mutex::new(Vec::new()));
        let reader = retry_reader(ScriptedReader::new(
            opens.clone(),
            vec![
                ScriptedOpen {
                    rp: RpRead::new(base_meta().build()),
                    chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
                },
                ScriptedOpen {
                    rp: second_rp,
                    chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
                },
            ],
        ));

        let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
        assert_eq!(
            oio::ReadStream::read(&mut stream).await?.to_bytes(),
            Bytes::from_static(b"AAAA"),
            "case={label}"
        );
        let err = oio::ReadStream::read(&mut stream)
            .await
            .expect_err("reopen with changed identity must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch, "case={label}");
        assert!(!err.is_temporary(), "case={label}");
        assert_eq!(opens.lock().unwrap().len(), 2, "case={label}");

        // A later read call must stay failed without reopening again.
        let err2 = oio::ReadStream::read(&mut stream)
            .await
            .expect_err("mismatched stream must remain failed");
        assert_eq!(err2.kind(), ErrorKind::ConditionNotMatch, "case={label}");
        assert_eq!(opens.lock().unwrap().len(), 2, "case={label}");
    }

    Ok(())
}

#[tokio::test]
async fn reopen_rejects_identity_change_before_first_byte() -> Result<()> {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut first = MetadataBuilder::file(8);
    first.etag("\"e1\"");
    let mut second = MetadataBuilder::file(8);
    second.etag("\"e2\"");

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: RpRead::new(first.build()),
                chunks: [Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::new(second.build()),
                chunks: [Ok(Bytes::from_static(b"BBBBBBBB"))].into(),
            },
        ],
    ));

    let (rp, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    assert_eq!(rp.metadata().and_then(Metadata::etag), Some("\"e1\""));
    let err = oio::ReadStream::read(&mut stream)
        .await
        .expect_err("bytes from a newer object must not be returned under the initial RpRead");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    assert!(!err.is_temporary());
    assert_eq!(opens.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn reopen_resumes_when_identity_matches() -> Result<()> {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut meta = MetadataBuilder::file(8);
    meta.version("v1").etag("\"e1\"");
    let rp = RpRead::new(meta.build());

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: rp.clone(),
                chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp,
                chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    let data = oio::ReadStream::read_all(&mut stream).await?.to_bytes();
    assert_eq!(data, Bytes::from_static(b"AAAABBBB"));
    assert_eq!(
        *opens.lock().unwrap(),
        vec![BytesRange::default(), BytesRange::new(4, Some(4))]
    );
    Ok(())
}

#[tokio::test]
async fn reopen_adopts_identity_after_empty_initial_rp_and_rejects_later_change() -> Result<()> {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut second = MetadataBuilder::file(12);
    second.etag("\"e1\"");
    let mut third = MetadataBuilder::file(12);
    third.etag("\"e2\"");

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: RpRead::default(),
                chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::new(second.build()),
                chunks: [Ok(Bytes::from_static(b"BBBB")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::new(third.build()),
                chunks: [Ok(Bytes::from_static(b"CCCC"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    assert_eq!(
        oio::ReadStream::read(&mut stream).await?.to_bytes(),
        Bytes::from_static(b"AAAA")
    );
    assert_eq!(
        oio::ReadStream::read(&mut stream).await?.to_bytes(),
        Bytes::from_static(b"BBBB")
    );
    let err = oio::ReadStream::read(&mut stream)
        .await
        .expect_err("identity change between second and third open must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    assert_eq!(opens.lock().unwrap().len(), 3);
    Ok(())
}

#[tokio::test]
async fn reopen_rejects_midstream_weak_etag_without_version() -> Result<()> {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut meta = MetadataBuilder::file(8);
    meta.etag("W/\"v1\"");
    let rp = RpRead::new(meta.build());

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: rp.clone(),
                chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp,
                chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    assert_eq!(
        oio::ReadStream::read(&mut stream).await?.to_bytes(),
        Bytes::from_static(b"AAAA")
    );
    let err = oio::ReadStream::read(&mut stream)
        .await
        .expect_err("mid-stream resume with weak etag and no version must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    assert!(!err.is_temporary());
    assert_eq!(opens.lock().unwrap().len(), 1);

    let err2 = oio::ReadStream::read(&mut stream)
        .await
        .expect_err("failed stream must remain terminal");
    assert_eq!(err2.kind(), ErrorKind::ConditionNotMatch);
    assert_eq!(opens.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn reopen_allows_pre_first_byte_retry_with_weak_etag() -> Result<()> {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut meta = MetadataBuilder::file(8);
    meta.etag("W/\"v1\"");
    let rp = RpRead::new(meta.build());

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: rp.clone(),
                chunks: [Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp,
                chunks: [Ok(Bytes::from_static(b"BBBBBBBB"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    let data = oio::ReadStream::read_all(&mut stream).await?.to_bytes();
    assert_eq!(data, Bytes::from_static(b"BBBBBBBB"));
    assert_eq!(opens.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn reopen_with_weak_etag_and_version_requires_matching_version_and_no_contradiction()
-> Result<()> {
    let base = || {
        let mut b = MetadataBuilder::file(8);
        b.version("ver1").etag("W/\"v1\"");
        b
    };

    // Matching strong version allows mid-stream resume even with a weak ETag.
    {
        let opens = Arc::new(Mutex::new(Vec::new()));
        let rp = RpRead::new(base().build());
        let reader = retry_reader(ScriptedReader::new(
            opens.clone(),
            vec![
                ScriptedOpen {
                    rp: rp.clone(),
                    chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
                },
                ScriptedOpen {
                    rp,
                    chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
                },
            ],
        ));

        let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
        let data = oio::ReadStream::read_all(&mut stream).await?.to_bytes();
        assert_eq!(data, Bytes::from_static(b"AAAABBBB"));
        assert_eq!(opens.lock().unwrap().len(), 2);
    }

    // Weak ETag changing still rejects even if version stays identical.
    {
        let opens = Arc::new(Mutex::new(Vec::new()));
        let mut changed = base();
        changed.etag("W/\"v2\"");
        let reader = retry_reader(ScriptedReader::new(
            opens.clone(),
            vec![
                ScriptedOpen {
                    rp: RpRead::new(base().build()),
                    chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
                },
                ScriptedOpen {
                    rp: RpRead::new(changed.build()),
                    chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
                },
            ],
        ));

        let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
        assert_eq!(
            oio::ReadStream::read(&mut stream).await?.to_bytes(),
            Bytes::from_static(b"AAAA")
        );
        let err = oio::ReadStream::read(&mut stream)
            .await
            .expect_err("changed weak etag must fail even when version matches");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(opens.lock().unwrap().len(), 2);
    }

    // Dropping version and leaving only weak ETag rejects.
    {
        let opens = Arc::new(Mutex::new(Vec::new()));
        let mut dropped = MetadataBuilder::file(8);
        dropped.etag("W/\"v1\"");
        let reader = retry_reader(ScriptedReader::new(
            opens.clone(),
            vec![
                ScriptedOpen {
                    rp: RpRead::new(base().build()),
                    chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
                },
                ScriptedOpen {
                    rp: RpRead::new(dropped.build()),
                    chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
                },
            ],
        ));

        let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
        assert_eq!(
            oio::ReadStream::read(&mut stream).await?.to_bytes(),
            Bytes::from_static(b"AAAA")
        );
        let err = oio::ReadStream::read(&mut stream)
            .await
            .expect_err("dropping strong version with weak etag must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert_eq!(opens.lock().unwrap().len(), 2);
    }

    Ok(())
}

#[tokio::test]
async fn reopen_rejects_weak_etag_after_prior_progress_across_multiple_reopens() -> Result<()> {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut weak = MetadataBuilder::file(8);
    weak.etag("W/\"v1\"");

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: RpRead::default(),
                chunks: [Ok(Bytes::from_static(b"AA")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::default(),
                chunks: [Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::new(weak.build()),
                chunks: [Ok(Bytes::from_static(b"BBBBBB"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    assert_eq!(
        oio::ReadStream::read(&mut stream).await?.to_bytes(),
        Bytes::from_static(b"AA")
    );
    let err = oio::ReadStream::read(&mut stream)
        .await
        .expect_err("weak etag after prior yielded bytes must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    assert_eq!(opens.lock().unwrap().len(), 3);
    Ok(())
}

#[tokio::test]
async fn reopen_allows_omitting_last_modified_when_strong_etag_matches() -> Result<()> {
    let ts = Timestamp::from_second(1_700_000_000).unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut first = MetadataBuilder::file(8);
    first.etag("\"e1\"").last_modified(ts);
    let mut second = MetadataBuilder::file(8);
    second.etag("\"e1\"");

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: RpRead::new(first.build()),
                chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::new(second.build()),
                chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    let data = oio::ReadStream::read_all(&mut stream).await?.to_bytes();
    assert_eq!(data, Bytes::from_static(b"AAAABBBB"));
    assert_eq!(opens.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn reopen_rejects_dropping_strong_etag_when_only_length_and_last_modified_remain()
-> Result<()> {
    let ts = Timestamp::from_second(1_700_000_000).unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let mut first = MetadataBuilder::file(8);
    first.etag("\"e1\"").last_modified(ts);
    let mut second = MetadataBuilder::file(8);
    second.last_modified(ts);

    let reader = retry_reader(ScriptedReader::new(
        opens.clone(),
        vec![
            ScriptedOpen {
                rp: RpRead::new(first.build()),
                chunks: [Ok(Bytes::from_static(b"AAAA")), Err(transient_err())].into(),
            },
            ScriptedOpen {
                rp: RpRead::new(second.build()),
                chunks: [Ok(Bytes::from_static(b"BBBB"))].into(),
            },
        ],
    ));

    let (_, mut stream) = oio::Read::open(&reader, BytesRange::default()).await?;
    assert_eq!(
        oio::ReadStream::read(&mut stream).await?.to_bytes(),
        Bytes::from_static(b"AAAA")
    );
    let err = oio::ReadStream::read(&mut stream)
        .await
        .expect_err("dropping strong etag must not silently downgrade to length/mtime matching");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    assert!(!err.is_temporary());
    assert_eq!(opens.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn http_partial_content_unknown_total_size_reopens_cleanly() -> Result<()> {
    use futures::stream;
    use http::{Request, Response, StatusCode};
    use opendal_service_http::Http;

    #[derive(Debug)]
    struct PartialUnknownSizeTransport {
        ranges: Arc<Mutex<Vec<Option<String>>>>,
    }

    impl HttpTransport for PartialUnknownSizeTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let range_header = req
                .headers()
                .get(http::header::RANGE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let attempt = {
                let mut ranges = self.ranges.lock().unwrap();
                ranges.push(range_header);
                ranges.len()
            };

            let response = match attempt {
                1 => Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("content-range", "bytes 0-3/*")
                    .header("content-length", "4")
                    .header("etag", "\"e1\"")
                    .body(HttpBody::new(
                        stream::iter([
                            Ok(Buffer::from(Bytes::from_static(b"AB"))),
                            Err(transient_err()),
                        ]),
                        Some(4),
                    ))
                    .expect("first response must build"),
                2 => Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("content-range", "bytes 2-3/*")
                    .header("content-length", "2")
                    .header("etag", "\"e1\"")
                    .body(HttpBody::new(
                        stream::iter([Ok(Buffer::from(Bytes::from_static(b"CD")))]),
                        Some(2),
                    ))
                    .expect("second response must build"),
                _ => unreachable!("unexpected HTTP request attempt {attempt}"),
            };

            Ok(response)
        }
    }

    let ranges = Arc::new(Mutex::new(Vec::new()));
    let op = Operator::new(Http::default().endpoint("https://example.com"))?
        .with_context(
            OperationContext::new().with_http_transport(HttpTransporter::new(
                PartialUnknownSizeTransport {
                    ranges: ranges.clone(),
                },
            )),
        )
        .layer(
            RetryLayer::new()
                .with_min_delay(Duration::ZERO)
                .with_max_delay(Duration::ZERO),
        );

    let data = op.read_with("file.txt").range(0..4).await?;
    assert_eq!(data.to_bytes(), Bytes::from_static(b"ABCD"));
    assert_eq!(
        *ranges.lock().unwrap(),
        vec![Some("bytes=0-3".to_string()), Some("bytes=2-3".to_string())]
    );
    Ok(())
}
