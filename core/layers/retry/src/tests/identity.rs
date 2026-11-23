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
