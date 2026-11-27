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
use std::time::Duration;

use futures::TryStreamExt;
use opendal_core::raw::oio::{self, ReadStreamDyn};
use opendal_core::raw::*;
use opendal_core::*;
use opendal_executor_tokio::TokioExecutor;
use opendal_layer_checksum::ChecksumLayer;
use opendal_layer_retry::RetryLayer;
use opendal_layer_route::RouteLayer;

const GOOD: &str = "123456789";
const CORRUPT: &str = "12345678X";

fn crc32c() -> Checksum {
    Checksum::new(ChecksumAlgo::Crc32c, [0xe3, 0x06, 0x92, 0x83])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadMethod {
    Open,
    Buffered,
}

type ReadRequest = (ReadMethod, BytesRange, bool);

#[derive(Clone, Debug, Default)]
struct Fixture {
    payload: Buffer,
    full_length: Option<u64>,
    checksum: Option<Checksum>,
    read_with_checksum: bool,
    hint_required: bool,
    interrupt: bool,
    requests: Arc<Mutex<Vec<ReadRequest>>>,
}

impl Fixture {
    fn new(payload: &'static str) -> Self {
        Self {
            payload: Buffer::from(payload),
            full_length: Some(payload.len() as u64),
            checksum: None,
            read_with_checksum: false,
            hint_required: false,
            interrupt: false,
            requests: Arc::default(),
        }
    }

    fn opt_in(payload: &'static str) -> Self {
        Self {
            checksum: Some(crc32c()),
            read_with_checksum: true,
            hint_required: true,
            ..Self::new(payload)
        }
    }

    fn operator(&self) -> Result<Operator> {
        Ok(Operator::new(self.clone())?.with_context(
            OperationContext::new().with_executor(Executor::with(TokioExecutor::default())),
        ))
    }

    fn requests(&self) -> Vec<ReadRequest> {
        std::mem::take(&mut *self.requests.lock().unwrap())
    }
}

impl Builder for Fixture {
    type Config = ();

    fn build(self) -> Result<impl Service> {
        Ok(self)
    }
}

impl Service for Fixture {
    type Reader = FixtureReader;
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("checksum-test")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            read_with_suffix: true,
            read_with_checksum: self.read_with_checksum,
            ..Default::default()
        }
    }

    fn read(&self, _: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        if path == "invalid" {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "fixture rejects this read",
            ));
        }
        Ok(FixtureReader {
            fixture: self.clone(),
            args,
        })
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        panic!("these reads must not issue a stat")
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unreachable!()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unreachable!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unreachable!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unreachable!()
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        unreachable!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unreachable!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unreachable!()
    }
}

struct FixtureReader {
    fixture: Fixture,
    args: OpRead,
}

impl FixtureReader {
    fn response(&self, method: ReadMethod, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let fixture = &self.fixture;
        let hint = self.args.checksum_for_range(range);
        fixture.requests.lock().unwrap().push((method, range, hint));
        let checksum = fixture
            .checksum
            .as_ref()
            .filter(|_| hint || !fixture.hint_required);
        let response = match fixture.full_length {
            Some(length) => {
                let mut metadata = MetadataBuilder::file(length);
                metadata.etag("\"fixture\"");
                if let Some(checksum) = checksum {
                    metadata.checksum(checksum.clone());
                }
                RpRead::new(metadata.build())
            }
            None => {
                if let Some(checksum) = checksum {
                    let mut metadata = MetadataBuilder::unknown();
                    metadata.checksum(checksum.clone());
                    RpRead::new(metadata.build())
                } else {
                    RpRead::default()
                }
            }
        };
        let range = range.to_content_range(fixture.payload.len())?;
        Ok((response, fixture.payload.slice(range)))
    }
}

impl oio::Read for FixtureReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn ReadStreamDyn>)> {
        let (response, buffer) = self.response(ReadMethod::Open, range)?;
        let split = buffer.len().min(4);
        let first = buffer.slice(..split);
        let rest = if self.fixture.interrupt && range.is_full() {
            Err(
                Error::new(ErrorKind::Unexpected, "fixture interrupted this stream")
                    .set_temporary(),
            )
        } else {
            Ok(buffer.slice(split..))
        };
        let chunks = VecDeque::from([Ok(first), rest]);
        Ok((response, Box::new(FixtureStream(chunks))))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.response(ReadMethod::Buffered, range)
    }
}

struct FixtureStream(VecDeque<Result<Buffer>>);

impl oio::ReadStream for FixtureStream {
    async fn read(&mut self) -> Result<Buffer> {
        self.0.pop_front().unwrap_or_else(|| Ok(Buffer::new()))
    }
}

fn assert_mismatch(error: Error) {
    assert_eq!(error.kind(), ErrorKind::ChecksumMismatch, "{error}");
    assert!(error.is_permanent(), "{error}");
}

#[tokio::test]
async fn exact_limit_checks_corruption_and_prefix_keeps_its_range() -> Result<()> {
    let exact = Fixture::opt_in(CORRUPT);
    let op = exact.operator()?.layer(ChecksumLayer::new());
    assert_mismatch(op.read("object").await.unwrap_err());
    assert_mismatch(op.read_with("object").limit(9).await.unwrap_err());
    assert_eq!(
        exact.requests(),
        [
            (ReadMethod::Open, BytesRange::default(), true),
            (ReadMethod::Open, BytesRange::new(0, Some(9)), true),
        ]
    );

    let prefix = Fixture::opt_in("12345678Xabcd");
    let op = prefix
        .operator()?
        .layer(ChecksumLayer::new().require_read(true));
    assert_eq!(op.read_with("object").limit(9).await?.to_bytes(), CORRUPT);
    assert_eq!(
        prefix.requests(),
        [(ReadMethod::Open, BytesRange::new(0, Some(9)), true)]
    );
    Ok(())
}

#[tokio::test]
async fn suffix_read_detects_corruption_with_an_explicit_hint() -> Result<()> {
    let fixture = Fixture::opt_in(CORRUPT);
    let op = fixture.operator()?.layer(ChecksumLayer::new());
    let range = BytesRange::suffix(9);
    let result = op.read_with("object").range(range).checksum(true).await;
    assert_eq!(fixture.requests(), [(ReadMethod::Open, range, true)]);
    assert_mismatch(result.unwrap_err());
    Ok(())
}

#[tokio::test]
async fn suffix_stream_withholds_corrupt_final_chunk_with_an_unsolicited_checksum() -> Result<()> {
    let fixture = Fixture {
        hint_required: false,
        ..Fixture::opt_in(CORRUPT)
    };
    let op = fixture.operator()?.layer(ChecksumLayer::new());
    let range = BytesRange::suffix(12);
    let mut stream = op.reader("object").await?.into_stream(range).await?;
    assert_eq!(stream.try_next().await?.unwrap().to_bytes(), "1234");
    assert_eq!(fixture.requests(), [(ReadMethod::Open, range, false)]);
    assert_mismatch(stream.try_next().await.unwrap_err());
    Ok(())
}

#[tokio::test]
async fn suffix_with_whole_object_coverage_requires_a_checksum_in_strict_mode() -> Result<()> {
    let fixture = Fixture {
        read_with_checksum: true,
        ..Fixture::new(GOOD)
    };
    let op = fixture
        .operator()?
        .layer(ChecksumLayer::new().require_read(true));
    let range = BytesRange::suffix(12);
    let result = op.read_with("object").range(range).await;
    assert_eq!(fixture.requests(), [(ReadMethod::Open, range, false)]);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Unsupported);
    Ok(())
}

#[tokio::test]
async fn suffix_with_partial_or_unknown_coverage_is_exempt_in_strict_mode() -> Result<()> {
    for (full_length, size, expected) in [(Some(9), 3, "78X"), (None, 9, CORRUPT)] {
        let fixture = Fixture {
            full_length,
            ..Fixture::opt_in(CORRUPT)
        };
        let op = fixture
            .operator()?
            .layer(ChecksumLayer::new().require_read(true));
        let range = BytesRange::suffix(size);
        for hint in [false, true] {
            let buffer = op.read_with("object").range(range).checksum(hint).await?;
            assert_eq!(buffer.to_bytes(), expected);
        }
        assert_eq!(
            fixture.requests(),
            [
                (ReadMethod::Open, range, false),
                (ReadMethod::Open, range, true),
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn suffix_zero_size_skips_the_read_but_a_nonempty_suffix_verifies_an_empty_file() -> Result<()>
{
    let fixture = Fixture::opt_in("");
    let op = fixture
        .operator()?
        .layer(ChecksumLayer::new().require_read(true));
    let buffer = op
        .read_with("object")
        .range(BytesRange::suffix(0))
        .checksum(true)
        .await?;
    assert!(buffer.is_empty());
    assert!(fixture.requests().is_empty());

    let range = BytesRange::suffix(1);
    let result = op.read_with("object").range(range).checksum(true).await;
    assert_eq!(fixture.requests(), [(ReadMethod::Open, range, true)]);
    assert_mismatch(result.unwrap_err());
    Ok(())
}

#[tokio::test]
async fn final_chunk_is_withheld_and_mismatch_stays_latched() -> Result<()> {
    let fixture = Fixture::opt_in(CORRUPT);
    let op = fixture.operator()?.layer(ChecksumLayer::new());
    let reader = op.reader("object").await?;
    let mut stream = reader.into_bytes_stream(0..9).await?;
    assert_eq!(stream.try_next().await.unwrap().unwrap(), "1234");
    for _ in 0..2 {
        let error = stream.try_next().await.unwrap_err().into_inner().unwrap();
        assert_mismatch(*error.downcast::<Error>().unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn buffered_reads_verify_single_whole_range_but_not_partial_chunks() -> Result<()> {
    let fixture = Fixture::opt_in(CORRUPT);
    let op = fixture
        .operator()?
        .layer(ChecksumLayer::new().require_read(true));
    let whole = op.reader_with("object").chunk(9).await?;
    assert_mismatch(whole.read(0..9).await.unwrap_err());
    let chunked = op.reader_with("object").chunk(3).await?;
    assert_eq!(chunked.read(0..9).await?.to_bytes(), CORRUPT);
    assert_eq!(
        fixture.requests(),
        [
            (ReadMethod::Buffered, BytesRange::new(0, Some(9)), true),
            (ReadMethod::Buffered, BytesRange::new(0, Some(3)), true),
            (ReadMethod::Buffered, BytesRange::new(3, Some(3)), false),
            (ReadMethod::Buffered, BytesRange::new(6, Some(3)), false),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn default_false_and_verify_read_gate_automatic_hints_only() -> Result<()> {
    let unsupported = Fixture::new(GOOD);
    let op = unsupported.operator()?.layer(ChecksumLayer::new());
    assert!(!op.info().capability().read_with_checksum);
    assert_eq!(op.read("object").await?.to_bytes(), GOOD);
    assert_eq!(
        op.read_with("object")
            .range(2..5)
            .checksum(true)
            .await?
            .to_bytes(),
        "345"
    );
    assert_eq!(
        unsupported.requests(),
        [
            (ReadMethod::Open, BytesRange::default(), false),
            (ReadMethod::Open, BytesRange::new(2, Some(3)), true),
        ]
    );

    let supported = Fixture::opt_in(CORRUPT);
    let op = supported.operator()?.layer(ChecksumLayer::new());
    assert_eq!(
        op.read_with("object")
            .range(BytesRange::suffix(3))
            .await?
            .to_bytes(),
        "78X"
    );
    assert_eq!(
        op.read_with("object")
            .range(2..5)
            .checksum(true)
            .await?
            .to_bytes(),
        "345"
    );

    let op = supported
        .operator()?
        .layer(ChecksumLayer::new().require_read(true).verify_read(false));
    assert_eq!(op.read("object").await?.to_bytes(), CORRUPT);
    assert_eq!(
        op.read_with("object").checksum(true).await?.to_bytes(),
        CORRUPT
    );
    assert_eq!(
        supported.requests(),
        [
            (ReadMethod::Open, BytesRange::suffix(3), false),
            (ReadMethod::Open, BytesRange::new(2, Some(3)), true),
            (ReadMethod::Open, BytesRange::default(), false),
            (ReadMethod::Open, BytesRange::default(), true),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn strict_requires_checksum_only_with_whole_object_coverage() -> Result<()> {
    let mut fixture = Fixture::new(GOOD);
    let strict = ChecksumLayer::new().require_read(true);
    let op = fixture.operator()?.layer(strict.clone());
    for result in [
        op.read("object").await,
        op.read_with("object").limit(9).await,
    ] {
        assert_eq!(result.unwrap_err().kind(), ErrorKind::Unsupported);
    }
    assert_eq!(op.read_with("object").limit(4).await?.to_bytes(), "1234");

    fixture.full_length = None;
    let op = fixture.operator()?.layer(strict);
    assert_eq!(op.read_with("object").limit(9).await?.to_bytes(), GOOD);
    Ok(())
}

#[tokio::test]
async fn verifies_unsolicited_digests_with_no_read_capability() -> Result<()> {
    // Standard check values for the same byte sequence, in wire digest order.
    for (algo, expected) in [
        (ChecksumAlgo::Crc64Nvme, "ae8b14860a799888"),
        (ChecksumAlgo::Crc64Xz, "995dc9bbdf1939fa"),
        (ChecksumAlgo::Crc32c, "e3069283"),
        (ChecksumAlgo::Md5, "25f9e794323b453885f5181f1b624d0b"),
        (
            ChecksumAlgo::Sha256,
            "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225",
        ),
    ] {
        let expected = (0..expected.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&expected[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        let checksum = Some(Checksum::new(algo, expected));
        let good = Fixture {
            checksum: checksum.clone(),
            ..Fixture::new(GOOD)
        };
        let corrupt = Fixture {
            checksum,
            ..Fixture::new(CORRUPT)
        };
        let layer = ChecksumLayer::new().require_read(true);
        assert_eq!(
            good.operator()?
                .layer(layer.clone())
                .read_with("object")
                .limit(12)
                .await?
                .to_bytes(),
            GOOD
        );
        assert_mismatch(
            corrupt
                .operator()?
                .layer(layer)
                .read("object")
                .await
                .unwrap_err(),
        );
        assert_eq!(
            corrupt.requests(),
            [(ReadMethod::Open, BytesRange::default(), false)]
        );
    }
    Ok(())
}

#[tokio::test]
async fn checks_reported_length_and_finalizes_at_eof_without_a_positive_length() -> Result<()> {
    for (payload, length) in [
        ("", Some(0)),
        (CORRUPT, None),
        (GOOD, Some(10)),
        (GOOD, Some(8)),
    ] {
        let fixture = Fixture {
            full_length: length,
            checksum: Some(crc32c()),
            ..Fixture::new(payload)
        };
        // Exercise the verifier without the completion layer validating length first.
        let op = Operator::from_parts(OperationContext::new(), Arc::new(fixture))
            .layer(ChecksumLayer::new());
        assert_mismatch(op.read("object").await.unwrap_err());
    }
    Ok(())
}

#[tokio::test]
async fn retries_resume_without_automatic_hint_and_verify_one_logical_stream() -> Result<()> {
    for payload in [GOOD, CORRUPT] {
        let fixture = Fixture {
            interrupt: true,
            ..Fixture::opt_in(payload)
        };
        let op = fixture
            .operator()?
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::ZERO)
                    .with_max_times(1),
            )
            .layer(ChecksumLayer::new().require_read(true));
        let result = op.read("object").await;
        if payload == GOOD {
            assert_eq!(result?.to_bytes(), GOOD);
        } else {
            assert_mismatch(result.unwrap_err());
        }
        assert_eq!(
            fixture.requests(),
            [
                (ReadMethod::Open, BytesRange::default(), true),
                (ReadMethod::Open, BytesRange::new(4, Some(5)), false),
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn mixed_routes_request_checksums_only_from_supporting_targets() -> Result<()> {
    let layer = ChecksumLayer::new();
    let supported = Fixture::opt_in(CORRUPT);
    let unsupported = Fixture::new(GOOD);

    let routes = RouteLayer::builder()
        .route("routed", supported.operator()?.layer(layer.clone()))
        .build()?;
    let op = unsupported.operator()?.layer(routes).layer(layer.clone());
    assert!(!op.info().capability().read_with_checksum);
    assert_mismatch(op.read("routed").await.unwrap_err());
    assert_eq!(
        supported.requests(),
        [(ReadMethod::Open, BytesRange::default(), true)]
    );

    let routes = RouteLayer::builder()
        .route("routed", unsupported.operator()?)
        .build()?;
    let op = supported
        .operator()?
        .layer(layer.clone())
        .layer(routes)
        .layer(layer);
    assert!(!op.info().capability().read_with_checksum);
    assert_eq!(op.read("routed").await?.to_bytes(), GOOD);
    assert_mismatch(op.read("default").await.unwrap_err());
    assert_eq!(
        unsupported.requests(),
        [(ReadMethod::Open, BytesRange::default(), false)]
    );
    assert_eq!(
        supported.requests(),
        [(ReadMethod::Open, BytesRange::default(), true)]
    );
    Ok(())
}

#[tokio::test]
async fn preserves_service_read_errors_before_open() -> Result<()> {
    let fixture = Fixture::opt_in(GOOD);
    let op = fixture.operator()?.layer(ChecksumLayer::new());
    let error = op
        .reader("invalid")
        .await
        .expect_err("reader construction must fail");
    assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    assert_eq!(
        op.read_with("invalid").limit(0).await.unwrap_err().kind(),
        ErrorKind::ConfigInvalid
    );
    assert!(fixture.requests().is_empty());
    Ok(())
}
