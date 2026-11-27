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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opendal_core::raw::oio::{self, ReadStreamDyn};
use opendal_core::raw::*;
use opendal_core::*;
use opendal_layer_capability_check::CapabilityCheckLayer;
use opendal_layer_checksum::ChecksumLayer;
use opendal_layer_retry::RetryLayer;

const SOURCE: &[u8] = b"123456789";

#[derive(Clone, Copy, Debug)]
enum Fault {
    RejectChecksum(ErrorKind),
    UnsupportedWrite,
    ReplayableMismatch,
    PostCommitVerification,
}

#[derive(Debug, Default)]
struct Calls {
    factories: Vec<OpWrite>,
    writes: usize,
    native_copies: usize,
    commits: Vec<Vec<u8>>,
    read_hints: Vec<bool>,
    stat_hints: Vec<bool>,
    presigned_checksums: Vec<Option<WriteChecksum>>,
}

impl Calls {
    fn write_checksums(&self) -> Vec<Option<WriteChecksum>> {
        self.factories.iter().map(OpWrite::checksum).collect()
    }
}

#[derive(Clone, Debug, Default)]
struct Fixture {
    native_checksums: bool,
    fault: Option<Fault>,
    calls: Arc<Mutex<Calls>>,
}

impl Fixture {
    fn new(native_checksums: bool, fault: Option<Fault>) -> Self {
        Self {
            native_checksums,
            fault,
            calls: Arc::default(),
        }
    }

    fn operator(&self) -> Result<Operator> {
        Operator::new(self.clone())
    }

    fn take_calls(&self) -> Calls {
        std::mem::take(&mut *self.calls.lock().unwrap())
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
    type Writer = FixtureWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::with_scheme("checksum-test")
    }

    fn capability(&self) -> Capability {
        Capability {
            read: true,
            read_with_checksum: true,
            stat: true,
            presign: true,
            presign_write: true,
            write: true,
            write_can_append: true,
            write_can_copy_from: true,
            write_can_multi: true,
            write_with_cache_control: true,
            write_with_checksum: self.native_checksums,
            write_with_if_match: true,
            ..Default::default()
        }
    }

    fn read(&self, _: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        assert_eq!(path, "source");
        Ok(FixtureReader {
            args,
            calls: self.calls.clone(),
        })
    }

    async fn stat(&self, _: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        assert_eq!(path, "source");
        self.calls.lock().unwrap().stat_hints.push(args.checksum());
        Ok(RpStat::new(
            MetadataBuilder::file(SOURCE.len() as u64).build(),
        ))
    }

    fn write(&self, _: &OperationContext, _: &str, args: OpWrite) -> Result<Self::Writer> {
        let has_checksum = args.checksum().is_some();
        self.calls.lock().unwrap().factories.push(args);
        if has_checksum && let Some(Fault::RejectChecksum(kind)) = self.fault {
            return Err(Error::new(kind, "fixture rejected the checksum intent"));
        }
        Ok(FixtureWriter {
            fault: self.fault,
            calls: self.calls.clone(),
            payload: Vec::new(),
        })
    }

    async fn presign(&self, _: &OperationContext, _: &str, args: OpPresign) -> Result<RpPresign> {
        let PresignOperation::Write(write) = args.operation() else {
            unreachable!()
        };
        self.calls
            .lock()
            .unwrap()
            .presigned_checksums
            .push(write.checksum());
        Ok(RpPresign::new(PresignedRequest::new(
            "PUT".parse().unwrap(),
            "https://checksum.test/object".parse().unwrap(),
            Default::default(),
        )))
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
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
}

struct FixtureReader {
    args: OpRead,
    calls: Arc<Mutex<Calls>>,
}

impl oio::Read for FixtureReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn ReadStreamDyn>)> {
        let (rp, buffer) = self.read(range).await?;
        Ok((rp, Box::new(buffer)))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        self.calls
            .lock()
            .unwrap()
            .read_hints
            .push(self.args.checksum_for_range(range));
        let mut metadata = MetadataBuilder::file(SOURCE.len() as u64);
        // An unsolicited bad checksum makes disabled verification observable even without a hint.
        metadata.checksum(Checksum::new(ChecksumAlgo::Crc32c, [0; 4]));
        let content = &SOURCE[range.to_content_range(SOURCE.len())?];
        Ok((RpRead::new(metadata.build()), Buffer::from(content)))
    }
}

struct FixtureWriter {
    fault: Option<Fault>,
    calls: Arc<Mutex<Calls>>,
    payload: Vec<u8>,
}

impl oio::Write for FixtureWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        let mut calls = self.calls.lock().unwrap();
        calls.writes += 1;
        match self.fault {
            Some(Fault::UnsupportedWrite) => {
                return Err(Error::new(ErrorKind::Unsupported, "fixture write failed"));
            }
            Some(Fault::ReplayableMismatch) if calls.writes == 1 => {
                return Err(
                    Error::new(ErrorKind::ChecksumMismatch, "fixture replayable mismatch")
                        .set_temporary(),
                );
            }
            _ => (),
        }
        self.payload.extend_from_slice(&buffer.to_bytes());
        Ok(())
    }

    async fn copy_from(&mut self, path: &str, _: OpRead, range: BytesRange) -> Result<()> {
        assert_eq!(path, "source");
        self.calls.lock().unwrap().native_copies += 1;
        self.payload
            .extend_from_slice(&SOURCE[range.to_content_range(SOURCE.len())?]);
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        let length = self.payload.len() as u64;
        self.calls
            .lock()
            .unwrap()
            .commits
            .push(std::mem::take(&mut self.payload));
        if matches!(self.fault, Some(Fault::PostCommitVerification)) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "fixture post-commit verification failed",
            ));
        }
        Ok(MetadataBuilder::file(length).build())
    }

    async fn abort(&mut self) -> Result<()> {
        self.payload.clear();
        Ok(())
    }
}

#[tokio::test]
async fn write_policy_uses_native_support_or_forwards_unchanged() -> Result<()> {
    let supported = Fixture::new(true, None);
    supported
        .operator()?
        .layer(ChecksumLayer::new())
        .writer("target")
        .await?;
    assert_eq!(
        supported.take_calls().write_checksums(),
        [Some(WriteChecksum::Fast)]
    );

    let unsupported = Fixture::new(false, None);
    let op = unsupported.operator()?;
    op.clone()
        .layer(ChecksumLayer::new())
        .writer("target")
        .await?;
    assert_eq!(unsupported.take_calls().write_checksums(), [None]);

    let error = op
        .layer(ChecksumLayer::new().require_write(true))
        .writer("target")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(unsupported.take_calls().factories.is_empty());
    Ok(())
}

#[tokio::test]
async fn explicit_intent_is_never_replaced_or_downgraded() -> Result<()> {
    let fixture = Fixture::new(true, Some(Fault::RejectChecksum(ErrorKind::Unsupported)));
    let op = fixture.operator()?.layer(ChecksumLayer::new());
    for checksum in [
        WriteChecksum::Expected(Checksum::new(
            ChecksumAlgo::Crc32c,
            [0xe3, 0x06, 0x92, 0x83],
        )),
        WriteChecksum::Algo(ChecksumAlgo::Crc32c),
    ] {
        let error = op
            .writer_with("target")
            .checksum(checksum.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        assert_eq!(error.message(), "fixture rejected the checksum intent");
        assert_eq!(fixture.take_calls().write_checksums(), [Some(checksum)]);
    }
    Ok(())
}

#[tokio::test]
async fn injection_validation_can_fall_back_before_factory_selection() -> Result<()> {
    let fixture = Fixture::new(true, None);
    let layer = ChecksumLayer::new().write(WriteChecksumPolicy::Algo(ChecksumAlgo::Crc32c));
    let op = fixture.operator()?;
    op.clone()
        .layer(layer.clone())
        .writer_with("target")
        .append(true)
        .await?;
    let calls = fixture.take_calls();
    assert_eq!(calls.write_checksums(), [None]);
    assert!(calls.factories[0].append());

    let error = op
        .layer(layer.require_write(true))
        .writer_with("target")
        .append(true)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(error.message().contains("full-file checksum with append"));
    assert!(fixture.take_calls().factories.is_empty());
    Ok(())
}

#[tokio::test]
async fn only_best_effort_unsupported_factory_errors_reopen_with_original_options() -> Result<()> {
    for (kind, required, falls_back) in [
        (ErrorKind::Unsupported, false, true),
        (ErrorKind::Unsupported, true, false),
        (ErrorKind::ConfigInvalid, false, false),
    ] {
        let fixture = Fixture::new(true, Some(Fault::RejectChecksum(kind)));
        let op = fixture.operator()?.layer(
            ChecksumLayer::new()
                .write(WriteChecksumPolicy::Any)
                .require_write(required),
        );
        let result = op
            .write_with("target", SOURCE)
            .append(true)
            .if_match("\"etag\"")
            .cache_control("no-store")
            .concurrent(3)
            .await;
        let calls = fixture.take_calls();
        if falls_back {
            result?;
            assert_eq!(calls.write_checksums(), [Some(WriteChecksum::Any), None]);
            assert_eq!(calls.commits, [SOURCE]);
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.message(), "fixture rejected the checksum intent");
            assert_eq!(calls.write_checksums(), [Some(WriteChecksum::Any)]);
            assert_eq!(calls.writes, 0);
        }
        for args in calls.factories {
            assert!(args.append());
            assert_eq!(args.if_match(), Some("\"etag\""));
            assert_eq!(args.cache_control(), Some("no-store"));
            assert_eq!(args.concurrent(), 3);
        }
    }
    Ok(())
}

#[tokio::test]
async fn preserves_unrelated_capability_check_rejection() -> Result<()> {
    let fixture = Fixture::new(true, None);
    let op = fixture.operator()?.layer(CapabilityCheckLayer::new());
    let bare = op
        .writer_with("target")
        .content_type("application/octet-stream")
        .await
        .unwrap_err();
    let layered = op
        .layer(ChecksumLayer::new())
        .writer_with("target")
        .content_type("application/octet-stream")
        .await
        .unwrap_err();
    assert_eq!(layered.kind(), ErrorKind::Unsupported);
    assert!(layered.message().contains("content_type"));
    assert_eq!(layered.to_string(), bare.to_string());
    assert!(fixture.take_calls().factories.is_empty());
    Ok(())
}

#[tokio::test]
async fn retry_layer_replays_only_replayable_body_work_on_the_same_writer() -> Result<()> {
    for (fault, expected_error, writes, committed) in [
        (
            Fault::UnsupportedWrite,
            Some((ErrorKind::Unsupported, "fixture write failed")),
            1,
            false,
        ),
        (Fault::ReplayableMismatch, None, 2, true),
        (
            Fault::PostCommitVerification,
            Some((
                ErrorKind::Unexpected,
                "fixture post-commit verification failed",
            )),
            1,
            true,
        ),
    ] {
        let fixture = Fixture::new(true, Some(fault));
        let op = fixture
            .operator()?
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::ZERO)
                    .with_max_times(1),
            )
            .layer(ChecksumLayer::new());
        let result = op.write("target", SOURCE).await;
        if let Some((kind, message)) = expected_error {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), kind);
            assert_eq!(error.message(), message);
            assert!(error.is_persistent());
        } else {
            result?;
        }
        let calls = fixture.take_calls();
        assert_eq!(calls.write_checksums(), [Some(WriteChecksum::Fast)]);
        assert_eq!(calls.writes, writes);
        if committed {
            assert_eq!(calls.commits, [SOURCE]);
        } else {
            assert!(calls.commits.is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn public_copy_from_preserves_native_dispatch_when_injection_is_inactive() -> Result<()> {
    for (native_checksums, disable_policy, fault, expect_native) in [
        (true, false, None, false),
        (
            true,
            false,
            Some(Fault::RejectChecksum(ErrorKind::Unsupported)),
            false,
        ),
        (false, false, None, true),
        (true, true, None, true),
    ] {
        let fixture = Fixture::new(native_checksums, fault);
        let layer = if disable_policy {
            ChecksumLayer::new().write(None).require_write(true)
        } else {
            ChecksumLayer::new()
        };
        let op = fixture.operator()?.layer(layer);
        assert_eq!(op.info().capability().write_can_copy_from, expect_native);
        let mut writer = op.writer_with("target").chunk(3).await?;
        writer.copy_from("source", 1..7).await?;
        writer.close().await?;
        let calls = fixture.take_calls();
        assert_eq!(calls.native_copies > 0, expect_native);
        assert_eq!(calls.read_hints.is_empty(), expect_native);
        assert_eq!(calls.commits, [b"234567".as_slice()]);
        if disable_policy {
            assert_eq!(calls.write_checksums(), [None]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn read_opt_out_is_independent_of_write_policy_and_stat_and_presign_pass_through()
-> Result<()> {
    let fixture = Fixture::new(true, None);
    let op = fixture.operator()?;
    let read_only = op.clone().layer(ChecksumLayer::new().write(None));
    let error = read_only.read("source").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ChecksumMismatch);
    assert_eq!(fixture.take_calls().read_hints, [true]);

    let write_only = op
        .clone()
        .layer(ChecksumLayer::new().verify_read(false).require_read(true));
    write_only.writer("target").await?;
    assert_eq!(write_only.read("source").await?.to_bytes(), SOURCE);
    assert_eq!(
        write_only
            .read_with("source")
            .checksum(true)
            .await?
            .to_bytes(),
        SOURCE
    );

    let op = op.layer(ChecksumLayer::new());
    op.stat("source").await?;
    op.stat_with("source").checksum(true).await?;
    op.presign_write("target", Duration::from_secs(60)).await?;
    let calls = fixture.take_calls();
    assert_eq!(calls.write_checksums(), [Some(WriteChecksum::Fast)]);
    assert_eq!(calls.read_hints, [false, true]);
    assert_eq!(calls.stat_hints, [false, true]);
    assert_eq!(calls.presigned_checksums, [None]);
    Ok(())
}
