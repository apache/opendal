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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use opendal_core::options::{DeleteOptions, ReadOptions};
use opendal_core::raw::oio::{self, Read, ReadStream, Write};
use opendal_core::raw::*;
use opendal_core::services::Memory;
use opendal_core::*;
use opendal_executor_tokio::TokioExecutor;
use opendal_layer_cache::{CacheDecision, CacheLayer, CacheReadRequest};
use opendal_service_fs::Fs;
use tokio::sync::Semaphore;

fn memory() -> Result<Operator> {
    Operator::new(Memory::default())
}

#[derive(Debug, Default)]
struct Observations {
    reads: AtomicUsize,
    stats: AtomicUsize,
    pinned: AtomicUsize,
    overrides: AtomicUsize,
    failed_reads: AtomicUsize,
    failed_writes: AtomicUsize,
    failed_deletes: AtomicUsize,
}

#[derive(Debug, Default, Clone)]
struct Probe {
    observed: Arc<Observations>,
    gate: Option<Arc<Gate>>,
    stat_gate: Option<Arc<Gate>>,
    version: Option<Arc<AtomicUsize>>,
    faults: Option<Arc<AtomicUsize>>,
    expose_version: bool,
    max_write_size: Option<usize>,
}

const FAIL_READ: usize = 1;
const FAIL_WRITE: usize = 2;
const FAIL_DELETE: usize = 4;
const READ_ONLY: usize = 8;
const FAIL_AFTER_WRITE: usize = 16;

#[derive(Debug)]
struct Gate {
    entered: Semaphore,
    release: Semaphore,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }
}

#[derive(Debug)]
struct ObservedService {
    inner: Servicer,
    probe: Probe,
}

impl Layer for Probe {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(ObservedService {
            inner,
            probe: self.clone(),
        })
    }
}

impl ObservedService {
    fn faults(&self) -> usize {
        self.probe
            .faults
            .as_ref()
            .map_or(0, |flags| flags.load(Ordering::Relaxed))
    }

    fn selection(&self, path: &str, explicit: Option<&str>) -> (String, Option<String>) {
        match &self.probe.version {
            Some(current) => {
                let version = explicit
                    .map(str::to_owned)
                    .unwrap_or_else(|| current.load(Ordering::Relaxed).to_string());
                (format!("{version}/{path}"), Some(version))
            }
            None => (path.to_owned(), None),
        }
    }
}

impl Service for ObservedService {
    type Reader = ObservedReader;
    type Writer = ObservedWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        if self.probe.version.is_some() {
            capability.read_with_if_match = true;
            capability.read_with_override_content_type = true;
        }
        if self.probe.expose_version {
            capability.read_with_version = true;
            capability.stat_with_version = true;
        }
        if self.faults() & READ_ONLY != 0 {
            capability.write = false;
        }
        capability.write_total_max_size = self.probe.max_write_size;
        capability
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.probe.observed.stats.fetch_add(1, Ordering::Relaxed);
        if let Some(gate) = &self.probe.stat_gate {
            gate.entered.add_permits(1);
            let _ = gate.release.acquire().await.unwrap();
        }
        let (path, version) = self.selection(path, args.version());
        Ok(self
            .inner
            .stat(ctx, &path, OpStat::new())
            .await?
            .map_metadata(|metadata| {
                observed_metadata(metadata, version.as_deref(), self.probe.expose_version)
            }))
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        if self.faults() & FAIL_READ != 0 && path != "v1/gen" {
            self.probe
                .observed
                .failed_reads
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::new(ErrorKind::PermissionDenied, "read fault"));
        }
        Ok(ObservedReader {
            origin: self.inner.clone(),
            context: ctx.clone(),
            path: path.to_owned(),
            args,
            probe: self.probe.clone(),
        })
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        if self.faults() & FAIL_WRITE != 0 {
            self.probe
                .observed
                .failed_writes
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::new(ErrorKind::PermissionDenied, "write fault"));
        }
        Ok(ObservedWriter {
            inner: self.inner.write(ctx, path, args)?,
            fail_close: self.faults() & FAIL_AFTER_WRITE != 0,
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        if self.faults() & FAIL_DELETE != 0 {
            self.probe
                .observed
                .failed_deletes
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::new(ErrorKind::PermissionDenied, "delete fault"));
        }
        self.inner.delete(ctx)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }
    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }
    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

fn observed_metadata(metadata: Metadata, version: Option<&str>, expose_version: bool) -> Metadata {
    let Some(version) = version else {
        return metadata;
    };
    let mut builder = metadata.into_builder();
    builder.etag(format!("\"{version}\""));
    if expose_version {
        builder.version(version);
    }
    builder.build()
}

struct ObservedReader {
    origin: Servicer,
    context: OperationContext,
    path: String,
    args: OpRead,
    probe: Probe,
}

impl Read for ObservedReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let index = self.probe.observed.reads.fetch_add(1, Ordering::Relaxed);
        let version = self.probe.version.as_ref().map(|current| {
            self.args
                .version()
                .map(str::to_owned)
                .unwrap_or_else(|| current.load(Ordering::Relaxed).to_string())
        });
        if let Some(etag) = self.args.if_match() {
            self.probe.observed.pinned.fetch_add(1, Ordering::Relaxed);
            if version
                .as_ref()
                .map(|version| format!("\"{version}\""))
                .as_deref()
                != Some(etag)
            {
                return Err(Error::new(ErrorKind::ConditionNotMatch, "changed ETag"));
            }
        }
        let path = version
            .as_ref()
            .map_or_else(|| self.path.clone(), |v| format!("{v}/{}", self.path));
        let (response, stream) = self
            .origin
            .read(&self.context, &path, OpRead::new())?
            .open(range)
            .await?;
        let response = match response.into_metadata() {
            Some(metadata) => {
                let mut metadata =
                    observed_metadata(metadata, version.as_deref(), self.probe.expose_version);
                if let Some(content_type) = self.args.override_content_type() {
                    self.probe
                        .observed
                        .overrides
                        .fetch_add(1, Ordering::Relaxed);
                    let mut builder = metadata.into_builder();
                    builder.content_type(content_type);
                    metadata = builder.build();
                }
                RpRead::new(metadata)
            }
            None => RpRead::default(),
        };
        if index == 0
            && let Some(gate) = &self.probe.gate
        {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        Ok((response, stream))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let (response, mut stream) = self.open(range).await?;
        let data = stream.read_all().await?;
        if Some(data.len() as u64) != range.size() {
            return Err(Error::new(ErrorKind::Unexpected, "short exact read"));
        }
        Ok((response, data))
    }
}

struct ObservedWriter {
    inner: oio::Writer,
    fail_close: bool,
}

impl oio::Write for ObservedWriter {
    async fn write(&mut self, data: Buffer) -> Result<()> {
        self.inner.write(data).await
    }
    async fn close(&mut self) -> Result<Metadata> {
        let metadata = self.inner.close().await?;
        if self.fail_close {
            return Err(
                Error::new(ErrorKind::Unexpected, "response lost after commit").set_temporary(),
            );
        }
        Ok(metadata)
    }
    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn memory_ranges_and_policy_use_cached_chunks() -> Result<()> {
    let probe = Probe::default();
    let origin = memory()?.layer(probe.clone());
    let cache = memory()?;
    for path in ["cache", "bypass"] {
        origin.write(path, "abcdefghij").await?;
    }
    origin.write("empty", "").await?;
    let policies = Arc::new(AtomicUsize::new(0));
    let called = policies.clone();
    let op = origin.layer(
        CacheLayer::new(cache.clone())?
            .with_chunk_size(4)
            .with_policy(move |request: &CacheReadRequest<'_>| {
                called.fetch_add(1, Ordering::Relaxed);
                if request.path() == "bypass" {
                    CacheDecision::Bypass
                } else {
                    CacheDecision::Cache
                }
            }),
    );
    let reader = op.service().read(op.context(), "cache", OpRead::new())?;
    let (_, mut stream) = reader.open(BytesRange::from(1..13)).await?;
    assert_eq!(stream.read_all().await?.to_vec(), b"bcdefghij");
    let reads = probe.observed.reads.load(Ordering::Relaxed);
    assert_eq!(reads, 1, "contiguous cold chunks share one aligned open");
    assert_eq!(cache.list("v1/data/").await?.len(), 1);
    let data = cache.list_with("v1/data/").recursive(true).await?;
    assert_eq!(data.len(), 3);
    assert!(
        data.iter()
            .any(|entry| entry.path().ends_with("/4/2") && entry.metadata().content_length() == 2)
    );
    for (range, expected) in [
        (BytesRange::from(2..5), b"cde".as_slice()),
        (BytesRange::suffix(3), b"hij".as_slice()),
        (BytesRange::from(4..8), b"efgh".as_slice()),
    ] {
        assert_eq!(reader.read(range).await?.1.to_vec(), expected);
    }
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), reads);
    assert_eq!(policies.load(Ordering::Relaxed), 1);
    assert_eq!(
        reader
            .open(BytesRange::from(11..12))
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::RangeNotSatisfied
    );
    for range in [BytesRange::from(10..), BytesRange::from(2..2)] {
        let (_, mut stream) = reader.open(range).await?;
        assert!(stream.read_all().await?.is_empty());
    }
    assert!(op.read("empty").await?.is_empty());
    let reads = probe.observed.reads.load(Ordering::Relaxed);
    for _ in 0..2 {
        assert_eq!(op.read("bypass").await?.to_vec(), b"abcdefghij");
    }
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed) - reads, 2);
    Ok(())
}

#[tokio::test]
async fn bounded_cache_configuration_and_backend_failures() -> Result<()> {
    let probe = Probe {
        faults: Some(Arc::new(AtomicUsize::new(READ_ONLY))),
        max_write_size: Some(2),
        ..Probe::default()
    };
    let cache_storage = memory()?;
    let cache = cache_storage.clone().layer(probe.clone());
    assert_eq!(
        CacheLayer::new(cache.clone()).unwrap_err().kind(),
        ErrorKind::ConfigInvalid
    );
    let flags = probe.faults.as_ref().unwrap();
    flags.store(0, Ordering::Relaxed);
    let origin = memory()?;
    let op = origin
        .clone()
        .layer(CacheLayer::new(cache.clone())?.with_chunk_size(50));
    origin.write("object", "abcde").await?;
    assert_eq!(op.read("object").await?.to_vec(), b"abcde");
    assert_eq!(
        cache_storage
            .list_with("v1/data/")
            .recursive(true)
            .await?
            .len(),
        3
    );

    flags.store(FAIL_READ | FAIL_WRITE | FAIL_DELETE, Ordering::Relaxed);
    op.write("object", "fghij").await?;
    assert_eq!(op.read("object").await?.to_vec(), b"fghij");
    assert_eq!(
        op.read("missing").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert!(probe.observed.failed_reads.load(Ordering::Relaxed) > 0);
    assert!(probe.observed.failed_writes.load(Ordering::Relaxed) > 0);
    assert!(probe.observed.failed_deletes.load(Ordering::Relaxed) > 0);
    Ok(())
}

#[tokio::test]
async fn disk_cache_reuses_persisted_metadata_and_chunks() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let open_cache = || Operator::new(Fs::default().root(directory.path().to_str().unwrap()));
    let probe = Probe::default();
    let origin = memory()?.layer(probe.clone());
    origin
        .write_with("object", "abcdefg")
        .content_type("text/plain")
        .await?;
    let range = BytesRange::from(2..7);
    for _ in 0..2 {
        let op = origin
            .clone()
            .layer(CacheLayer::new(open_cache()?)?.with_chunk_size(3));
        let reader = op.service().read(op.context(), "object", OpRead::new())?;
        let (response, mut stream) = reader.open(range).await?;
        let metadata = response.into_metadata().unwrap();
        assert_eq!(metadata.content_length(), 7);
        assert_eq!(metadata.content_type(), Some("text/plain"));
        assert_eq!(stream.read_all().await?.to_vec(), b"cdefg");
    }
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn fs_origin_handles_missing_read_metadata_and_mutations() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let origin = Operator::new(Fs::default().root(directory.path().to_str().unwrap()))?;
    let cache = memory()?;
    origin.write("dir/file", "abcdefg").await?;
    origin.write("other", "independent").await?;
    let op = origin
        .clone()
        .layer(CacheLayer::new(cache.clone())?.with_chunk_size(3));
    assert_eq!(
        op.read_with("dir/file")
            .range(BytesRange::suffix(4))
            .await?
            .to_vec(),
        b"defg"
    );
    assert_eq!(op.read("other").await?.to_vec(), b"independent");
    op.write_with("dir/file", "hi").append(true).await?;
    assert_eq!(op.read("dir/file").await?.to_vec(), b"abcdefghi");
    op.copy("dir/file", "other").await?;
    assert_eq!(op.read("other").await?.to_vec(), b"abcdefghi");
    op.write("dir/replacement", "new").await?;
    op.rename("dir/replacement", "dir/file").await?;
    assert_eq!(op.read("dir/file").await?.to_vec(), b"new");
    assert_eq!(
        op.read("dir/replacement").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    op.write("batch", "remove").await?;
    assert_eq!(op.read("batch").await?.to_vec(), b"remove");
    op.delete_iter(["batch", "other"]).await?;
    for path in ["batch", "other"] {
        assert_eq!(op.read(path).await.unwrap_err().kind(), ErrorKind::NotFound);
    }
    op.write("other", "independent").await?;
    assert_eq!(op.read("other").await?.to_vec(), b"independent");
    op.delete_with("dir/").recursive(true).await?;
    assert_eq!(cache.read("v1/gen").await?.to_vec(), b"2");
    assert_eq!(
        op.read("dir/file").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    origin.write("other", "changed").await?;
    assert_eq!(op.read("other").await?.to_vec(), b"changed");
    Ok(())
}

#[tokio::test]
async fn etag_revalidation_and_stat_discard_old_chunks() -> Result<()> {
    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/object", "abcdefgh").await?;
    source.write("2/object", "ABCDEFGH").await?;
    let origin = source.layer(probe.clone());
    let cache = memory()?;
    let op = origin.clone().layer(
        CacheLayer::new(cache.clone())?
            .with_chunk_size(4)
            .with_policy(|_: &CacheReadRequest<'_>| CacheDecision::CacheFor(Duration::ZERO)),
    );
    assert_eq!(op.read("object").await?.to_vec(), b"abcdefgh");
    assert_eq!(op.read("object").await?.to_vec(), b"abcdefgh");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);
    assert_eq!(probe.observed.stats.load(Ordering::Relaxed), 1);
    probe.version.as_ref().unwrap().store(2, Ordering::Relaxed);
    assert_eq!(op.read("object").await?.to_vec(), b"ABCDEFGH");
    let keys = cache.list_with("v1/data/").recursive(true).await?;
    assert_eq!(keys.len(), 4, "the two ETags use distinct chunk keys");
    let op = origin.layer(
        CacheLayer::new(cache)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    probe.version.as_ref().unwrap().store(1, Ordering::Relaxed);
    assert_eq!(op.stat("object").await?.etag(), Some("\"1\""));
    assert_eq!(op.read("object").await?.to_vec(), b"abcdefgh");
    Ok(())
}

#[tokio::test]
async fn versions_are_immutable_and_overrides_and_conditions_pass_through() -> Result<()> {
    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        expose_version: true,
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/object", "first").await?;
    source.write("2/object", "other").await?;
    let op = source.layer(probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(3)
            .with_ttl(Duration::ZERO),
    );
    for _ in 0..2 {
        assert_eq!(
            op.read_with("object").version("1").await?.to_vec(),
            b"first"
        );
        assert_eq!(
            op.read_with("object").version("2").await?.to_vec(),
            b"other"
        );
    }
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 2);
    assert_eq!(probe.observed.stats.load(Ordering::Relaxed), 0);
    assert_eq!(
        op.read_with("object")
            .if_match("\"2\"")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ConditionNotMatch
    );
    let options = ReadOptions {
        override_content_type: Some("application/octet-stream".into()),
        ..Default::default()
    };
    let (_, args, _) = options.into();
    let reader = op.service().read(op.context(), "object", args)?;
    let (response, _) = reader.open(BytesRange::default()).await?;
    assert_eq!(
        response.metadata().unwrap().content_type(),
        Some("application/octet-stream")
    );
    assert_eq!(probe.observed.overrides.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn verified_stream_retries_before_output_and_never_splices_after_output() -> Result<()> {
    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/object", vec![b'a'; 80]).await?;
    source.write("2/object", vec![b'b'; 80]).await?;
    let op = source.layer(probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    assert_eq!(op.read_with("object").range(0..4).await?.to_vec(), b"aaaa");
    let current = probe.version.as_ref().unwrap();
    current.store(2, Ordering::Relaxed);
    assert_eq!(op.read_with("object").range(4..8).await?.to_vec(), b"bbbb");
    let reader = op.service().read(op.context(), "object", OpRead::new())?;
    let (response, mut stream) = reader.open(BytesRange::default()).await?;
    assert_eq!(response.metadata().unwrap().etag(), Some("\"2\""));
    assert!(
        stream
            .read()
            .await?
            .to_vec()
            .iter()
            .all(|&byte| byte == b'b')
    );
    current.store(1, Ordering::Relaxed);
    loop {
        match stream.read().await {
            Ok(data) => {
                assert!(!data.is_empty(), "the stream must report the overwrite");
                assert!(data.to_vec().iter().all(|&byte| byte == b'b'));
            }
            Err(error) => {
                assert!(error.is_temporary());
                assert!(stream.read().await.unwrap_err().is_temporary());
                break;
            }
        }
    }
    assert!(probe.observed.pinned.load(Ordering::Relaxed) >= 2);
    Ok(())
}

#[tokio::test]
async fn canceled_reader_does_not_cancel_shared_fill() -> Result<()> {
    let gate = Gate::new();
    let probe = Probe {
        gate: Some(gate.clone()),
        ..Probe::default()
    };
    let origin = memory()?.layer(probe.clone());
    origin.write("object", "shared").await?;
    let op = origin.layer(CacheLayer::new(memory()?)?.with_chunk_size(4));
    let first = tokio::spawn({
        let op = op.clone();
        async move { op.read("object").await }
    });
    gate.entered.acquire().await.unwrap().forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = tokio::spawn({
        let op = op.clone();
        async move { op.read("object").await }
    });
    gate.release.add_permits(1);
    assert_eq!(second.await.unwrap()?.to_vec(), b"shared");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn mutation_discards_inflight_pointer_even_after_ambiguous_close_failure() -> Result<()> {
    let gate = Gate::new();
    let probe = Probe {
        gate: Some(gate.clone()),
        faults: Some(Arc::new(AtomicUsize::new(0))),
        ..Probe::default()
    };
    let origin = memory()?.layer(probe.clone());
    origin.write("object", "old").await?;
    let op = origin.layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(3)
            .with_ttl(Duration::MAX),
    );
    let pending = tokio::spawn({
        let op = op.clone();
        async move { op.read("object").await }
    });
    gate.entered.acquire().await.unwrap().forget();
    op.write("object", "new").await?;
    gate.release.add_permits(1);
    assert_eq!(pending.await.unwrap()?.to_vec(), b"old");
    assert_eq!(op.read("object").await?.to_vec(), b"new");
    probe
        .faults
        .as_ref()
        .unwrap()
        .store(FAIL_AFTER_WRITE, Ordering::Relaxed);
    let error = op.write("object", "now").await.unwrap_err();
    assert_eq!(error.message(), "response lost after commit");
    assert!(error.is_temporary());
    assert_eq!(op.read("object").await?.to_vec(), b"now");
    Ok(())
}

#[tokio::test]
async fn deleter_delete_invalidates_cache_before_close() -> Result<()> {
    let cache = memory()?;
    let op = memory()?.layer(CacheLayer::new(cache.clone())?.with_ttl(Duration::MAX));

    op.write("object", "value").await?;
    assert_eq!(op.read("object").await?.to_vec(), b"value");

    let mut deleter = op.deleter().await?;
    deleter.delete("object").await?;
    assert_eq!(
        op.read("object").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    deleter.close().await?;
    assert_eq!(
        op.read("object").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    op.write("dir/a", "first").await?;
    op.write("dir/b", "second").await?;
    assert_eq!(op.read("dir/a").await?.to_vec(), b"first");
    assert_eq!(op.read("dir/b").await?.to_vec(), b"second");

    let mut deleter = op.deleter().await?;
    deleter
        .delete((
            "dir/".to_owned(),
            DeleteOptions {
                recursive: true,
                ..Default::default()
            },
        ))
        .await?;
    assert_eq!(cache.read("v1/gen").await?.to_vec(), b"1");
    assert_eq!(
        op.read("dir/a").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        op.read("dir/b").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    deleter.close().await?;
    assert_eq!(cache.read("v1/gen").await?.to_vec(), b"2");
    assert_eq!(
        op.read("dir/a").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn buffered_deleter_invalidates_again_on_close() -> Result<()> {
    #[derive(Debug, Clone)]
    struct BufferedDeleteLayer;

    #[derive(Debug)]
    struct BufferedDeleteService {
        inner: Servicer,
    }

    struct BufferedDeleter {
        inner: oio::Deleter,
        pending: Vec<(String, OpDelete)>,
    }

    impl Layer for BufferedDeleteLayer {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(BufferedDeleteService { inner })
        }
    }

    impl Service for BufferedDeleteService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = BufferedDeleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            self.inner.capability()
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }

        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            Ok(BufferedDeleter {
                inner: self.inner.delete(ctx)?,
                pending: Vec::new(),
            })
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    impl oio::Delete for BufferedDeleter {
        async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
            self.pending.push((path.to_owned(), args));
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            for (path, args) in self.pending.drain(..) {
                self.inner.delete(&path, args).await?;
            }
            self.inner.close().await
        }
    }

    let op = memory()?
        .layer(BufferedDeleteLayer)
        .layer(CacheLayer::new(memory()?)?.with_ttl(Duration::MAX));

    op.write("object", "value").await?;
    assert_eq!(op.read("object").await?.to_vec(), b"value");
    let mut deleter = op.deleter().await?;
    deleter.delete("object").await?;
    // Buffered delete has not hit origin yet; reading re-populates the cache.
    assert_eq!(op.read("object").await?.to_vec(), b"value");
    deleter.close().await?;
    assert_eq!(
        op.read("object").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );

    op.write("dir/a", "first").await?;
    assert_eq!(op.read("dir/a").await?.to_vec(), b"first");
    let mut deleter = op.deleter().await?;
    deleter
        .delete((
            "dir/".to_owned(),
            DeleteOptions {
                recursive: true,
                ..Default::default()
            },
        ))
        .await?;
    // Buffered recursive delete has not hit origin yet; reading re-populates under the new generation.
    assert_eq!(op.read("dir/a").await?.to_vec(), b"first");
    deleter.close().await?;
    assert_eq!(
        op.read("dir/a").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn nonce_large_read_caches_all_chunks_and_partial_miss_preserves_pointer() -> Result<()> {
    let probe = Probe::default();
    let origin = memory()?.layer(probe.clone());
    let payload: Vec<u8> = (0_u8..24).map(|i| b'a' + i).collect();
    origin.write("big", payload.clone()).await?;
    origin.write("pair", "abcdefgh").await?;

    let cache = memory()?;
    let op = origin.layer(
        CacheLayer::new(cache.clone())?
            .with_chunk_size(1)
            .with_ttl(Duration::MAX),
    );

    // 24 chunks exceeds FILL_CHUNKS (16): cold read populates all 24 chunks via
    // single-open NonceFillStream, and subsequent reads hit cache with zero origin reads.
    assert_eq!(op.read("big").await?.to_vec(), payload);
    let reads_after_cold = probe.observed.reads.load(Ordering::Relaxed);
    assert_eq!(op.read("big").await?.to_vec(), payload);
    assert_eq!(
        op.read_with("big").range(5..20).await?.to_vec(),
        &payload[5..20]
    );
    assert_eq!(
        probe.observed.reads.load(Ordering::Relaxed),
        reads_after_cold,
        "hot large Tier 4 reads must not hit origin"
    );

    // Evicting a middle chunk after a large Tier 4 stream has opened must fail
    // with a temporary error rather than splicing from origin, then recover cleanly.
    let mut stream = op.reader("big").await?.into_stream(..).await?;
    let first_part = stream
        .next()
        .await
        .transpose()?
        .expect("first chunk must be yielded before mid-stream eviction");
    assert_eq!(first_part.to_vec(), &payload[..1]);

    let mut data_keys: Vec<String> = cache
        .list_with("v1/data/")
        .recursive(true)
        .await?
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| entry.path().to_owned())
        .collect();
    data_keys.sort();
    let middle_keys: Vec<String> = data_keys
        .into_iter()
        .filter(|key| key.ends_with("/a"))
        .collect();
    assert!(
        !middle_keys.is_empty(),
        "chunk index 10 (0xa) must be cached"
    );
    for key in middle_keys {
        cache.delete(&key).await?;
    }
    let mut err = None;
    while let Some(item) = stream.next().await {
        if let Err(e) = item {
            err = Some(e);
            break;
        }
    }
    let err = err.expect("mid-stream eviction of Tier 4 chunk must not splice");
    assert!(err.is_temporary());
    assert_eq!(op.read("big").await?.to_vec(), payload);

    // Caching disjoint boundary chunks of a large Tier 4 object (first and last)
    // must not trick a subsequent full read into failing mid-stream; it should
    // populate the missing middle chunks and serve future full reads from cache.
    let probe_sparse = Probe::default();
    let origin_sparse = memory()?.layer(probe_sparse.clone());
    origin_sparse.write("sparse", payload.clone()).await?;
    let op_sparse = origin_sparse.layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(1)
            .with_ttl(Duration::MAX),
    );
    assert_eq!(
        op_sparse.read_with("sparse").range(0..1).await?.to_vec(),
        &payload[..1]
    );
    assert_eq!(
        op_sparse
            .read_with("sparse")
            .range((payload.len() as u64 - 1)..payload.len() as u64)
            .await?
            .to_vec(),
        &payload[payload.len() - 1..]
    );
    assert_eq!(op_sparse.read("sparse").await?.to_vec(), payload);
    let reads_after_fill = probe_sparse.observed.reads.load(Ordering::Relaxed);
    assert_eq!(op_sparse.read("sparse").await?.to_vec(), payload);
    assert_eq!(
        probe_sparse.observed.reads.load(Ordering::Relaxed),
        reads_after_fill,
        "full read after filling sparse large Tier 4 object must hit cache"
    );

    // Partial miss on a fresh Tier 4 pointer must populate missing chunks under
    // the existing nonce without discarding the pointer or orphaning chunk 0.
    let probe_pair = Probe::default();
    let origin_pair = memory()?.layer(probe_pair.clone());
    origin_pair.write("pair", "abcdefgh").await?;
    let op_pair = origin_pair.clone().layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    assert_eq!(
        op_pair.read_with("pair").range(0..4).await?.to_vec(),
        b"abcd"
    );
    assert_eq!(probe_pair.observed.reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        op_pair.read_with("pair").range(4..8).await?.to_vec(),
        b"efgh"
    );
    assert_eq!(probe_pair.observed.reads.load(Ordering::Relaxed), 2);
    assert_eq!(
        op_pair.read_with("pair").range(4..8).await?.to_vec(),
        b"efgh"
    );
    assert_eq!(
        probe_pair.observed.reads.load(Ordering::Relaxed),
        2,
        "partial miss on chunk 1 must cache chunk 1 under the existing nonce"
    );
    assert_eq!(
        op_pair.read_with("pair").range(0..4).await?.to_vec(),
        b"abcd"
    );
    assert_eq!(
        probe_pair.observed.reads.load(Ordering::Relaxed),
        2,
        "partial miss on chunk 1 must not invalidate chunk 0"
    );
    assert_eq!(op_pair.read("pair").await?.to_vec(), b"abcdefgh");
    assert_eq!(
        probe_pair.observed.reads.load(Ordering::Relaxed),
        2,
        "full read must hit both chunks under the existing nonce"
    );

    // If origin length changes externally before a Tier 4 partial miss fills,
    // the stale pointer must be discarded and the read must recover cleanly.
    origin_pair.write("growing", "abcdefgh").await?;
    assert_eq!(
        op_pair.read_with("growing").range(0..4).await?.to_vec(),
        b"abcd"
    );
    origin_pair.write("growing", "0123456789ab").await?;
    assert_eq!(
        op_pair.read_with("growing").range(4..8).await?.to_vec(),
        b"4567"
    );
    assert_eq!(op_pair.read("growing").await?.to_vec(), b"0123456789ab");
    Ok(())
}

#[tokio::test]
async fn cached_reader_read_serves_short_suffix_from_cache() -> Result<()> {
    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/short", "hello").await?;
    let op = source.layer(probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );

    let reader = op.service().read(op.context(), "short", OpRead::new())?;
    let (rp1, buf1) = reader.read(BytesRange::suffix(100)).await?;
    assert_eq!(buf1.to_vec(), b"hello");
    assert_eq!(rp1.metadata().unwrap().content_length(), 5);
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);

    let (rp2, buf2) = reader.read(BytesRange::suffix(100)).await?;
    assert_eq!(buf2.to_vec(), b"hello");
    assert_eq!(rp2.metadata().unwrap().content_length(), 5);
    assert_eq!(
        probe.observed.reads.load(Ordering::Relaxed),
        1,
        "hot short suffix read must be served from cache"
    );

    // Unbounded raw `oio::Read::read` stays delegated to the source contract.
    assert!(reader.read(BytesRange::default()).await.is_err());
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn cold_suffix_and_fs_chunk_miss_avoid_extra_opens_and_stats() -> Result<()> {
    // Verifiable backend: cold Suffix read uses 1 stat + 1 aligned read (not 2 reads).
    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/object", "0123456789").await?;
    let op = source.layer(probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    assert_eq!(
        op.read_with("object")
            .range(BytesRange::suffix(3))
            .await?
            .to_vec(),
        b"789"
    );
    assert_eq!(probe.observed.stats.load(Ordering::Relaxed), 1);
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);

    // Fs backend (empty RpRead): cold suffix uses 1 open + 2 stats (pre-stat + post-stat),
    // and a subsequent chunk miss uses 1 open + 1 post-stat (not 2 opens + 2 stats).
    let directory = tempfile::tempdir().unwrap();
    let fs_probe = Probe::default();
    let origin = Operator::new(Fs::default().root(directory.path().to_str().unwrap()))?
        .layer(fs_probe.clone());
    origin.write("file", "abcdefghij").await?;
    let op = origin.layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    assert_eq!(
        op.read_with("file")
            .range(BytesRange::suffix(2))
            .await?
            .to_vec(),
        b"ij"
    );
    assert_eq!(fs_probe.observed.reads.load(Ordering::Relaxed), 1);
    assert_eq!(fs_probe.observed.stats.load(Ordering::Relaxed), 2);

    assert_eq!(op.read_with("file").range(0..4).await?.to_vec(), b"abcd");
    assert_eq!(
        fs_probe.observed.reads.load(Ordering::Relaxed),
        2,
        "chunk miss on Fs must open once, not twice"
    );
    assert_eq!(
        fs_probe.observed.stats.load(Ordering::Relaxed),
        3,
        "chunk miss on Fs must run only post-read verify_stat"
    );
    Ok(())
}

#[tokio::test]
async fn discover_retries_condition_not_match_and_does_not_cache_on_stat_error() -> Result<()> {
    // Case A: stream.read() fails with ConditionNotMatch before any byte is yielded;
    // discover() must map it to FillError::Changed and retry once transparently.
    #[derive(Debug)]
    struct FailFirstStreamLayer {
        opens: Arc<AtomicUsize>,
    }
    #[derive(Debug)]
    struct FailFirstStreamService {
        inner: Servicer,
        opens: Arc<AtomicUsize>,
    }
    struct FailFirstStreamReader {
        inner: oio::Reader,
        opens: Arc<AtomicUsize>,
    }
    struct ConditionMismatchStream;

    impl ReadStream for ConditionMismatchStream {
        async fn read(&mut self) -> Result<Buffer> {
            Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "object changed during inner retry",
            ))
        }
    }

    impl Layer for FailFirstStreamLayer {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(FailFirstStreamService {
                inner,
                opens: self.opens.clone(),
            })
        }
    }

    impl Service for FailFirstStreamService {
        type Reader = FailFirstStreamReader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }
        fn capability(&self) -> Capability {
            self.inner.capability()
        }
        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }
        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }
        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            Ok(FailFirstStreamReader {
                inner: self.inner.read(ctx, path, args)?,
                opens: self.opens.clone(),
            })
        }
        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }
        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }
        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            self.inner.delete(ctx)
        }
        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }
        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }
        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    impl Read for FailFirstStreamReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            let attempt = self.opens.fetch_add(1, Ordering::Relaxed);
            if attempt == 0 {
                let mut meta = MetadataBuilder::file(4);
                meta.etag("\"v1\"");
                return Ok((RpRead::new(meta.build()), Box::new(ConditionMismatchStream)));
            }
            let (rp, stream) = self.inner.open(range).await?;
            let meta = rp
                .into_metadata()
                .map(|m| {
                    let mut b = m.into_builder();
                    b.etag("\"v2\"");
                    b.build()
                })
                .unwrap();
            Ok((RpRead::new(meta), stream))
        }
        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            self.inner.read(range).await
        }
    }

    let opens = Arc::new(AtomicUsize::new(0));
    let source = memory()?;
    source.write("obj", "v222").await?;
    let op = source
        .layer(FailFirstStreamLayer {
            opens: opens.clone(),
        })
        .layer(CacheLayer::new(memory()?)?.with_chunk_size(4));
    assert_eq!(op.read("obj").await?.to_vec(), b"v222");
    assert_eq!(opens.load(Ordering::Relaxed), 2);

    // Case B: length-only RpRead followed by stat error must not publish a stale pointer.
    #[derive(Debug)]
    struct FailingStatLayer(ErrorKind);
    #[derive(Debug)]
    struct FailingStatService {
        inner: Servicer,
        kind: ErrorKind,
    }
    impl Layer for FailingStatLayer {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(FailingStatService {
                inner,
                kind: self.0,
            })
        }
    }
    impl Service for FailingStatService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }
        fn capability(&self) -> Capability {
            self.inner.capability()
        }
        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }
        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            Err(Error::new(self.kind, "injected stat failure"))
        }
        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            self.inner.read(ctx, path, args)
        }
        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }
        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }
        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            self.inner.delete(ctx)
        }
        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }
        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }
        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    let source = memory()?;
    source.write("obj", "data").await?;
    let cache = memory()?;
    let op = source
        .layer(FailingStatLayer(ErrorKind::PermissionDenied))
        .layer(CacheLayer::new(cache.clone())?.with_chunk_size(4));
    assert_eq!(op.read("obj").await?.to_vec(), b"data");
    assert!(
        cache
            .list_with("v1/meta/")
            .recursive(true)
            .await?
            .is_empty(),
        "stat failure during discovery must not publish a pointer"
    );
    Ok(())
}

#[tokio::test]
async fn singleflight_uses_operation_context_executor() -> Result<()> {
    struct CountingExecutor {
        spawns: Arc<AtomicUsize>,
    }

    impl Execute for CountingExecutor {
        fn execute(&self, f: BoxedStaticFuture<()>) {
            self.spawns.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(f);
        }
    }

    let spawns = Arc::new(AtomicUsize::new(0));
    let gate = Gate::new();
    let probe = Probe {
        gate: Some(gate.clone()),
        ..Probe::default()
    };
    let origin = memory()?.layer(probe.clone());
    origin.write("obj", "payload").await?;
    let op = origin
        .layer(CacheLayer::new(memory()?)?.with_chunk_size(4))
        .with_context(
            OperationContext::new().with_executor(Executor::with(CountingExecutor {
                spawns: spawns.clone(),
            })),
        );

    let first = tokio::spawn({
        let op = op.clone();
        async move { op.read("obj").await }
    });
    gate.entered.acquire().await.unwrap().forget();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let second = tokio::spawn({
        let op = op.clone();
        async move { op.read("obj").await }
    });
    gate.release.add_permits(1);
    assert_eq!(second.await.unwrap()?.to_vec(), b"payload");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);
    assert!(
        spawns.load(Ordering::Relaxed) >= 1,
        "singleflight must submit fill tasks through OperationContext::executor"
    );
    Ok(())
}

#[tokio::test]
async fn abandoning_executor_does_not_hang_or_leak_flight_reservations() -> Result<()> {
    struct AbandoningExecutor;

    impl Execute for AbandoningExecutor {
        fn execute(&self, _f: BoxedStaticFuture<()>) {}
    }

    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/obj", "abcdefgh").await?;
    let layer = CacheLayer::new(memory()?)?
        .with_chunk_size(4)
        .with_ttl(Duration::MAX);
    let working = source.clone().layer(probe.clone()).layer(layer.clone());
    let abandoning = working
        .clone()
        .with_context(OperationContext::new().with_executor(Executor::with(AbandoningExecutor)));

    // Cold read: discovery flight is abandoned by the executor; read falls back
    // cleanly to origin without hanging or reporting "object changed".
    let cold = tokio::time::timeout(Duration::from_secs(1), abandoning.read("obj"))
        .await
        .expect("cold read with abandoning executor must not hang")?;
    assert_eq!(cold.to_vec(), b"abcdefgh");

    // Warm pointer + chunk 0 using a working executor so flight map is proven clear.
    assert_eq!(
        working.read_with("obj").range(0..4).await?.to_vec(),
        b"abcd"
    );

    // Warm pointer + chunk 1 miss via abandoning executor (both exact range and
    // streaming read spanning cached chunk 0 and missing chunk 1).
    let partial = tokio::time::timeout(
        Duration::from_secs(1),
        abandoning.read_with("obj").range(4..8),
    )
    .await
    .expect("chunk miss with abandoning executor must not hang")?;
    assert_eq!(partial.to_vec(), b"efgh");

    // Verify another uncached object works end-to-end across chunk 0 (cached) and
    // chunk 1 (missed during streaming) with the abandoning executor.
    source.write("1/stream", "01234567").await?;
    assert_eq!(
        working.read_with("stream").range(0..4).await?.to_vec(),
        b"0123"
    );
    let full = tokio::time::timeout(Duration::from_secs(1), abandoning.read("stream"))
        .await
        .expect("streaming chunk miss with abandoning executor must not hang")?;
    assert_eq!(full.to_vec(), b"01234567");

    // Working executor must still hit cache cleanly afterwards.
    let reads_before = probe.observed.reads.load(Ordering::Relaxed);
    assert_eq!(working.read("stream").await?.to_vec(), b"01234567");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), reads_before);
    Ok(())
}

#[test]
fn tokio_executor_falls_back_inline_outside_tokio_runtime() -> Result<()> {
    futures::executor::block_on(async {
        let probe = Probe {
            version: Some(Arc::new(AtomicUsize::new(1))),
            ..Probe::default()
        };
        let source = memory()?;
        source.write("1/obj", "inline-ok").await?;
        let op = source
            .layer(probe.clone())
            .layer(
                CacheLayer::new(memory()?)?
                    .with_chunk_size(4)
                    .with_ttl(Duration::MAX),
            )
            .with_context(
                OperationContext::new().with_executor(Executor::with(TokioExecutor::default())),
            );

        assert_eq!(op.read("obj").await?.to_vec(), b"inline-ok");
        let reads_after_cold = probe.observed.reads.load(Ordering::Relaxed);
        assert_eq!(reads_after_cold, 1);

        assert_eq!(op.read("obj").await?.to_vec(), b"inline-ok");
        assert_eq!(
            probe.observed.reads.load(Ordering::Relaxed),
            reads_after_cold,
            "inline fallback outside Tokio runtime must still populate cache"
        );
        Ok(())
    })
}

#[tokio::test]
async fn mutation_invalidation_covers_copy_from_compose_restore_and_versioned_delete() -> Result<()>
{
    #[derive(Debug)]
    struct MutationHarness {
        inner: Servicer,
        current_version: Arc<AtomicUsize>,
        restored: Arc<AtomicUsize>,
    }

    impl Layer for MutationHarness {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(MutationHarness {
                inner,
                current_version: self.current_version.clone(),
                restored: self.restored.clone(),
            })
        }
    }

    struct HarnessComposer {
        inner: Servicer,
        ctx: OperationContext,
        to: String,
        parts: Vec<String>,
    }

    impl oio::Compose for HarnessComposer {
        async fn compose(&mut self, path: &str, _: OpRead) -> Result<()> {
            self.parts.push(path.to_owned());
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            let mut combined = Vec::new();
            for part in &self.parts {
                let (_, mut stream) = self
                    .inner
                    .read(&self.ctx, part, OpRead::new())?
                    .open(BytesRange::default())
                    .await?;
                combined.extend_from_slice(&stream.read_all().await?.to_bytes());
            }
            let mut writer = self.inner.write(&self.ctx, &self.to, OpWrite::new())?;
            writer.write(Buffer::from(combined)).await?;
            writer.close().await
        }
    }

    struct HarnessDeleter {
        inner: Servicer,
        ctx: OperationContext,
        current_version: Arc<AtomicUsize>,
    }

    impl oio::Delete for HarnessDeleter {
        async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
            let mut deleter = self.inner.delete(&self.ctx)?;
            if let Some(version) = args.version() {
                deleter
                    .delete(&format!("{version}/{path}"), OpDelete::new())
                    .await?;
                if self.current_version.load(Ordering::Relaxed).to_string() == version {
                    self.current_version.store(2, Ordering::Relaxed);
                }
            } else {
                deleter.delete(path, args).await?;
            }
            deleter.close().await
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct HarnessReader {
        inner: Servicer,
        ctx: OperationContext,
        path: String,
        version: Option<String>,
    }

    impl Read for HarnessReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            let target = self
                .version
                .as_ref()
                .map_or_else(|| self.path.clone(), |v| format!("{v}/{}", self.path));
            let (rp, stream) = self
                .inner
                .read(&self.ctx, &target, OpRead::new())?
                .open(range)
                .await?;
            let rp = match (rp.into_metadata(), &self.version) {
                (Some(meta), Some(v)) => {
                    let mut b = meta.into_builder();
                    b.etag(format!("\"{v}\"")).version(v);
                    RpRead::new(b.build())
                }
                (Some(meta), None) => RpRead::new(meta),
                (None, _) => RpRead::default(),
            };
            Ok((rp, stream))
        }

        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            let target = self
                .version
                .as_ref()
                .map_or_else(|| self.path.clone(), |v| format!("{v}/{}", self.path));
            self.inner
                .read(&self.ctx, &target, OpRead::new())?
                .read(range)
                .await
        }
    }

    impl Service for MutationHarness {
        type Reader = HarnessReader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = HarnessDeleter;
        type Copier = oio::Copier;
        type Composer = HarnessComposer;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }
        fn capability(&self) -> Capability {
            let mut cap = self.inner.capability();
            cap.compose = true;
            cap.restore = true;
            cap.restore_with_version = true;
            cap.read_with_version = true;
            cap.delete_with_version = true;
            cap.write_can_copy_from = true;
            cap
        }
        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }
        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            if path == "ver" {
                let v = args
                    .version()
                    .map(str::to_owned)
                    .unwrap_or_else(|| self.current_version.load(Ordering::Relaxed).to_string());
                return Ok(self
                    .inner
                    .stat(ctx, &format!("{v}/{path}"), OpStat::new())
                    .await?
                    .map_metadata(|m| {
                        let mut b = m.into_builder();
                        b.etag(format!("\"{v}\"")).version(v);
                        b.build()
                    }));
            }
            self.inner.stat(ctx, path, args).await
        }
        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            let version =
                if path == "ver" {
                    Some(args.version().map(str::to_owned).unwrap_or_else(|| {
                        self.current_version.load(Ordering::Relaxed).to_string()
                    }))
                } else {
                    None
                };
            Ok(HarnessReader {
                inner: self.inner.clone(),
                ctx: ctx.clone(),
                path: path.to_owned(),
                version,
            })
        }
        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }
        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }
        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            Ok(HarnessDeleter {
                inner: self.inner.clone(),
                ctx: ctx.clone(),
                current_version: self.current_version.clone(),
            })
        }
        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }
        fn compose(
            &self,
            ctx: &OperationContext,
            to: &str,
            _: OpCompose,
        ) -> Result<Self::Composer> {
            Ok(HarnessComposer {
                inner: self.inner.clone(),
                ctx: ctx.clone(),
                to: to.to_owned(),
                parts: Vec::new(),
            })
        }
        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }
        async fn restore(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRestore,
        ) -> Result<RpRestore> {
            self.restored.fetch_add(1, Ordering::Relaxed);
            let target = args
                .version()
                .map_or_else(|| path.to_owned(), |v| format!("{v}/{path}"));
            let mut w = self.inner.write(ctx, &target, OpWrite::new())?;
            w.write(Buffer::from("restored")).await?;
            w.close().await?;
            Ok(RpRestore::default())
        }
        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    let source = memory()?;
    source.write("src", "copied").await?;
    source.write("dest", "old").await?;
    source.write("p1", "hel").await?;
    source.write("p2", "lo").await?;
    source.write("composed", "stale").await?;
    source.write("archived", "cold").await?;
    source.write("1/ver", "v1data").await?;
    source.write("2/ver", "v2data").await?;

    let current_version = Arc::new(AtomicUsize::new(1));
    let restored = Arc::new(AtomicUsize::new(0));
    let harness = MutationHarness {
        inner: source.service().clone(),
        current_version: current_version.clone(),
        restored: restored.clone(),
    };
    let op = source
        .layer(harness)
        .layer(CacheLayer::new(memory()?)?.with_ttl(Duration::MAX));

    // 1. Writer::copy_from invalidates destination.
    assert_eq!(op.read("dest").await?.to_vec(), b"old");
    let mut writer = op.writer("dest").await?;
    writer.copy_from("src", ..).await?;
    writer.close().await?;
    assert_eq!(op.read("dest").await?.to_vec(), b"copied");

    // 2. compose invalidates destination.
    assert_eq!(op.read("composed").await?.to_vec(), b"stale");
    op.compose(["p1", "p2"], "composed").await?;
    assert_eq!(op.read("composed").await?.to_vec(), b"hello");

    // 3. restore invalidates target.
    assert_eq!(op.read("archived").await?.to_vec(), b"cold");
    op.restore("archived").await?;
    assert_eq!(restored.load(Ordering::Relaxed), 1);
    assert_eq!(op.read("archived").await?.to_vec(), b"restored");

    // 4. Versioned delete invalidates both the versioned subject and unversioned current subject.
    assert_eq!(op.read("ver").await?.to_vec(), b"v1data");
    assert_eq!(op.read_with("ver").version("1").await?.to_vec(), b"v1data");
    op.delete_with("ver").version("1").await?;
    assert_eq!(
        op.read_with("ver").version("1").await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(op.read("ver").await?.to_vec(), b"v2data");

    Ok(())
}

#[tokio::test]
async fn expired_revalidation_and_cold_suffix_stat_coalesce_via_singleflight() -> Result<()> {
    // 1. Expired TTL revalidation coalesces concurrent readers into a single origin stat.
    let stat_gate = Gate::new();
    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        stat_gate: Some(stat_gate.clone()),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/hot", "abcdefgh").await?;
    let op = source.layer(probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::ZERO),
    );
    // Initial cold read populates pointer and chunks without calling stat.
    assert_eq!(op.read("hot").await?.to_vec(), b"abcdefgh");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);
    assert_eq!(probe.observed.stats.load(Ordering::Relaxed), 0);

    // Fire 8 concurrent reads against the expired pointer.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let op = op.clone();
        tasks.push(tokio::spawn(async move { op.read("hot").await }));
    }
    let _ = stat_gate.entered.acquire().await.unwrap();
    // Give sibling tasks a turn to join the in-flight revalidation before releasing.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    stat_gate.release.add_permits(8);
    for task in tasks {
        assert_eq!(task.await.unwrap()?.to_vec(), b"abcdefgh");
    }
    assert_eq!(
        probe.observed.stats.load(Ordering::Relaxed),
        1,
        "concurrent expired-TTL readers must coalesce into one origin stat"
    );
    assert_eq!(
        probe.observed.reads.load(Ordering::Relaxed),
        1,
        "revalidated readers must reuse cached chunks without extra origin reads"
    );

    // 2. Cold Suffix reads coalesce their initial length-discovery stat into one origin stat.
    let suffix_gate = Gate::new();
    let suffix_probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        stat_gate: Some(suffix_gate.clone()),
        ..Probe::default()
    };
    let suffix_source = memory()?;
    suffix_source.write("1/parquet", "0123456789abcdef").await?;
    let suffix_op = suffix_source.layer(suffix_probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );

    let mut suffix_tasks = Vec::new();
    for _ in 0..8 {
        let op = suffix_op.clone();
        suffix_tasks.push(tokio::spawn(async move {
            op.read_with("parquet").range(BytesRange::suffix(4)).await
        }));
    }
    let _ = suffix_gate.entered.acquire().await.unwrap();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    suffix_gate.release.add_permits(8);
    for task in suffix_tasks {
        assert_eq!(task.await.unwrap()?.to_vec(), b"cdef");
    }
    assert_eq!(
        suffix_probe.observed.stats.load(Ordering::Relaxed),
        1,
        "concurrent cold suffix readers must coalesce into one origin stat"
    );
    assert_eq!(
        suffix_probe.observed.reads.load(Ordering::Relaxed),
        1,
        "concurrent cold suffix readers must coalesce chunk fills"
    );
    Ok(())
}

#[tokio::test]
async fn observe_stat_preserves_matching_and_unrelated_cold_publish_but_blocks_stale() -> Result<()>
{
    let gate = Gate::new();
    let version = Arc::new(AtomicUsize::new(1));
    let probe = Probe {
        gate: Some(gate.clone()),
        version: Some(version.clone()),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/target", "v1target").await?;
    source.write("1/stale", "v1stale!").await?;
    source.write("2/stale", "v2stale!").await?;

    let op = source.clone().layer(probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );

    // Part 1: Concurrent unrelated `stat` (including misses across all shards) and a
    // matching same-subject `stat` must NOT cancel cold-read pointer publication.
    let cold_task = tokio::spawn({
        let op = op.clone();
        async move { op.read("target").await }
    });
    let _ = gate.entered.acquire().await.unwrap();
    // Run missing stats across many paths to hit every shard, plus a matching stat on `target`.
    for i in 0..300 {
        let _ = op.stat(&format!("unrelated-{i}")).await;
    }
    assert_eq!(op.stat("target").await?.etag(), Some("\"1\""));
    gate.release.add_permits(1);
    assert_eq!(cold_task.await.unwrap()?.to_vec(), b"v1target");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);

    // Pointer must have been published so the next read hits cache.
    assert_eq!(op.read("target").await?.to_vec(), b"v1target");
    assert_eq!(
        probe.observed.reads.load(Ordering::Relaxed),
        1,
        "unrelated or matching stat must not cancel cold-read pointer publish"
    );

    // Part 2: A newer contradicting `stat` on the SAME subject MUST suppress stale publish.
    let stale_gate = Gate::new();
    let stale_version = Arc::new(AtomicUsize::new(1));
    let stale_probe = Probe {
        gate: Some(stale_gate.clone()),
        version: Some(stale_version.clone()),
        ..Probe::default()
    };
    let stale_op = source.layer(stale_probe.clone()).layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    let stale_task = tokio::spawn({
        let op = stale_op.clone();
        async move { op.read("stale").await }
    });
    let _ = stale_gate.entered.acquire().await.unwrap();
    // Flip origin to version 2 while the V1 cold read is still in flight, then observe via stat.
    stale_version.store(2, Ordering::Relaxed);
    assert_eq!(stale_op.stat("stale").await?.etag(), Some("\"2\""));
    stale_gate.release.add_permits(1);
    assert_eq!(stale_task.await.unwrap()?.to_vec(), b"v1stale!");

    // Because `stat` observed version 2, the stale V1 pointer must not have been published.
    assert_eq!(stale_probe.observed.reads.load(Ordering::Relaxed), 1);
    assert_eq!(stale_op.read("stale").await?.to_vec(), b"v2stale!");
    assert_eq!(stale_probe.observed.reads.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn partial_chunk_miss_reuses_cached_trailing_chunks_without_rereading() -> Result<()> {
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, Clone, Default)]
    struct KeyCounter {
        reads: Arc<Mutex<HashMap<String, usize>>>,
    }
    impl KeyCounter {
        fn count(&self, prefix_or_key: &str) -> usize {
            self.reads
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.contains(prefix_or_key))
                .map(|(_, v)| *v)
                .sum()
        }
        fn clear(&self) {
            self.reads.lock().unwrap().clear();
        }
    }
    impl Layer for KeyCounter {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(KeyCounterService {
                inner,
                reads: self.reads.clone(),
            })
        }
    }
    #[derive(Debug)]
    struct KeyCounterService {
        inner: Servicer,
        reads: Arc<Mutex<HashMap<String, usize>>>,
    }
    impl Service for KeyCounterService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;
        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }
        fn capability(&self) -> Capability {
            self.inner.capability()
        }
        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }
        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }
        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            *self
                .reads
                .lock()
                .unwrap()
                .entry(path.to_owned())
                .or_default() += 1;
            self.inner.read(ctx, path, args)
        }
        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }
        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
            self.inner.list(ctx, path, args)
        }
        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            self.inner.delete(ctx)
        }
        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            self.inner.copy(ctx, from, to, args)
        }
        fn compose(
            &self,
            ctx: &OperationContext,
            to: &str,
            args: OpCompose,
        ) -> Result<Self::Composer> {
            self.inner.compose(ctx, to, args)
        }
        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }
        async fn restore(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpRestore,
        ) -> Result<RpRestore> {
            self.inner.restore(ctx, path, args).await
        }
        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }

    let probe = Probe {
        version: Some(Arc::new(AtomicUsize::new(1))),
        ..Probe::default()
    };
    let source = memory()?;
    source.write("1/obj", "aaaabbbbcccc").await?;

    let counter = KeyCounter::default();
    let cache = memory()?.layer(counter.clone());
    let op = source.layer(probe.clone()).layer(
        CacheLayer::new(cache.clone())?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );

    // Warm only chunks 1 and 2 (`4..12`), leaving chunk 0 uncached.
    assert_eq!(
        op.read_with("obj").range(4..12).await?.to_vec(),
        b"bbbbcccc"
    );
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 1);

    // Now read the full object (`0..12`). Chunk 0 misses and is filled from origin,
    // while chunks 1 and 2 must each be read from cache only once (never discarded & re-read).
    counter.clear();
    assert_eq!(op.read("obj").await?.to_vec(), b"aaaabbbbcccc");
    assert_eq!(probe.observed.reads.load(Ordering::Relaxed), 2);
    assert_eq!(
        counter.count("/1"),
        1,
        "chunk 1 must be read from cache once"
    );
    assert_eq!(
        counter.count("/2"),
        1,
        "chunk 2 must be read from cache once"
    );

    // Reusing one Reader across multiple ranges caches v1/meta and v1/gen in the reader
    // and invalidates immediately when a local mutation runs.
    counter.clear();
    let reader = op.reader("obj").await?;
    for offset in [0u64, 4, 8, 0, 4] {
        let buf = reader.read(offset..offset + 4).await?;
        assert_eq!(buf.len(), 4);
    }
    assert_eq!(
        counter.count("v1/gen"),
        0,
        "steady-state hot reads must reuse cached generation"
    );
    assert_eq!(
        counter.count("v1/meta/"),
        1,
        "reused Reader must resolve v1/meta once across multiple range reads"
    );

    // Mutating through the operator invalidates the reused Reader's cached pointer.
    let origin_mem = memory()?;
    origin_mem.write("mut", "old-data").await?;
    let mut_op = origin_mem.layer(
        CacheLayer::new(memory()?)?
            .with_chunk_size(4)
            .with_ttl(Duration::MAX),
    );
    let mut_reader = mut_op.reader("mut").await?;
    assert_eq!(mut_reader.read(0..8).await?.to_vec(), b"old-data");
    mut_op.write("mut", "new-data").await?;
    assert_eq!(mut_reader.read(0..8).await?.to_vec(), b"new-data");

    Ok(())
}
