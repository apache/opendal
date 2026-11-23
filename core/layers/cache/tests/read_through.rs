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

use opendal_core::options::{DeleteOptions, ReadOptions};
use opendal_core::raw::oio::{self, Read, ReadStream};
use opendal_core::raw::*;
use opendal_core::services::Memory;
use opendal_core::*;
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
