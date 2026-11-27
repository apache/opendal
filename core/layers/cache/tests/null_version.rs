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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use opendal_core::raw::*;
use opendal_core::services::Memory;
use opendal_core::{
    Buffer, BytesRange, Capability, Error, ErrorKind, MetadataBuilder, OperationContext, Operator,
    Result,
};
use opendal_layer_cache::CacheLayer;

struct NullVersionEntry {
    data: Vec<u8>,
    etag: String,
    version: String,
}

#[derive(Default)]
struct NullVersionState {
    objects: HashMap<String, NullVersionEntry>,
    read_versions: Vec<Option<String>>,
    next_etag: u64,
}

#[derive(Clone)]
struct NullVersionService {
    state: Arc<Mutex<NullVersionState>>,
}

impl fmt::Debug for NullVersionService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NullVersionService").finish()
    }
}

impl NullVersionService {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(NullVersionState::default())),
        }
    }

    fn put(&self, path: &str, data: Vec<u8>, version: &str) {
        let mut guard = self.state.lock().unwrap();
        guard.next_etag += 1;
        let etag = format!("\"etag-{}\"", guard.next_etag);
        guard.objects.insert(
            path.to_owned(),
            NullVersionEntry {
                data,
                etag,
                version: version.to_owned(),
            },
        );
    }

    fn recorded_read_versions(&self) -> Vec<Option<String>> {
        self.state.lock().unwrap().read_versions.clone()
    }
}

struct NullVersionReader {
    state: Arc<Mutex<NullVersionState>>,
    path: String,
    args: OpRead,
}

struct SliceStream(Option<Buffer>);

impl oio::ReadStream for SliceStream {
    async fn read(&mut self) -> Result<Buffer> {
        Ok(self.0.take().unwrap_or_default())
    }
}

impl oio::Read for NullVersionReader {
    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let mut guard = self.state.lock().unwrap();
        guard
            .read_versions
            .push(self.args.version().map(str::to_owned));
        let entry = guard
            .objects
            .get(&self.path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "missing"))?;
        if let Some(if_match) = self.args.if_match()
            && if_match != entry.etag
        {
            return Err(Error::new(ErrorKind::ConditionNotMatch, "etag mismatch"));
        }
        let len = entry.data.len() as u64;
        let offset = range.offset().min(len);
        let end = range
            .size()
            .map_or(len, |size| offset.saturating_add(size).min(len));
        let bytes = entry.data[offset as usize..end as usize].to_vec();
        let mut builder = MetadataBuilder::file(len);
        builder.etag(&entry.etag).version(&entry.version);
        let rp = RpRead::new(builder.build());
        Ok((rp, Buffer::from(bytes)))
    }

    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (rp, buf) = self.read(range).await?;
        Ok((rp, Box::new(SliceStream(Some(buf)))))
    }
}

struct NullVersionWriter {
    state: Arc<Mutex<NullVersionState>>,
    path: String,
    version: String,
    buf: Vec<u8>,
}

impl oio::Write for NullVersionWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.buf.extend_from_slice(&bs.to_bytes());
        Ok(())
    }

    async fn close(&mut self) -> Result<opendal_core::Metadata> {
        let mut guard = self.state.lock().unwrap();
        guard.next_etag += 1;
        let etag = format!("\"etag-{}\"", guard.next_etag);
        let len = self.buf.len() as u64;
        let data = std::mem::take(&mut self.buf);
        guard.objects.insert(
            self.path.clone(),
            NullVersionEntry {
                data,
                etag: etag.clone(),
                version: self.version.clone(),
            },
        );
        let mut builder = MetadataBuilder::file(len);
        builder.etag(&etag).version(&self.version);
        Ok(builder.build())
    }

    async fn abort(&mut self) -> Result<()> {
        self.buf.clear();
        Ok(())
    }
}

impl Service for NullVersionService {
    type Reader = NullVersionReader;
    type Writer = NullVersionWriter;
    type Lister = ();
    type Deleter = ();
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("s3", "/", "null-version-test")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            stat_with_version: true,
            read: true,
            read_with_version: true,
            read_with_if_match: true,
            write: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Service::create_dir(&(), ctx, path, args).await
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let guard = self.state.lock().unwrap();
        let entry = guard
            .objects
            .get(path)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "missing"))?;
        let mut builder = MetadataBuilder::file(entry.data.len() as u64);
        builder.etag(&entry.etag).version(&entry.version);
        Ok(RpStat::new(builder.build()))
    }

    fn read(&self, _: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(NullVersionReader {
            state: self.state.clone(),
            path: path.to_owned(),
            args,
        })
    }

    fn write(&self, _: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        Ok(NullVersionWriter {
            state: self.state.clone(),
            path: path.to_owned(),
            version: "null".to_owned(),
            buf: Vec::new(),
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Service::delete(&(), ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        Service::list(&(), ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        Service::copy(&(), ctx, from, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        Service::rename(&(), ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        Service::presign(&(), ctx, path, args).await
    }
}

#[tokio::test]
async fn unversioned_multi_chunk_read_does_not_pin_null_version() -> Result<()> {
    let source = NullVersionService::new();
    // 20 chunks > FILL_CHUNKS (16) so a full read triggers a second chunk fill.
    let payload: Vec<u8> = (0..(20 * 16)).map(|i| (i % 251) as u8).collect();
    source.put("large.bin", payload.clone(), "null");

    let cache = Operator::new(Memory::default())?;
    let op = Operator::from_parts(OperationContext::new(), Arc::new(source.clone())).layer(
        CacheLayer::new(cache)?
            .with_chunk_size(16)
            .with_ttl(Duration::from_secs(60)),
    );

    let got = op.read("large.bin").await?.to_vec();
    assert_eq!(got, payload);

    let versions = source.recorded_read_versions();
    assert!(
        versions.len() >= 2,
        "expected multiple chunk reads, got {versions:?}"
    );
    assert!(
        versions.iter().all(Option::is_none),
        "unversioned read must not pin mutable 'null' version on follow-up chunks: {versions:?}"
    );
    Ok(())
}

#[tokio::test]
async fn explicit_null_version_read_sees_local_overwrite_immediately() -> Result<()> {
    let source = NullVersionService::new();
    source.put("item.txt", b"old-bytes".to_vec(), "null");

    let cache = Operator::new(Memory::default())?;
    let op = Operator::from_parts(OperationContext::new(), Arc::new(source.clone())).layer(
        CacheLayer::new(cache)?
            .with_chunk_size(1024)
            .with_ttl(Duration::from_secs(3600)),
    );

    let first = op.read_with("item.txt").version("null").await?.to_vec();
    assert_eq!(first, b"old-bytes");

    op.write("item.txt", "new-bytes").await?;

    let second = op.read_with("item.txt").version("null").await?.to_vec();
    assert_eq!(second, b"new-bytes");
    Ok(())
}
