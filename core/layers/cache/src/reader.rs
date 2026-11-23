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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use opendal_core::options::{ReadOptions, StatOptions};
use opendal_core::raw::oio::{Read, ReadStream, ReadStreamDyn, Reader};
use opendal_core::raw::{OpRead, OpStat, RpRead, Service, Servicer, Timestamp};
use opendal_core::{Buffer, BytesRange, Error, ErrorKind, Metadata, OperationContext, Result};

use crate::key::{Identity, Subject, Tier, data_prefix, strong_etag};
use crate::record::Pointer;
use crate::state::State;

// Bound both buffered bytes and the number of cache operations per fill. A
// configured chunk larger than this budget still fills one chunk at a time.
const FILL_BYTES: u64 = 4 * 1024 * 1024;
const FILL_CHUNKS: u64 = 16;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) generation: u64,
    pub(crate) epoch: u64,
}

#[derive(Clone)]
pub(crate) struct CachedReader {
    pub(crate) state: Arc<State>,
    pub(crate) source: Servicer,
    pub(crate) context: OperationContext,
    pub(crate) path: String,
    pub(crate) args: OpRead,
    pub(crate) subject: Subject,
    pub(crate) chunk_size: u64,
    pub(crate) ttl: Duration,
}

#[derive(Clone, Debug)]
pub(crate) enum FillError {
    Changed,
    Unverifiable(Arc<Error>),
    Origin(Arc<Error>),
}

impl From<Error> for FillError {
    fn from(error: Error) -> Self {
        Self::Origin(Arc::new(error))
    }
}

impl FillError {
    fn into_error(self) -> Error {
        match self {
            Self::Changed => Error::new(ErrorKind::Unexpected, "object changed during cache read")
                .set_temporary(),
            Self::Unverifiable(error) => Error::new(
                ErrorKind::Unexpected,
                "cannot verify object identity during cache read",
            )
            .set_temporary()
            .set_source(error),
            Self::Origin(error) => Arc::try_unwrap(error).unwrap_or_else(|error| {
                let shared = Error::new(error.kind(), error.message());
                let shared = if error.is_temporary() {
                    shared.set_temporary()
                } else if error.is_persistent() {
                    shared.set_persistent()
                } else {
                    shared
                };
                shared.set_source(error)
            }),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct DiscoveryKey {
    snapshot: Snapshot,
    subject: Subject,
    index: u64,
    chunk_size: u64,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct FillKey {
    epoch: u64,
    key: String,
}

#[derive(Clone)]
pub(crate) struct Unit {
    start: u64,
    data: Buffer,
}

impl Unit {
    fn end(&self) -> u64 {
        self.start + self.data.len() as u64
    }
}

#[derive(Clone)]
pub(crate) struct Discovery {
    pointer: Pointer,
    unit: Option<Unit>,
}

pub(crate) type DiscoveryResult = std::result::Result<Option<Discovery>, FillError>;
pub(crate) type FillResult = std::result::Result<Unit, FillError>;
type Prepared = std::result::Result<Option<PreparedStream>, FillError>;

impl CachedReader {
    fn unit_chunks(&self) -> u64 {
        (FILL_BYTES / self.chunk_size).clamp(1, FILL_CHUNKS)
    }

    fn unit_end(&self, start: u64, end: u64) -> u64 {
        let span = self.chunk_size.saturating_mul(self.unit_chunks());
        let last = ((end - 1) / self.chunk_size).saturating_add(1);
        start
            .saturating_add(span)
            .min(last.saturating_mul(self.chunk_size))
    }

    fn normalized(&self, range: BytesRange, length: u64) -> Option<Range<u64>> {
        match range {
            BytesRange::Range { offset, size } if offset < length && size != Some(0) => {
                Some(offset..size.map_or(length, |size| offset.saturating_add(size).min(length)))
            }
            BytesRange::Suffix { size } if size > 0 && length > 0 => {
                Some(length.saturating_sub(size)..length)
            }
            _ => None,
        }
    }

    fn origin(&self) -> Result<Reader> {
        self.source
            .read(&self.context, &self.path, self.args.clone())
    }

    async fn stat(&self, version: Option<&str>) -> Result<Metadata> {
        let options = StatOptions {
            version: version.map(str::to_owned),
            ..Default::default()
        };
        Ok(self
            .source
            .stat(&self.context, &self.path, OpStat::from(options))
            .await?
            .into_metadata())
    }

    async fn prepare(&self, range: BytesRange) -> Prepared {
        if range.size() == Some(0) {
            return Ok(None);
        }
        let Some(generation) = self.state.generation().await else {
            return Ok(None);
        };
        let snapshot = Snapshot {
            generation,
            epoch: self.state.epoch(&self.subject).await,
        };
        let cached = self.state.pointer(generation, &self.subject).await;
        if let Some(pointer) = cached {
            if pointer.is_fresh(self.ttl, self.args.version()) {
                if pointer.identity.tier != Tier::Nonce {
                    return self.prepare_verified(snapshot, pointer, range, None).await;
                }
                if let Some(stream) = self.cached_nonce(snapshot, pointer.clone(), range).await {
                    return Ok(Some(PreparedStream::Cached(stream)));
                }
                let length = pointer.metadata.content_length();
                let Some(bytes) = self.normalized(range, length) else {
                    return Ok(None);
                };
                // Fill missing chunks under the existing fresh Tier 4 pointer
                // without rotating the nonce or orphaning already-cached chunks.
                return self
                    .prepare_nonce_fill(snapshot, Some(pointer), length, bytes)
                    .await;
            } else if pointer.identity.tier != Tier::Nonce {
                let observed_at = Timestamp::now();
                match self.stat(self.args.version()).await {
                    Ok(metadata) => {
                        let current = Pointer::new(metadata, observed_at);
                        if current.identity.tier != Tier::Nonce {
                            self.state.publish(snapshot, &self.subject, &current).await;
                            return self.prepare_verified(snapshot, current, range, None).await;
                        }
                    }
                    Err(error) if error.kind() != ErrorKind::NotFound => return Ok(None),
                    Err(_) => {
                        self.state.discard(snapshot, &self.subject, &pointer).await;
                        return Ok(None);
                    }
                }
            }
            self.state.discard(snapshot, &self.subject, &pointer).await;
        }

        let forward = match range {
            BytesRange::Range { offset, size } => {
                offset..size.map_or(u64::MAX, |size| offset.saturating_add(size))
            }
            BytesRange::Suffix { .. } => {
                // Resolve total length via `stat` when available to avoid an extra
                // GET whose body would be discarded before aligned chunk fetching.
                let observed_at = Timestamp::now();
                let mut metadata = None;
                if self.source.capability().stat {
                    match self.stat(self.args.version()).await {
                        Ok(meta) if meta.is_file() => metadata = Some(meta),
                        Ok(_) | Err(_) => return Ok(None),
                    }
                } else if self.source.capability().read_with_suffix {
                    let limit = BytesRange::suffix(range.size().unwrap().min(self.chunk_size));
                    match self.origin()?.open(limit).await {
                        Ok((response, _)) => {
                            metadata = response.into_metadata().filter(Metadata::is_file);
                        }
                        Err(error) if error.kind() == ErrorKind::RangeNotSatisfied => {
                            return Ok(None);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                let Some(metadata) = metadata else {
                    return Ok(None);
                };
                if Identity::verifiable(&metadata).is_some() {
                    let pointer = Pointer::new(metadata, observed_at);
                    self.state.publish(snapshot, &self.subject, &pointer).await;
                    return self.prepare_verified(snapshot, pointer, range, None).await;
                }
                let Some(forward) = self.normalized(range, metadata.content_length()) else {
                    return Ok(None);
                };
                forward
            }
        };
        if forward.is_empty() {
            return Ok(None);
        }
        let start = forward.start / self.chunk_size * self.chunk_size;
        let aligned = start..self.unit_end(start, forward.end);
        let keys = (start / self.chunk_size..=(aligned.end - 1) / self.chunk_size)
            .map(|index| DiscoveryKey {
                snapshot,
                subject: self.subject,
                index,
                chunk_size: self.chunk_size,
            })
            .collect();
        let reader = self.clone();
        let Some(result) = self
            .state
            .discoveries
            .run(self.context.executor(), keys, move |count| async move {
                let end = start.saturating_add(reader.chunk_size.saturating_mul(count as u64));
                reader.discover(snapshot, start..end.min(aligned.end)).await
            })
            .await
        else {
            return Ok(None);
        };
        let Some(discovery) = result? else {
            return Ok(None);
        };
        if discovery.pointer.identity.tier == Tier::Nonce {
            let Some(bytes) = self.normalized(range, discovery.pointer.metadata.content_length())
            else {
                return Ok(None);
            };
            if let Some(unit) = discovery
                .unit
                .filter(|unit| unit.start <= bytes.start && unit.end() >= bytes.end)
            {
                return Ok(Some(PreparedStream::Cached(CachedStream::new(
                    self.clone(),
                    snapshot,
                    discovery.pointer,
                    bytes,
                    vec![unit],
                ))));
            }
            return self
                .prepare_nonce_fill(
                    snapshot,
                    None,
                    discovery.pointer.metadata.content_length(),
                    bytes,
                )
                .await;
        }
        self.prepare_verified(snapshot, discovery.pointer, range, discovery.unit)
            .await
    }

    async fn prepare_nonce_fill(
        &self,
        snapshot: Snapshot,
        existing: Option<Pointer>,
        expected_length: u64,
        requested: Range<u64>,
    ) -> Prepared {
        let aligned_start = requested.start / self.chunk_size * self.chunk_size;
        let aligned_end = (((requested.end - 1) / self.chunk_size) + 1)
            .saturating_mul(self.chunk_size)
            .min(expected_length);
        let observed_at = Timestamp::now();
        let origin = self.origin()?;
        let (response, inner) = match origin
            .open(BytesRange::new(
                aligned_start,
                Some(aligned_end - aligned_start),
            ))
            .await
        {
            Ok(opened) => opened,
            Err(error) => {
                let error = fill_open_error(error);
                if matches!(error, FillError::Changed)
                    && let Some(pointer) = &existing
                {
                    self.state.discard(snapshot, &self.subject, pointer).await;
                }
                return Err(error);
            }
        };
        let Some(metadata) = response.into_metadata().filter(Metadata::is_file) else {
            return Ok(None);
        };
        if metadata.content_length() != expected_length || Identity::verifiable(&metadata).is_some()
        {
            if let Some(pointer) = &existing {
                self.state.discard(snapshot, &self.subject, pointer).await;
            }
            return Err(FillError::Changed);
        }
        let publish_pointer = existing.is_none();
        let pointer = existing.unwrap_or_else(|| Pointer::new(metadata, observed_at));
        let prefix = data_prefix(
            snapshot.generation,
            &self.subject,
            pointer.identity,
            self.chunk_size,
        );
        Ok(Some(PreparedStream::NonceFill(NonceFillStream {
            reader: self.clone(),
            snapshot,
            pointer,
            publish_pointer,
            prefix,
            inner,
            aligned_offset: aligned_start,
            aligned_end,
            requested,
            chunk_index: aligned_start / self.chunk_size,
            chunk_parts: Vec::new(),
            chunk_len: 0,
            done: false,
            terminal: None,
        })))
    }

    async fn discover(&self, snapshot: Snapshot, aligned: Range<u64>) -> DiscoveryResult {
        let observed_at = Timestamp::now();
        let origin = self.origin()?;
        let (response, mut stream) = match origin
            .open(BytesRange::new(
                aligned.start,
                Some(aligned.end - aligned.start),
            ))
            .await
        {
            Ok(result) => result,
            Err(error) if error.kind() == ErrorKind::RangeNotSatisfied => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let response = response.into_metadata().filter(Metadata::is_file);
        let mut use_body = true;
        let metadata = if let Some(metadata) = response
            .as_ref()
            .filter(|metadata| Identity::verifiable(metadata).is_some())
        {
            metadata.clone()
        } else if self.source.capability().stat {
            match self.stat(self.args.version()).await {
                Ok(metadata) if metadata.is_file() => {
                    if response.as_ref().is_some_and(|response| {
                        response.content_length() != metadata.content_length()
                    }) {
                        return Err(FillError::Changed);
                    }
                    use_body = response.is_some() && Identity::verifiable(&metadata).is_none();
                    // Only the metadata returned with these bytes can describe
                    // a validator-free snapshot. Stat still confirms length.
                    if use_body {
                        response.unwrap()
                    } else {
                        metadata
                    }
                }
                Ok(_) => return Ok(None),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return if response.is_some() {
                        Err(FillError::Changed)
                    } else {
                        Ok(None)
                    };
                }
                Err(error) if error.kind() == ErrorKind::Unsupported => match response {
                    Some(metadata) => metadata,
                    None => return Ok(None),
                },
                Err(error) => return Err(FillError::Unverifiable(Arc::new(error))),
            }
        } else {
            match response {
                Some(metadata) => metadata,
                None => return Ok(None),
            }
        };
        let pointer = Pointer::new(metadata, observed_at);
        let length = pointer.metadata.content_length();
        if aligned.start >= length {
            return Ok(None);
        }
        let unit = if use_body {
            let end = aligned.end.min(length);
            let data = read_unit(&mut stream, end - aligned.start)
                .await
                .map_err(fill_open_error)?;
            if data.len() as u64 != end - aligned.start {
                return Err(FillError::Changed);
            }
            let unit = Unit {
                start: aligned.start,
                data,
            };
            self.store_unit(snapshot, &pointer, &unit).await;
            Some(unit)
        } else {
            if pointer.identity.tier == Tier::Nonce {
                return Ok(None);
            }
            None
        };
        self.state.publish(snapshot, &self.subject, &pointer).await;
        Ok(Some(Discovery { pointer, unit }))
    }

    async fn prepare_verified(
        &self,
        snapshot: Snapshot,
        pointer: Pointer,
        range: BytesRange,
        unit: Option<Unit>,
    ) -> Prepared {
        let Some(bytes) = self.normalized(range, pointer.metadata.content_length()) else {
            return Ok(None);
        };
        let unit = unit.filter(|unit| unit.start <= bytes.start && bytes.start < unit.end());
        let mut stream = CachedStream::new(
            self.clone(),
            snapshot,
            pointer,
            bytes,
            unit.into_iter().collect(),
        );
        if stream.units.is_empty() {
            match stream.fetch().await {
                Ok(unit) => stream.units.push(unit),
                Err(error) => {
                    if matches!(error, FillError::Changed) {
                        self.state
                            .discard(snapshot, &self.subject, &stream.pointer)
                            .await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(Some(PreparedStream::Cached(stream)))
    }

    async fn cached_nonce(
        &self,
        snapshot: Snapshot,
        pointer: Pointer,
        range: BytesRange,
    ) -> Option<CachedStream> {
        let bytes = self.normalized(range, pointer.metadata.content_length())?;
        let first = bytes.start / self.chunk_size;
        let last = (bytes.end - 1) / self.chunk_size;
        let mut stream = CachedStream::new(self.clone(), snapshot, pointer, bytes, Vec::new());
        if last - first < self.unit_chunks() {
            for index in (first..=last).rev() {
                stream.units.push(stream.cached_chunk(index).await?);
            }
            return Some(stream);
        }
        // Large Tier 4 range: verify all chunks are present without buffering
        // the full range in memory, then stream lazily from cache.
        let first_unit = stream.cached_chunk(first).await?;
        if !stream.has_cached_chunk(last).await {
            return None;
        }
        for index in (first + 1)..last {
            if !stream.has_cached_chunk(index).await {
                return None;
            }
        }
        stream.units.push(first_unit);
        Some(stream)
    }

    async fn store_unit(&self, snapshot: Snapshot, pointer: &Pointer, unit: &Unit) {
        let prefix = data_prefix(
            snapshot.generation,
            &self.subject,
            pointer.identity,
            self.chunk_size,
        );
        let mut offset = 0;
        let mut index = unit.start / self.chunk_size;
        while offset < unit.data.len() {
            let end = offset
                .saturating_add(self.chunk_size as usize)
                .min(unit.data.len());
            let _ = self
                .state
                .cache
                .write(&format!("{prefix}/{index:x}"), unit.data.slice(offset..end))
                .await;
            offset = end;
            index += 1;
        }
    }

    async fn fetch_unit(
        &self,
        snapshot: Snapshot,
        pointer: Pointer,
        start: u64,
        count: usize,
    ) -> FillResult {
        let length = pointer.metadata.content_length();
        let end = start
            .saturating_add(self.chunk_size.saturating_mul(count as u64))
            .min(length);
        let cap = self.source.capability();
        let version = if cap.read_with_version {
            pointer.metadata.version().or(self.args.version())
        } else {
            self.args.version()
        };
        let etag = cap
            .read_with_if_match
            .then(|| strong_etag(&pointer.metadata))
            .flatten();
        let args = if version != self.args.version() || etag.is_some() {
            let options = ReadOptions {
                version: version.map(str::to_owned),
                if_match: etag.map(str::to_owned),
                ..Default::default()
            };
            let (_, args, _) = options.into();
            args
        } else {
            self.args.clone()
        };
        let range = BytesRange::new(start, Some(end - start));
        let origin = self.source.read(&self.context, &self.path, args)?;
        let (response, mut stream) = origin.open(range).await.map_err(fill_open_error)?;
        let verified = verify_response(&pointer, response.metadata(), version, etag)?;
        let data = read_unit(&mut stream, end - start)
            .await
            .map_err(fill_open_error)?;
        if !verified {
            let stat_version = if cap.stat_with_version {
                version
            } else {
                self.args.version()
            };
            self.verify_stat(&pointer, stat_version).await?;
        }
        if data.len() as u64 != end - start {
            return Err(FillError::Changed);
        }
        let unit = Unit { start, data };
        self.store_unit(snapshot, &pointer, &unit).await;
        Ok(unit)
    }

    async fn verify_stat(
        &self,
        pointer: &Pointer,
        version: Option<&str>,
    ) -> std::result::Result<(), FillError> {
        let metadata = self.stat(version).await.map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                FillError::Changed
            } else {
                FillError::Unverifiable(Arc::new(error))
            }
        })?;
        if metadata.content_length() != pointer.metadata.content_length()
            || Identity::verifiable(&metadata) != Some(pointer.identity)
        {
            return Err(FillError::Changed);
        }
        Ok(())
    }
}

fn fill_open_error(error: Error) -> FillError {
    match error.kind() {
        ErrorKind::ConditionNotMatch | ErrorKind::NotFound | ErrorKind::RangeNotSatisfied => {
            FillError::Changed
        }
        _ => error.into(),
    }
}

fn verify_response(
    pointer: &Pointer,
    response: Option<&Metadata>,
    version: Option<&str>,
    etag: Option<&str>,
) -> std::result::Result<bool, FillError> {
    let expected = &pointer.metadata;
    if let Some(response) = response
        && (response.is_file() && response.content_length() != expected.content_length()
            || matches!((expected.version(), response.version()), (Some(a), Some(b)) if a != b)
            || matches!((strong_etag(expected), strong_etag(response)), (Some(a), Some(b)) if a != b)
            || (pointer.identity.tier == Tier::Modified
                && response.last_modified().is_some()
                && response.last_modified() != expected.last_modified()))
    {
        return Err(FillError::Changed);
    }
    let version = version.is_some() && version == expected.version()
        || response
            .and_then(Metadata::version)
            .is_some_and(|v| Some(v) == expected.version());
    let etag = etag.is_some()
        || response
            .and_then(strong_etag)
            .is_some_and(|v| Some(v) == strong_etag(expected));
    Ok(match pointer.identity.tier {
        Tier::Version => version && (strong_etag(expected).is_none() || etag),
        Tier::Etag => etag,
        Tier::Modified => response
            .filter(|metadata| metadata.is_file())
            .and_then(Metadata::last_modified)
            .is_some_and(|v| Some(v) == expected.last_modified()),
        Tier::Nonce => false,
    })
}

async fn read_unit(stream: &mut Box<dyn ReadStreamDyn>, expected: u64) -> Result<Buffer> {
    let mut bytes = 0_u64;
    let mut parts = Vec::new();
    loop {
        let next = stream.read().await?;
        if next.is_empty() {
            return Ok(parts.into_iter().flatten().collect());
        }
        bytes = bytes.saturating_add(next.len() as u64);
        if bytes > expected {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "origin exceeded the aligned read range",
            ));
        }
        parts.push(next);
    }
}

struct CachedStream {
    reader: CachedReader,
    snapshot: Snapshot,
    pointer: Pointer,
    prefix: String,
    remaining: Range<u64>,
    units: Vec<Unit>,
    terminal: Option<FillError>,
}

impl CachedStream {
    fn new(
        reader: CachedReader,
        snapshot: Snapshot,
        pointer: Pointer,
        remaining: Range<u64>,
        units: Vec<Unit>,
    ) -> Self {
        let prefix = data_prefix(
            snapshot.generation,
            &reader.subject,
            pointer.identity,
            reader.chunk_size,
        );
        Self {
            reader,
            snapshot,
            pointer,
            prefix,
            remaining,
            units,
            terminal: None,
        }
    }

    async fn cached_chunk(&self, index: u64) -> Option<Unit> {
        let start = index * self.reader.chunk_size;
        let expected = (self.pointer.metadata.content_length() - start).min(self.reader.chunk_size);
        let data = self
            .reader
            .state
            .cache
            .read(&format!("{}/{index:x}", self.prefix))
            .await
            .ok()?;
        (data.len() as u64 == expected).then_some(Unit { start, data })
    }

    async fn has_cached_chunk(&self, index: u64) -> bool {
        let start = index * self.reader.chunk_size;
        let expected = (self.pointer.metadata.content_length() - start).min(self.reader.chunk_size);
        let key = format!("{}/{index:x}", self.prefix);
        if self.reader.state.cache.info().capability().stat {
            self.reader
                .state
                .cache
                .stat(&key)
                .await
                .is_ok_and(|meta| meta.is_file() && meta.content_length() == expected)
        } else {
            self.cached_chunk(index).await.is_some()
        }
    }

    async fn fetch(&self) -> FillResult {
        let chunk_size = self.reader.chunk_size;
        let first = self.remaining.start / chunk_size;
        if let Some(hit) = self.cached_chunk(first).await {
            return Ok(hit);
        }
        if self.pointer.identity.tier == Tier::Nonce {
            return Err(FillError::Changed);
        }
        let last = ((self.remaining.end - 1) / chunk_size)
            .min(first.saturating_add(self.reader.unit_chunks() - 1));
        let mut keys = Vec::new();
        for index in first..=last {
            if index != first && self.cached_chunk(index).await.is_some() {
                break;
            }
            keys.push(FillKey {
                epoch: self.snapshot.epoch,
                key: format!("{}/{index:x}", self.prefix),
            });
        }
        let count = keys.len();
        let reader = self.reader.clone();
        let pointer = self.pointer.clone();
        let snapshot = self.snapshot;
        match self
            .reader
            .state
            .fills
            .run(
                self.reader.context.executor(),
                keys,
                move |count| async move {
                    reader
                        .fetch_unit(snapshot, pointer, first * chunk_size, count)
                        .await
                },
            )
            .await
        {
            Some(result) => result,
            None => {
                // If the executor dropped the shared fill task without running it
                // to completion, fall back to fetching the unit inline.
                self.reader
                    .fetch_unit(
                        self.snapshot,
                        self.pointer.clone(),
                        first * chunk_size,
                        count,
                    )
                    .await
            }
        }
    }

    async fn next(&mut self) -> std::result::Result<Buffer, FillError> {
        if let Some(error) = &self.terminal {
            return Err(error.clone());
        }
        if self.remaining.is_empty() {
            return Ok(Buffer::new());
        }
        let unit = if let Some(unit) = self.units.pop() {
            unit
        } else {
            match self.fetch().await {
                Ok(unit) => unit,
                Err(error) => {
                    self.terminal = Some(error.clone());
                    if matches!(error, FillError::Changed) {
                        self.reader
                            .state
                            .discard(self.snapshot, &self.reader.subject, &self.pointer)
                            .await;
                    }
                    return Err(error);
                }
            }
        };
        let start = self.remaining.start;
        let end = self.remaining.end.min(unit.end());
        self.remaining.start = end;
        Ok(unit
            .data
            .slice((start - unit.start) as usize..(end - unit.start) as usize))
    }
}

struct NonceFillStream {
    reader: CachedReader,
    snapshot: Snapshot,
    pointer: Pointer,
    publish_pointer: bool,
    prefix: String,
    inner: Box<dyn ReadStreamDyn>,
    aligned_offset: u64,
    aligned_end: u64,
    requested: Range<u64>,
    chunk_index: u64,
    chunk_parts: Vec<Buffer>,
    chunk_len: usize,
    done: bool,
    terminal: Option<FillError>,
}

impl NonceFillStream {
    async fn flush_chunk(&mut self) {
        if self.chunk_len == 0 {
            return;
        }
        let data: Buffer = self.chunk_parts.drain(..).flatten().collect();
        self.chunk_len = 0;
        if self.reader.state.epoch(&self.reader.subject).await == self.snapshot.epoch {
            let _ = self
                .reader
                .state
                .cache
                .write(&format!("{}/{:x}", self.prefix, self.chunk_index), data)
                .await;
        }
        self.chunk_index += 1;
    }

    async fn finish(&mut self) {
        self.flush_chunk().await;
        if self.publish_pointer {
            self.reader
                .state
                .publish(self.snapshot, &self.reader.subject, &self.pointer)
                .await;
        }
        self.done = true;
    }

    async fn next(&mut self) -> std::result::Result<Buffer, FillError> {
        if let Some(error) = &self.terminal {
            return Err(error.clone());
        }
        if self.done {
            return Ok(Buffer::new());
        }
        let chunk_size = self.reader.chunk_size as usize;
        loop {
            let buf = match self.inner.read().await {
                Ok(buf) => buf,
                Err(error) => {
                    let error = fill_open_error(error);
                    if matches!(error, FillError::Changed) && !self.publish_pointer {
                        self.reader
                            .state
                            .discard(self.snapshot, &self.reader.subject, &self.pointer)
                            .await;
                    }
                    self.terminal = Some(error.clone());
                    return Err(error);
                }
            };
            if buf.is_empty() {
                if self.aligned_offset != self.aligned_end {
                    let error = FillError::Changed;
                    if !self.publish_pointer {
                        self.reader
                            .state
                            .discard(self.snapshot, &self.reader.subject, &self.pointer)
                            .await;
                    }
                    self.terminal = Some(error.clone());
                    return Err(error);
                }
                self.finish().await;
                return Ok(Buffer::new());
            }
            let buf_start = self.aligned_offset;
            let buf_end = buf_start.saturating_add(buf.len() as u64);
            if buf_end > self.aligned_end {
                let error = FillError::from(Error::new(
                    ErrorKind::Unexpected,
                    "origin exceeded the aligned read range",
                ));
                self.terminal = Some(error.clone());
                return Err(error);
            }
            self.aligned_offset = buf_end;

            let mut remaining = buf.clone();
            while !remaining.is_empty() {
                let need = chunk_size - self.chunk_len;
                let take = need.min(remaining.len());
                self.chunk_parts.push(remaining.slice(..take));
                self.chunk_len += take;
                remaining = remaining.slice(take..);
                if self.chunk_len == chunk_size {
                    self.flush_chunk().await;
                }
            }

            if self.aligned_offset == self.aligned_end {
                self.finish().await;
            }

            let out_start = buf_start.max(self.requested.start);
            let out_end = buf_end.min(self.requested.end);
            if out_start < out_end {
                return Ok(
                    buf.slice((out_start - buf_start) as usize..(out_end - buf_start) as usize)
                );
            }
            if self.done {
                return Ok(Buffer::new());
            }
        }
    }
}

enum PreparedStream {
    Cached(CachedStream),
    NonceFill(NonceFillStream),
}

impl PreparedStream {
    fn pointer(&self) -> &Pointer {
        match self {
            Self::Cached(stream) => &stream.pointer,
            Self::NonceFill(stream) => &stream.pointer,
        }
    }

    fn into_pointer(self) -> Pointer {
        match self {
            Self::Cached(stream) => stream.pointer,
            Self::NonceFill(stream) => stream.pointer,
        }
    }

    async fn next(&mut self) -> std::result::Result<Buffer, FillError> {
        match self {
            Self::Cached(stream) => stream.next().await,
            Self::NonceFill(stream) => stream.next().await,
        }
    }
}

impl ReadStream for PreparedStream {
    async fn read(&mut self) -> Result<Buffer> {
        self.next().await.map_err(FillError::into_error)
    }
}

impl Read for CachedReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn ReadStreamDyn>)> {
        let mut prepared = self.prepare(range).await;
        if matches!(prepared, Err(FillError::Changed)) {
            prepared = self.prepare(range).await;
        }
        match prepared {
            Ok(Some(stream)) => Ok((
                RpRead::new(stream.pointer().metadata.clone()),
                Box::new(stream),
            )),
            Ok(None) | Err(FillError::Unverifiable(_)) => self.origin()?.open(range).await,
            Err(error) => Err(error.into_error()),
        }
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let Some(expected) = range.size() else {
            return self.origin()?.read(range).await;
        };
        let mut last_error = FillError::Changed;
        for attempt in 0..2 {
            let mut stream = match self.prepare(range).await {
                Ok(Some(stream)) => stream,
                Ok(None) | Err(FillError::Unverifiable(_)) => {
                    return self.origin()?.read(range).await;
                }
                Err(error @ FillError::Changed) if attempt == 0 => {
                    last_error = error;
                    continue;
                }
                Err(error) => return Err(error.into_error()),
            };
            let mut parts = Vec::new();
            let mut retry = false;
            loop {
                match stream.next().await {
                    Ok(part) if part.is_empty() => break,
                    Ok(part) => parts.push(part),
                    Err(error @ FillError::Changed) if attempt == 0 => {
                        last_error = error;
                        retry = true;
                        break;
                    }
                    Err(FillError::Unverifiable(_)) => return self.origin()?.read(range).await,
                    Err(error) => return Err(error.into_error()),
                }
            }
            if retry {
                continue;
            }
            let data: Buffer = parts.into_iter().flatten().collect();
            let bound = if range.is_suffix() {
                expected.min(stream.pointer().metadata.content_length())
            } else {
                expected
            };
            if data.len() as u64 != bound {
                // Defer exact-read EOF error classification to the source.
                return self.origin()?.read(range).await;
            }
            return Ok((RpRead::new(stream.into_pointer().metadata), data));
        }
        Err(last_error.into_error())
    }
}
