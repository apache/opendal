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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::future::join_all;
use opendal_core::options::{ReadOptions, StatOptions};
use opendal_core::raw::oio::{Read, ReadStream, ReadStreamDyn, Reader};
use opendal_core::raw::{Instant, OpRead, OpStat, RpRead, Service, Servicer, Timestamp};
use opendal_core::{Buffer, BytesRange, Error, ErrorKind, Metadata, OperationContext, Result};

use crate::key::{Identity, Subject, Tier, data_prefix, is_immutable_version, strong_etag};
use crate::record::Pointer;
use crate::state::State;

// Bound both buffered bytes and the number of cache operations per fill. A
// configured chunk larger than this budget still fills one chunk at a time.
const FILL_BYTES: u64 = 4 * 1024 * 1024;
const FILL_CHUNKS: u64 = 16;
const LOCAL_POINTER_WINDOW: Duration = Duration::from_secs(1);

pub(crate) struct LocalPointer {
    epoch: u64,
    pointer: Pointer,
    remembered_at: Instant,
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
    pub(crate) last_pointer: Arc<RwLock<Option<LocalPointer>>>,
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
    epoch: u64,
    subject: Subject,
    index: u64,
    chunk_size: u64,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct RevalidationKey {
    epoch: u64,
    subject: Subject,
}

#[derive(Clone)]
pub(crate) enum RevalidationResult {
    Verified { pointer: Pointer, published: bool },
    NotFound,
    Fallback,
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
    published: bool,
}

pub(crate) type DiscoveryResult = std::result::Result<Option<Discovery>, FillError>;
pub(crate) type FillResult = std::result::Result<Unit, FillError>;
type Prepared = std::result::Result<Option<CachedStream>, FillError>;

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

    fn local_pointer_window(&self) -> Duration {
        self.ttl.min(LOCAL_POINTER_WINDOW)
    }

    fn local_pointer(&self, epoch: u64) -> Option<Pointer> {
        self.local_pointer_at(epoch, Instant::now())
    }

    fn local_pointer_at(&self, epoch: u64, now: Instant) -> Option<Pointer> {
        let window = self.local_pointer_window();
        if window.is_zero() {
            return None;
        }
        let guard = self.last_pointer.read().unwrap();
        let cached = guard.as_ref()?;
        (cached.epoch == epoch
            && now.saturating_duration_since(cached.remembered_at) < window
            && cached.pointer.is_fresh(self.ttl, self.args.version()))
        .then(|| cached.pointer.clone())
    }

    fn remember_pointer(&self, epoch: u64, pointer: &Pointer) {
        self.remember_pointer_at(epoch, pointer, Instant::now());
    }

    fn remember_pointer_at(&self, epoch: u64, pointer: &Pointer, now: Instant) {
        if !self.local_pointer_window().is_zero() && pointer.is_fresh(self.ttl, self.args.version())
        {
            *self.last_pointer.write().unwrap() = Some(LocalPointer {
                epoch,
                pointer: pointer.clone(),
                remembered_at: now,
            });
        }
    }

    fn forget_pointer(&self, epoch: u64) {
        let mut guard = self.last_pointer.write().unwrap();
        if guard.as_ref().is_some_and(|cached| cached.epoch == epoch) {
            *guard = None;
        }
    }

    async fn revalidate_stat(&self, epoch: u64, stale: Option<Pointer>) -> RevalidationResult {
        let reader = self.clone();
        let key = RevalidationKey {
            epoch,
            subject: self.subject,
        };
        self.state
            .revalidations
            .run(self.context.executor(), vec![key], move |_| async move {
                let observed_at = Timestamp::now();
                match reader.stat(reader.args.version()).await {
                    Ok(metadata) if metadata.is_file() => {
                        if let Some(current) = Pointer::from_metadata(metadata, observed_at) {
                            let published =
                                reader.state.publish(epoch, &reader.subject, &current).await;
                            RevalidationResult::Verified {
                                pointer: current,
                                published,
                            }
                        } else {
                            if let Some(pointer) = &stale {
                                reader.state.discard(epoch, &reader.subject, pointer).await;
                            }
                            RevalidationResult::Fallback
                        }
                    }
                    Ok(_) => {
                        if let Some(pointer) = &stale {
                            reader.state.discard(epoch, &reader.subject, pointer).await;
                        }
                        RevalidationResult::Fallback
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {
                        if let Some(pointer) = &stale {
                            reader.state.discard(epoch, &reader.subject, pointer).await;
                        }
                        RevalidationResult::NotFound
                    }
                    Err(_) => RevalidationResult::Fallback,
                }
            })
            .await
            .unwrap_or(RevalidationResult::Fallback)
    }

    async fn prepare(&self, range: BytesRange) -> Prepared {
        if range.size() == Some(0) {
            return Ok(None);
        }
        let epoch = self.state.epoch(&self.subject);
        let (cached, from_local) = match self.local_pointer(epoch) {
            Some(pointer) => (Some(pointer), true),
            None => (self.state.pointer(&self.subject).await, false),
        };
        if let Some(pointer) = cached {
            if pointer.is_fresh(self.ttl, self.args.version()) {
                if !from_local {
                    self.remember_pointer(epoch, &pointer);
                }
                return self
                    .prepare_stream(epoch, pointer, range, None, false)
                    .await;
            }
            return match self.revalidate_stat(epoch, Some(pointer)).await {
                RevalidationResult::Verified {
                    pointer: current,
                    published,
                } => {
                    if published {
                        self.remember_pointer(epoch, &current);
                    }
                    self.prepare_stream(epoch, current, range, None, false)
                        .await
                }
                RevalidationResult::NotFound | RevalidationResult::Fallback => {
                    self.forget_pointer(epoch);
                    Ok(None)
                }
            };
        }

        let forward = match range {
            BytesRange::Range { offset, size } => {
                offset..size.map_or(u64::MAX, |size| offset.saturating_add(size))
            }
            BytesRange::Suffix { .. } => {
                // Resolve total length via `stat` when available to avoid an extra
                // GET whose body would be discarded before aligned chunk fetching.
                if self.source.capability().stat {
                    return match self.revalidate_stat(epoch, None).await {
                        RevalidationResult::Verified { pointer, published } => {
                            if published {
                                self.remember_pointer(epoch, &pointer);
                            }
                            self.prepare_stream(epoch, pointer, range, None, true).await
                        }
                        RevalidationResult::NotFound | RevalidationResult::Fallback => Ok(None),
                    };
                }
                if !self.source.capability().read_with_suffix {
                    return Ok(None);
                }
                let observed_at = Timestamp::now();
                let limit = BytesRange::suffix(range.size().unwrap().min(self.chunk_size));
                let metadata = match self.origin()?.open(limit).await {
                    Ok((response, _)) => response.into_metadata().filter(Metadata::is_file),
                    Err(error) if error.kind() == ErrorKind::RangeNotSatisfied => {
                        return Ok(None);
                    }
                    Err(error) => return Err(error.into()),
                };
                let Some(pointer) =
                    metadata.and_then(|meta| Pointer::from_metadata(meta, observed_at))
                else {
                    return Ok(None);
                };
                if self.state.publish(epoch, &self.subject, &pointer).await {
                    self.remember_pointer(epoch, &pointer);
                }
                return self.prepare_stream(epoch, pointer, range, None, true).await;
            }
        };
        if forward.is_empty() {
            return Ok(None);
        }
        let start = forward.start / self.chunk_size * self.chunk_size;
        let aligned = start..self.unit_end(start, forward.end);
        let keys = (start / self.chunk_size..=(aligned.end - 1) / self.chunk_size)
            .map(|index| DiscoveryKey {
                epoch,
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
                reader.discover(epoch, start..end.min(aligned.end)).await
            })
            .await
        else {
            return Ok(None);
        };
        let Some(discovery) = result? else {
            return Ok(None);
        };
        if discovery.published {
            self.remember_pointer(epoch, &discovery.pointer);
        }
        self.prepare_stream(epoch, discovery.pointer, range, discovery.unit, true)
            .await
    }

    async fn discover(&self, epoch: u64, aligned: Range<u64>) -> DiscoveryResult {
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
        let (pointer, use_body) = if let Some(pointer) = response
            .as_ref()
            .and_then(|meta| Pointer::from_metadata(meta.clone(), observed_at))
        {
            (pointer, true)
        } else if self.source.capability().stat {
            match self.stat(self.args.version()).await {
                Ok(metadata) if metadata.is_file() => {
                    if response.as_ref().is_some_and(|response| {
                        response.content_length() != metadata.content_length()
                    }) {
                        return Err(FillError::Changed);
                    }
                    let Some(pointer) = Pointer::from_metadata(metadata, observed_at) else {
                        return Ok(None);
                    };
                    // `open` lacked a verifiable identity, so only publish the
                    // verified metadata and fetch chunks under identity checks.
                    (pointer, false)
                }
                Ok(_) => return Ok(None),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return if response.is_some() {
                        Err(FillError::Changed)
                    } else {
                        Ok(None)
                    };
                }
                Err(error) if error.kind() == ErrorKind::Unsupported => return Ok(None),
                Err(error) => return Err(FillError::Unverifiable(Arc::new(error))),
            }
        } else {
            return Ok(None);
        };
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
            self.store_unit(&pointer, &unit).await;
            Some(unit)
        } else {
            None
        };
        let published = self.state.publish(epoch, &self.subject, &pointer).await;
        Ok(Some(Discovery {
            pointer,
            unit,
            published,
        }))
    }

    async fn prepare_stream(
        &self,
        epoch: u64,
        pointer: Pointer,
        range: BytesRange,
        unit: Option<Unit>,
        cold: bool,
    ) -> Prepared {
        let Some(bytes) = self.normalized(range, pointer.metadata.content_length()) else {
            return Ok(None);
        };
        let unit = unit.filter(|unit| unit.start <= bytes.start && bytes.start < unit.end());
        let mut stream = CachedStream::new(
            self.clone(),
            epoch,
            pointer,
            bytes,
            unit.into_iter().collect(),
            cold,
        );
        if stream.units.is_empty() {
            match stream.fetch().await {
                Ok(unit) => stream.units.push(unit),
                Err(error) => {
                    if matches!(error, FillError::Changed) {
                        self.state
                            .discard(epoch, &self.subject, &stream.pointer)
                            .await;
                        self.forget_pointer(epoch);
                    }
                    return Err(error);
                }
            }
        }
        Ok(Some(stream))
    }

    async fn store_unit(&self, pointer: &Pointer, unit: &Unit) {
        let prefix = data_prefix(&self.subject, pointer.identity, self.chunk_size);
        let chunk_size = self.chunk_size as usize;
        if unit.data.len() <= chunk_size {
            let _ = self
                .state
                .cache
                .write(
                    &format!("{prefix}/{:x}", unit.start / self.chunk_size),
                    unit.data.clone(),
                )
                .await;
            return;
        }
        let mut writes = Vec::new();
        let mut offset = 0;
        let mut index = unit.start / self.chunk_size;
        while offset < unit.data.len() {
            let end = offset.saturating_add(chunk_size).min(unit.data.len());
            let key = format!("{prefix}/{index:x}");
            let slice = unit.data.slice(offset..end);
            let cache = &self.state.cache;
            writes.push(async move {
                let _ = cache.write(&key, slice).await;
            });
            offset = end;
            index += 1;
        }
        join_all(writes).await;
    }

    async fn fetch_unit(&self, pointer: Pointer, start: u64, count: usize) -> FillResult {
        let length = pointer.metadata.content_length();
        let end = start
            .saturating_add(self.chunk_size.saturating_mul(count as u64))
            .min(length);
        let cap = self.source.capability();
        let version = self.args.version().or_else(|| {
            cap.read_with_version
                .then(|| {
                    pointer
                        .metadata
                        .version()
                        .filter(|v| is_immutable_version(v))
                })
                .flatten()
        });
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
        self.store_unit(&pointer, &unit).await;
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
            || Identity::from_metadata(&metadata) != Some(pointer.identity)
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
    epoch: u64,
    pointer: Pointer,
    prefix: String,
    remaining: Range<u64>,
    units: Vec<Unit>,
    cold: bool,
    terminal: Option<FillError>,
}

impl CachedStream {
    fn new(
        reader: CachedReader,
        epoch: u64,
        pointer: Pointer,
        remaining: Range<u64>,
        units: Vec<Unit>,
        cold: bool,
    ) -> Self {
        let prefix = data_prefix(&reader.subject, pointer.identity, reader.chunk_size);
        Self {
            reader,
            epoch,
            pointer,
            prefix,
            remaining,
            units,
            cold,
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
            .read_with(&format!("{}/{index:x}", self.prefix))
            .limit(expected.saturating_add(1))
            .await
            .ok()?;
        (data.len() as u64 == expected).then_some(Unit { start, data })
    }

    async fn fill_missing(&self, first: u64, keys: Vec<FillKey>) -> FillResult {
        let chunk_size = self.reader.chunk_size;
        let count = keys.len();
        let reader = self.reader.clone();
        let pointer = self.pointer.clone();
        match self
            .reader
            .state
            .fills
            .run(
                self.reader.context.executor(),
                keys,
                move |count| async move { reader.fetch_unit(pointer, first * chunk_size, count).await },
            )
            .await
        {
            Some(result) => result,
            None => {
                // If the executor dropped the shared fill task without running it
                // to completion, fall back to fetching the unit inline.
                self.reader
                    .fetch_unit(self.pointer.clone(), first * chunk_size, count)
                    .await
            }
        }
    }

    async fn fetch(&mut self) -> FillResult {
        let chunk_size = self.reader.chunk_size;
        let first = self.remaining.start / chunk_size;
        let last = ((self.remaining.end - 1) / chunk_size)
            .min(first.saturating_add(self.reader.unit_chunks() - 1));

        if first == last || self.cold {
            if let Some(hit) = self.cached_chunk(first).await {
                self.cold = false;
                return Ok(hit);
            }
            let keys = (first..=last)
                .map(|index| FillKey {
                    epoch: self.epoch,
                    key: format!("{}/{index:x}", self.prefix),
                })
                .collect();
            return self.fill_missing(first, keys).await;
        }

        let results = join_all((first..=last).map(|index| self.cached_chunk(index))).await;
        let mut iter = results.into_iter();
        if let Some(first_hit) = iter.next().flatten() {
            let contiguous: Vec<Unit> = iter.take_while(Option::is_some).flatten().collect();
            for unit in contiguous.into_iter().rev() {
                self.units.push(unit);
            }
            return Ok(first_hit);
        }

        let mut keys = vec![FillKey {
            epoch: self.epoch,
            key: format!("{}/{first:x}", self.prefix),
        }];
        let mut prefetched = Vec::new();
        let mut index = first + 1;
        while let Some(item) = iter.next() {
            if let Some(hit) = item {
                prefetched.push(hit);
                for next_hit in iter.by_ref().take_while(Option::is_some).flatten() {
                    prefetched.push(next_hit);
                }
                break;
            }
            keys.push(FillKey {
                epoch: self.epoch,
                key: format!("{}/{index:x}", self.prefix),
            });
            index += 1;
        }
        let unit = self.fill_missing(first, keys).await?;
        // Retain prefetched cached chunks only when the filled unit ends right
        // where they begin (singleflight may truncate a flight if another reader
        // already reserved an intermediate chunk).
        if prefetched
            .first()
            .is_some_and(|hit| hit.start == unit.end())
        {
            for hit in prefetched.into_iter().rev() {
                self.units.push(hit);
            }
        }
        Ok(unit)
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
                            .discard(self.epoch, &self.reader.subject, &self.pointer)
                            .await;
                        self.reader.forget_pointer(self.epoch);
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

impl ReadStream for CachedStream {
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
                RpRead::new(stream.pointer.metadata.clone()),
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
                expected.min(stream.pointer.metadata.content_length())
            } else {
                expected
            };
            if data.len() as u64 != bound {
                // Defer exact-read EOF error classification to the source.
                return self.origin()?.read(range).await;
            }
            return Ok((RpRead::new(stream.pointer.metadata), data));
        }
        Err(last_error.into_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use opendal_core::services::Memory;
    use opendal_core::{MetadataBuilder, Operator};
    use opendal_service_fs::Fs;

    use crate::CacheLayer;
    use crate::key::subject;

    fn test_reader(
        context: OperationContext,
        source: Servicer,
        cache: Operator,
        path: &str,
        ttl: Duration,
    ) -> CachedReader {
        let subject = subject(&source.info(), path, None);
        CachedReader {
            state: Arc::new(State::new(cache)),
            source,
            context,
            path: path.to_owned(),
            args: OpRead::new(),
            subject,
            chunk_size: 4,
            ttl,
            last_pointer: Arc::new(RwLock::new(None)),
        }
    }

    fn verifiable_metadata(len: u64) -> Metadata {
        let mut builder = MetadataBuilder::file(len);
        builder.etag("\"v1\"");
        builder.build()
    }

    #[test]
    fn local_pointer_window_is_capped_by_ttl_and_refresh_interval() -> Result<()> {
        let (ctx, source) = Operator::new(Memory::default())?.into_parts();
        let cache = Operator::new(Memory::default())?;
        let epoch = 0;
        let now = Instant::now();

        // Warm pointer already older than 1s (5m old) within a 10m TTL still
        // gets a full 1s local reuse window from when this reader remembered it.
        let five_min_ago = Timestamp::now() - Duration::from_secs(300);
        let warm_pointer = Pointer::from_metadata(verifiable_metadata(8), five_min_ago).unwrap();
        let reader_10m = test_reader(
            ctx.clone(),
            source.clone(),
            cache.clone(),
            "obj",
            Duration::from_secs(600),
        );
        reader_10m.remember_pointer_at(epoch, &warm_pointer, now);
        assert!(
            reader_10m
                .local_pointer_at(epoch, now + Duration::from_millis(999))
                .is_some()
        );
        assert!(
            reader_10m
                .local_pointer_at(epoch, now + LOCAL_POINTER_WINDOW)
                .is_none()
        );

        // Short TTL (< 1s) caps local reuse at ttl rather than 1s.
        let short_ttl = Duration::from_millis(200);
        let fresh_pointer =
            Pointer::from_metadata(verifiable_metadata(8), Timestamp::now()).unwrap();
        let reader_short =
            test_reader(ctx.clone(), source.clone(), cache.clone(), "obj", short_ttl);
        reader_short.remember_pointer_at(epoch, &fresh_pointer, now);
        assert!(
            reader_short
                .local_pointer_at(epoch, now + Duration::from_millis(199))
                .is_some()
        );
        assert!(
            reader_short
                .local_pointer_at(epoch, now + short_ttl)
                .is_none()
        );

        // Duration::MAX still caps local reuse at LOCAL_POINTER_WINDOW.
        let reader_max = test_reader(
            ctx.clone(),
            source.clone(),
            cache.clone(),
            "obj",
            Duration::MAX,
        );
        reader_max.remember_pointer_at(epoch, &fresh_pointer, now);
        assert!(
            reader_max
                .local_pointer_at(epoch, now + Duration::from_millis(999))
                .is_some()
        );
        assert!(
            reader_max
                .local_pointer_at(epoch, now + LOCAL_POINTER_WINDOW)
                .is_none()
        );

        // Duration::ZERO never reuses locally.
        let reader_zero = test_reader(ctx, source, cache, "obj", Duration::ZERO);
        reader_zero.remember_pointer_at(epoch, &fresh_pointer, now);
        assert!(reader_zero.local_pointer_at(epoch, now).is_none());

        Ok(())
    }

    #[tokio::test]
    async fn local_pointer_expires_for_cross_instance_invalidation() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let (ctx, source_svc) =
            Operator::new(Fs::default().root(dir.path().to_str().unwrap()))?.into_parts();
        let source = Operator::from_parts(ctx.clone(), source_svc.clone());
        let cache = Operator::new(Memory::default())?;
        source.write("obj", "old-data").await?;

        // Instance A reuses a CachedReader with ttl == Duration::MAX.
        let reader_a = test_reader(ctx, source_svc, cache.clone(), "obj", Duration::MAX);
        let (_, first) = reader_a.read(BytesRange::from(0..8)).await?;
        assert_eq!(first.to_vec(), b"old-data");

        // Instance B shares the same source and cache backend and performs a
        // single-path overwrite, invalidating v1/meta without changing
        // Instance A's local shard epoch.
        let op_b = source.layer(
            CacheLayer::new(cache)?
                .with_chunk_size(4)
                .with_ttl(Duration::MAX),
        );
        op_b.write("obj", "new-data-v2").await?;

        // Age Instance A's locally remembered pointer past LOCAL_POINTER_WINDOW.
        if let Some(expired) = Instant::now().checked_sub(LOCAL_POINTER_WINDOW) {
            let mut guard = reader_a.last_pointer.write().unwrap();
            guard
                .as_mut()
                .expect("pointer should be cached")
                .remembered_at = expired;
        } else {
            tokio::time::sleep(LOCAL_POINTER_WINDOW).await;
        }

        // The reused Reader must re-read v1/meta and observe Instance B's overwrite.
        let (_, second) = reader_a.read(BytesRange::from(0..11)).await?;
        assert_eq!(second.to_vec(), b"new-data-v2");

        Ok(())
    }
}
