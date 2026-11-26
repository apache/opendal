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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

mod glob;

use crate::raw::oio::Delete;
use crate::raw::oio::List;
use crate::raw::oio::PrefixLister;
use crate::raw::*;
use crate::*;

/// Simulate missing capabilities for a storage service.
///
/// The default `auto-simulate` Cargo feature applies this layer automatically
/// when you create an operator with [`Operator::new`]. To opt in per operator,
/// set `default-features = false` on your `opendal` or `opendal-core` dependency,
/// leave `auto-simulate` disabled, and add the layer with [`Operator::layer`].
///
/// # Examples
///
/// The memory service has no native directory creation. The layer uses listing
/// and empty writes to provide `create_dir`. This example uses Tokio with the
/// `macros` and `rt` features.
///
/// ```
/// use opendal_core::layers::SimulateLayer;
/// use opendal_core::services::Memory;
/// use opendal_core::{Operator, Result};
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() -> Result<()> {
///     let op = Operator::new(Memory::default())?;
///     # #[cfg(not(feature = "auto-simulate"))]
///     # assert!(!op.info().capability().create_dir);
///     let op = op.layer(SimulateLayer::default());
///
///     assert!(op.info().capability().create_dir);
///     op.create_dir("reports/").await?;
///     assert!(op.stat("reports/").await?.is_dir());
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SimulateLayer {
    read_with_suffix: bool,
    list_recursive: bool,
    list_glob: bool,
    stat_dir: bool,
    create_dir: bool,
    delete_recursive: bool,
}

impl Default for SimulateLayer {
    fn default() -> Self {
        Self {
            read_with_suffix: true,
            list_recursive: true,
            list_glob: true,
            stat_dir: true,
            create_dir: true,
            delete_recursive: true,
        }
    }
}

impl SimulateLayer {
    /// Enable or disable suffix read simulation. Default: true.
    pub fn with_read_with_suffix(mut self, enabled: bool) -> Self {
        self.read_with_suffix = enabled;
        self
    }

    /// Enable or disable recursive list simulation. Default: true.
    pub fn with_list_recursive(mut self, enabled: bool) -> Self {
        self.list_recursive = enabled;
        self
    }

    /// Enable or disable glob list simulation. Default: true.
    ///
    /// Services that advertise native [`Capability::list_with_glob`] receive
    /// glob requests unchanged. Otherwise this simulation evaluates
    /// [`options::ListOptions::glob`] client-side by pruning unrelated
    /// directories or filtering an underlying listing.
    pub fn with_list_glob(mut self, enabled: bool) -> Self {
        self.list_glob = enabled;
        self
    }

    /// Enable or disable stat dir simulation. Default: true.
    pub fn with_stat_dir(mut self, enabled: bool) -> Self {
        self.stat_dir = enabled;
        self
    }

    /// Enable or disable create_dir simulation. Default: true.
    pub fn with_create_dir(mut self, enabled: bool) -> Self {
        self.create_dir = enabled;
        self
    }

    /// Enable or disable recursive delete simulation. Default: true.
    pub fn with_delete_recursive(mut self, enabled: bool) -> Self {
        self.delete_recursive = enabled;
        self
    }
}

impl Layer for SimulateLayer {
    fn apply_service(&self, srv: Servicer) -> Servicer {
        Arc::new(SimulateService {
            srv,
            config: self.clone(),
        })
    }
}

/// Service that applies capability simulation.
struct SimulateService {
    srv: Servicer,
    config: SimulateLayer,
}

impl Debug for SimulateService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.srv.fmt(f)
    }
}

impl SimulateService {
    fn simulate_capability(&self, mut cap: Capability) -> Capability {
        if self.config.read_with_suffix && cap.read {
            cap.read_with_suffix = true;
        }
        if self.config.list_glob && cap.list {
            cap.list_with_glob = true;
        }
        if self.config.create_dir && cap.list && cap.write_can_empty {
            cap.create_dir = true;
        }
        if self.config.delete_recursive
            && cap.list
            && cap.delete
            && (cap.list_with_recursive || self.config.list_recursive)
        {
            cap.delete_with_recursive = true;
        }
        cap
    }

    async fn simulate_create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let capability = self.srv.capability();

        if capability.create_dir || !self.config.create_dir {
            return self.srv.create_dir(ctx, path, args).await;
        }

        if capability.write_can_empty && capability.list {
            let mut w = self.srv.write(ctx, path, OpWrite::default())?;
            oio::Write::close(&mut w).await?;
            return Ok(RpCreateDir::default());
        }

        self.srv.create_dir(ctx, path, args).await
    }

    async fn simulate_stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> Result<RpStat> {
        let capability = self.srv.capability();

        if path == "/" && args.version().is_none() {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }

        if path.ends_with('/') {
            if capability.create_dir {
                let meta = self
                    .srv
                    .stat(ctx, path, args.clone())
                    .await?
                    .into_metadata();

                if meta.is_file() {
                    return Err(Error::new(
                        ErrorKind::NotFound,
                        "stat expected a directory, but found a file",
                    ));
                }

                return Ok(RpStat::new(meta));
            }

            if self.config.stat_dir && capability.list && args.version().is_none() {
                let mut l = self.srv.list(
                    ctx,
                    path,
                    options::ListOptions {
                        recursive: capability.list_with_recursive,
                        limit: Some(1),
                        ..Default::default()
                    }
                    .into(),
                )?;

                return if l.next().await?.is_some() {
                    Ok(RpStat::new(MetadataBuilder::dir().build()))
                } else {
                    Err(Error::new(
                        ErrorKind::NotFound,
                        "the directory is not found",
                    ))
                };
            }
        }

        self.srv.stat(ctx, path, args).await
    }

    fn simulate_list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<SimulateLister> {
        let cap = self.srv.capability();
        if args.glob().is_some() {
            if !cap.list || cap.list_with_glob || !self.config.list_glob {
                return self.srv.list(ctx, path, args).map(SimulateLister::Direct);
            }
            return glob::simulate_list(self, ctx, path, args).map(SimulateLister::Glob);
        }

        self.simulate_list_without_glob(ctx, path, args)
    }

    fn simulate_list_without_glob(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> Result<SimulateLister> {
        let cap = self.srv.capability();
        let lister = match (
            args.recursive(),
            cap.list_with_recursive,
            self.config.list_recursive,
        ) {
            // Backend supports recursive list, forward directly.
            (_, true, _) => {
                let p = self.srv.list(ctx, path, args)?;
                SimulateLister::Direct(p)
            }
            // Simulate recursive via flat list when enabled.
            (true, false, true) => {
                let directory = if path.ends_with('/') {
                    path
                } else {
                    get_parent(path)
                };
                let lister =
                    ServicerFlatLister::new(ctx.clone(), self.srv.clone(), directory, args);
                if directory == path {
                    SimulateLister::Flat(lister)
                } else {
                    SimulateLister::PrefixFlat(PrefixLister::new(lister, path))
                }
            }
            // Recursive requested but simulation disabled; rely on backend and propagate errors.
            (true, false, false) => {
                let p = self.srv.list(ctx, path, args)?;
                SimulateLister::Direct(p)
            }
            // Non-recursive list: keep existing prefix handling semantics.
            (false, false, _) => {
                if path.ends_with('/') {
                    let p = self.srv.list(ctx, path, args)?;
                    SimulateLister::Direct(p)
                } else {
                    let parent = get_parent(path);
                    let p = self.srv.list(ctx, parent, args)?;
                    let p = PrefixLister::new(p, path);
                    SimulateLister::Prefix(p)
                }
            }
        };

        Ok(lister)
    }

    async fn simulate_delete_with_recursive(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpDelete,
        deleter: &mut oio::Deleter,
    ) -> Result<()> {
        if !self.capability().delete_with_recursive {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "recursive delete is not supported",
            ));
        }

        let mut non_recursive = args;
        non_recursive.set_recursive(false);

        let mut lister = self.simulate_list(
            ctx,
            path,
            options::ListOptions {
                recursive: true,
                ..Default::default()
            }
            .into(),
        )?;

        while let Some(entry) = lister.next().await? {
            let entry = entry.into_entry();
            let mut entry_args = non_recursive.clone();
            if entry_args.version().is_none()
                && let Some(version) = entry.metadata().version()
            {
                entry_args = entry_args.into_version(version);
            }
            deleter.delete(entry.path(), entry_args).await?;
        }

        Ok(())
    }
}

impl Service for SimulateService {
    type Reader = SimulateReader;
    type Writer = oio::Writer;
    type Lister = SimulateLister;
    type Deleter = SimulateDeleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.srv.info()
    }

    fn capability(&self) -> Capability {
        self.simulate_capability(self.srv.capability())
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.simulate_create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let capability = self.srv.capability();
        let simulate_read_with_suffix =
            self.config.read_with_suffix && capability.read && !capability.read_with_suffix;
        let reader = self.srv.read(ctx, path, args.clone())?;
        let reader = SimulateReader::new(
            ctx.clone(),
            self.srv.clone(),
            path.to_string(),
            args,
            reader,
            simulate_read_with_suffix,
        );
        Ok(reader)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.srv.write(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.srv.copy(ctx, from, to, args)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.srv.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.srv.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.srv.restore(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.simulate_stat(ctx, path, args).await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let deleter = self.srv.delete(ctx)?;
        Ok(SimulateDeleter::new(
            ctx.clone(),
            self.srv.clone(),
            self.config.clone(),
            deleter,
        ))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.simulate_list(ctx, path, args)
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.srv.presign(ctx, path, args).await
    }
}

enum SimulateLister {
    Direct(oio::Lister),
    Flat(ServicerFlatLister),
    Prefix(PrefixLister<oio::Lister>),
    PrefixFlat(PrefixLister<ServicerFlatLister>),
    Glob(glob::GlobLister),
}

impl SimulateLister {
    fn into_oio_lister(self) -> oio::Lister {
        match self {
            Self::Direct(lister) => lister,
            other => Box::new(other),
        }
    }
}

impl oio::List for SimulateLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            Self::Direct(lister) => lister.next().await,
            Self::Flat(lister) => lister.next().await,
            Self::Prefix(lister) => lister.next().await,
            Self::PrefixFlat(lister) => lister.next().await,
            Self::Glob(lister) => lister.next().await,
        }
    }
}

struct SimulateReader {
    ctx: OperationContext,
    srv: Servicer,
    path: String,
    args: OpRead,
    inner: oio::Reader,
    simulate_read_with_suffix: bool,
}

impl SimulateReader {
    fn new(
        ctx: OperationContext,
        srv: Servicer,
        path: String,
        args: OpRead,
        inner: oio::Reader,
        simulate_read_with_suffix: bool,
    ) -> Self {
        Self {
            ctx,
            srv,
            path,
            args,
            inner,
            simulate_read_with_suffix,
        }
    }

    async fn stat_content_length(&self) -> Result<u64> {
        let op = options::StatOptions {
            version: self.args.version().map(str::to_owned),
            ..Default::default()
        }
        .into();

        Ok(self
            .srv
            .stat(&self.ctx, &self.path, op)
            .await?
            .into_metadata()
            .content_length())
    }

    fn suffix_to_range(content_length: u64, size: u64) -> BytesRange {
        let start = content_length.saturating_sub(size);
        BytesRange::new(start, Some(content_length - start))
    }

    fn size_changed_error(&self, expected_size: u64, actual_size: u64) -> Error {
        Error::new(
            ErrorKind::Conflict,
            "object size changed while simulating suffix read",
        )
        .set_temporary()
        .with_operation(Operation::Read)
        .with_context("service", self.srv.info().scheme())
        .with_context("path", &self.path)
        .with_context("expected_size", expected_size.to_string())
        .with_context("actual_size", actual_size.to_string())
    }
}

impl oio::Read for SimulateReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if !self.simulate_read_with_suffix || !range.is_suffix() {
            return self.inner.open(range).await;
        }

        let BytesRange::Suffix { size } = range else {
            unreachable!("checked by BytesRange::is_suffix")
        };

        if let Some(hint) = self.args.content_length_hint() {
            return self.inner.open(Self::suffix_to_range(hint, size)).await;
        }

        let mut expected_len = self.stat_content_length().await?;
        let mut retried = false;
        loop {
            let resolved = Self::suffix_to_range(expected_len, size);
            match self.inner.open(resolved).await {
                Ok((rp, stream)) => {
                    if let Some(actual_len) = rp
                        .metadata()
                        .filter(|m| m.is_file())
                        .map(Metadata::content_length)
                        && actual_len != expected_len
                    {
                        drop(stream);
                        if !retried {
                            retried = true;
                            expected_len = actual_len;
                            continue;
                        }
                        return Err(self.size_changed_error(expected_len, actual_len));
                    }
                    return Ok((rp, stream));
                }
                Err(err) if err.kind() == ErrorKind::RangeNotSatisfied && !retried => {
                    let new_len = self.stat_content_length().await?;
                    if new_len == expected_len {
                        return Err(err);
                    }
                    retried = true;
                    expected_len = new_len;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        if !self.simulate_read_with_suffix || !range.is_suffix() {
            return self.inner.read(range).await;
        }

        let BytesRange::Suffix { size } = range else {
            unreachable!("checked by BytesRange::is_suffix")
        };

        if let Some(hint) = self.args.content_length_hint() {
            return self.inner.read(Self::suffix_to_range(hint, size)).await;
        }

        let mut expected_len = self.stat_content_length().await?;
        let mut retried = false;
        loop {
            let resolved = Self::suffix_to_range(expected_len, size);
            match self.inner.read(resolved).await {
                Ok((rp, buf)) => {
                    if let Some(actual_len) = rp
                        .metadata()
                        .filter(|m| m.is_file())
                        .map(Metadata::content_length)
                        && actual_len != expected_len
                    {
                        if !retried {
                            retried = true;
                            expected_len = actual_len;
                            continue;
                        }
                        return Err(self.size_changed_error(expected_len, actual_len));
                    }
                    return Ok((rp, buf));
                }
                Err(err) if err.kind() == ErrorKind::RangeNotSatisfied && !retried => {
                    let new_len = self.stat_content_length().await?;
                    if new_len == expected_len {
                        return Err(err);
                    }
                    retried = true;
                    expected_len = new_len;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

/// A directory waiting to be listed.
///
/// The initial directory comes from the caller and has not been validated yet.
/// `oio::Entry` rejects paths whose metadata exceeds the compact block limit,
/// so materialize it only after listing succeeds and produces entries.
enum PendingDir {
    Initial(String),
    Discovered(oio::Entry),
}

impl PendingDir {
    fn path(&self) -> &str {
        match self {
            PendingDir::Initial(path) => path,
            PendingDir::Discovered(entry) => entry.path(),
        }
    }

    fn is_discovered(&self) -> bool {
        matches!(self, PendingDir::Discovered(_))
    }

    fn try_into_entry(self) -> Result<oio::Entry> {
        match self {
            PendingDir::Initial(path) => {
                // `oio::Entry` stores `path` inside a `CompactValues` block
                // bounded by `u16::MAX` bytes (2-byte bitmap + 2-byte offset + path).
                let encoded_len = 2 * size_of::<u16>() + path.len();
                if encoded_len > u16::MAX as usize {
                    return Err(Error::new(
                        ErrorKind::Unexpected,
                        "directory path exceeds compact entry metadata limit",
                    ));
                }
                Ok(oio::Entry::with(path, MetadataBuilder::dir().build()))
            }
            PendingDir::Discovered(entry) => Ok(entry),
        }
    }
}

// Keep the lister in `next_dir` until its first entry arrives so cancelling
// `ServicerFlatLister::next` cannot detach the lister from its directory.
enum NextDir {
    Directory(PendingDir),
    Listing(PendingDir, oio::Lister),
}

struct ActiveLister {
    entry: oio::Entry,
    lister: oio::Lister,
    discovered: bool,
}

struct ServicerFlatLister {
    ctx: OperationContext,
    srv: Servicer,
    directory_args: OpList,
    start_after: Option<String>,
    next_dir: Option<NextDir>,
    active_lister: Vec<ActiveLister>,
}

impl ServicerFlatLister {
    fn new(ctx: OperationContext, srv: Servicer, path: &str, args: OpList) -> Self {
        let initial_path = if path.is_empty() {
            "/".to_string()
        } else {
            path.to_string()
        };
        Self {
            ctx,
            srv,
            directory_args: options::ListOptions {
                versions: args.versions(),
                deleted: args.deleted(),
                ..Default::default()
            }
            .into(),
            start_after: args.start_after().map(str::to_owned),
            next_dir: Some(NextDir::Directory(PendingDir::Initial(initial_path))),
            active_lister: vec![],
        }
    }

    async fn next_entry(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if matches!(self.next_dir.as_ref(), Some(NextDir::Directory(_))) {
                let Some(NextDir::Directory(de)) = self.next_dir.take() else {
                    unreachable!("next_dir must contain a directory")
                };
                let lister = match self
                    .srv
                    .list(&self.ctx, de.path(), self.directory_args.clone())
                {
                    Ok(v) => v,
                    Err(e) if e.kind() == ErrorKind::PermissionDenied && de.is_discovered() => {
                        log::warn!(
                            "ServicerFlatLister skipping directory due to permission denied: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        log::warn!(
                            "ServicerFlatLister skipping directory due to not found during listing: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) => {
                        self.next_dir = Some(NextDir::Directory(de));
                        return Err(e);
                    }
                };
                self.next_dir = Some(NextDir::Listing(de, lister));
            }

            if matches!(self.next_dir.as_ref(), Some(NextDir::Listing(..))) {
                let first = {
                    let Some(NextDir::Listing(_, lister)) = self.next_dir.as_mut() else {
                        unreachable!("next_dir must contain a lister")
                    };
                    lister.next().await
                };

                let Some(NextDir::Listing(de, lister)) = self.next_dir.take() else {
                    unreachable!("next_dir must contain a lister")
                };
                let first = match first {
                    Ok(v) => v,
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        log::warn!(
                            "ServicerFlatLister skipping directory due to not found during listing: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) if e.kind() == ErrorKind::PermissionDenied && de.is_discovered() => {
                        log::warn!(
                            "ServicerFlatLister skipping directory due to permission denied during listing: {}",
                            de.path()
                        );
                        continue;
                    }
                    Err(e) => {
                        // No entry has been yielded from this lister, so restart
                        // the directory when the caller retries after an error.
                        self.next_dir = Some(NextDir::Directory(de));
                        return Err(e);
                    }
                };

                let v = match first {
                    Some(v) => v,
                    // A non-recursive listing may omit the self-entry of an
                    // empty directory. Yield the exhausted directory in
                    // post-order instead of dropping it. The top-level prefix
                    // itself is not an entry, so an empty prefix stays empty.
                    None => match de {
                        PendingDir::Initial(_) => return Ok(None),
                        PendingDir::Discovered(entry) => return Ok(Some(entry)),
                    },
                };

                let discovered = de.is_discovered();
                let de = de.try_into_entry()?;
                self.active_lister.push(ActiveLister {
                    entry: de.clone(),
                    lister,
                    discovered,
                });

                if v.mode().is_dir() {
                    if v.path() != de.path() {
                        self.next_dir = Some(NextDir::Directory(PendingDir::Discovered(v)));
                        continue;
                    }
                } else {
                    return Ok(Some(v));
                }
            }

            let active = match self.active_lister.last_mut() {
                Some(active) => active,
                None => return Ok(None),
            };

            match active.lister.next().await {
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    log::warn!(
                        "ServicerFlatLister skipping directory due to not found during recursive listing: {}",
                        active.entry.path()
                    );
                    let _ = self.active_lister.pop();
                    continue;
                }
                Err(e) if e.kind() == ErrorKind::PermissionDenied && active.discovered => {
                    log::warn!(
                        "ServicerFlatLister skipping directory due to permission denied during recursive listing: {}",
                        active.entry.path()
                    );
                    let _ = self.active_lister.pop();
                    continue;
                }
                Err(e) => return Err(e),
                Ok(Some(v)) if v.mode().is_dir() => {
                    if v.path() != active.entry.path() {
                        self.next_dir = Some(NextDir::Directory(PendingDir::Discovered(v)));
                        continue;
                    }
                }
                Ok(Some(v)) => return Ok(Some(v)),
                Ok(None) => {
                    let active = self
                        .active_lister
                        .pop()
                        .expect("the exhausted lister must still be active");
                    return Ok(Some(active.entry));
                }
            }
        }
    }
}

impl oio::List for ServicerFlatLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        // Directories at or before the marker can still contain later entries.
        while let Some(entry) = self.next_entry().await? {
            if self
                .start_after
                .as_deref()
                .is_none_or(|start_after| entry.path() > start_after)
            {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

/// Deleter wrapper that simulates recursive deletion.
struct SimulateDeleter {
    ctx: OperationContext,
    srv: Servicer,
    config: SimulateLayer,
    inner: oio::Deleter,
}

impl SimulateDeleter {
    fn new(
        ctx: OperationContext,
        srv: Servicer,
        config: SimulateLayer,
        inner: oio::Deleter,
    ) -> Self {
        Self {
            ctx,
            srv,
            config,
            inner,
        }
    }
}

impl oio::Delete for SimulateDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        if args.recursive() {
            let cap = self.srv.capability();

            if cap.delete_with_recursive {
                return self.inner.delete(path, args).await;
            }

            if self.config.delete_recursive {
                let service = SimulateService {
                    srv: self.srv.clone(),
                    config: self.config.clone(),
                };
                return service
                    .simulate_delete_with_recursive(&self.ctx, path, args, &mut self.inner)
                    .await;
            }
        }

        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::{Future, poll_fn};
    use std::sync::Mutex;
    use std::task::Poll;

    use super::*;

    #[derive(Debug, Default)]
    struct MockService {
        capability: Capability,
        tree: Option<Arc<TreeState>>,
    }

    struct MockLister(bool);

    /// A directory tree for `TreeLister`, with one-shot listing faults.
    ///
    /// Temporary errors model a 503 or a dropped connection after `RetryLayer`
    /// has classified it. A pending first entry models cancellation while a
    /// directory is being opened.
    #[derive(Debug, Default)]
    struct TreeState {
        /// Directory path to its children, as `(path, is_dir)`.
        tree: HashMap<String, Vec<(String, bool)>>,
        fail_first_list_on: Option<String>,
        fail_first_next_on: Option<String>,
        pending_once_on: Option<(String, usize)>,
        list_injected: Mutex<bool>,
        next_injected: Mutex<bool>,
        pending_injected: Mutex<bool>,
        /// Every path passed to `list`, so a test can assert on retries.
        listed: Mutex<Vec<String>>,
        /// Every path passed to `delete`, in order.
        deleted: Mutex<Vec<String>>,
    }

    impl TreeState {
        /// Claim a one-shot fault for `path`, if it is armed and unused.
        fn take_fault(&self, armed: &Option<String>, used: &Mutex<bool>, path: &str) -> bool {
            if armed.as_deref() != Some(path) {
                return false;
            }
            let mut used = used.lock().expect("mutex must not poison");
            let first_time = !*used;
            *used = true;
            first_time
        }

        fn take_pending_call(&self, path: &str) -> Option<usize> {
            let (target, call) = self.pending_once_on.as_ref()?;
            if target != path {
                return None;
            }
            let mut used = self.pending_injected.lock().expect("mutex must not poison");
            if *used {
                return None;
            }
            *used = true;
            Some(*call)
        }
    }

    struct TreeLister {
        entries: std::vec::IntoIter<(String, bool)>,
        fail_first: bool,
        pending_on_call: Option<usize>,
        next_calls: usize,
    }

    impl TreeLister {
        fn new(state: &Arc<TreeState>, path: &str) -> Self {
            Self {
                entries: state
                    .tree
                    .get(path)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter(),
                fail_first: state.take_fault(&state.fail_first_next_on, &state.next_injected, path),
                pending_on_call: state.take_pending_call(path),
                next_calls: 0,
            }
        }

        fn next_ready(&mut self) -> Result<Option<oio::Entry>> {
            if self.fail_first {
                self.fail_first = false;
                return Err(
                    Error::new(ErrorKind::Unexpected, "injected listing failure").set_temporary(),
                );
            }

            Ok(self.entries.next().map(|(path, is_dir)| {
                let meta = if is_dir {
                    MetadataBuilder::dir().build()
                } else {
                    MetadataBuilder::file(0).build()
                };
                oio::Entry::new(&path, meta)
            }))
        }
    }

    impl oio::List for TreeLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            self.next_calls += 1;
            let mut pending = self.pending_on_call == Some(self.next_calls);
            if pending {
                self.pending_on_call = None;
            }
            poll_fn(move |cx| {
                if pending {
                    pending = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(self.next_ready())
                }
            })
            .await
        }
    }

    impl oio::List for MockLister {
        async fn next(&mut self) -> Result<Option<oio::Entry>> {
            if self.0 {
                return Ok(None);
            }
            self.0 = true;
            Ok(Some(oio::Entry::new(
                "parent/file",
                MetadataBuilder::file(0).build(),
            )))
        }
    }

    struct MockDeleter {
        state: Arc<TreeState>,
    }

    impl oio::Delete for MockDeleter {
        async fn delete(&mut self, path: &str, _: OpDelete) -> Result<()> {
            self.state
                .deleted
                .lock()
                .expect("mutex must not poison")
                .push(path.to_string());
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    impl Service for MockService {
        type Reader = ();
        type Writer = ();
        type Lister = TwoWays<MockLister, TreeLister>;
        type Deleter = MockDeleter;
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            self.capability
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn read(&self, _ctx: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn write(&self, _ctx: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            let Some(state) = &self.tree else {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "operation is not supported",
                ));
            };
            Ok(MockDeleter {
                state: state.clone(),
            })
        }

        fn list(&self, _ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
            if let Some(state) = &self.tree {
                state
                    .listed
                    .lock()
                    .expect("mutex must not poison")
                    .push(path.to_string());

                if state.take_fault(&state.fail_first_list_on, &state.list_injected, path) {
                    return Err(
                        Error::new(ErrorKind::Unexpected, "injected list failure").set_temporary()
                    );
                }

                return Ok(TwoWays::Two(TreeLister::new(state, path)));
            }
            if self.capability.list {
                return Ok(TwoWays::One(MockLister(false)));
            }
            Err(Error::new(ErrorKind::Unsupported, "list is not supported"))
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    struct MockReader {
        observed_range: Arc<Mutex<Option<BytesRange>>>,
    }

    impl oio::Read for MockReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            *self.observed_range.lock().expect("mutex must not poison") = Some(range);
            Ok((
                RpRead::new({
                    let metadata = MetadataBuilder::file(0);
                    metadata.build()
                }),
                Box::new(Buffer::new()),
            ))
        }

        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            *self.observed_range.lock().expect("mutex must not poison") = Some(range);
            Ok((
                RpRead::new({
                    let metadata = MetadataBuilder::file(0);
                    metadata.build()
                }),
                Buffer::new(),
            ))
        }
    }

    #[test]
    fn simulate_layer_exposes_read_with_suffix() {
        let capability = Capability {
            read: true,
            read_with_suffix: false,
            ..Default::default()
        };
        let srv = Arc::new(MockService {
            capability,
            ..Default::default()
        }) as Servicer;

        let srv = SimulateLayer::default().apply_service(srv);

        assert!(srv.capability().read_with_suffix);
    }

    #[test]
    fn simulate_layer_can_disable_read_with_suffix() {
        let capability = Capability {
            read: true,
            read_with_suffix: false,
            ..Default::default()
        };
        let srv = Arc::new(MockService {
            capability,
            ..Default::default()
        }) as Servicer;

        let srv = SimulateLayer::default()
            .with_read_with_suffix(false)
            .apply_service(srv);

        assert!(!srv.capability().read_with_suffix);
    }

    #[tokio::test]
    async fn simulate_stat_dir_without_recursive_list() -> Result<()> {
        let capability = Capability {
            stat: true,
            list: true,
            list_with_recursive: false,
            ..Default::default()
        };
        let srv = Arc::new(MockService {
            capability,
            ..Default::default()
        }) as Servicer;
        let srv = SimulateLayer::default().apply_service(srv);

        let metadata = srv
            .stat(&OperationContext::new(), "parent/", OpStat::default())
            .await?
            .into_metadata();

        assert_eq!(metadata.mode(), EntryMode::DIR);
        Ok(())
    }

    #[tokio::test]
    async fn versioned_stat_bypasses_simulated_directories() -> Result<()> {
        let srv = Arc::new(MockService {
            capability: Capability {
                stat: true,
                stat_with_version: true,
                list: true,
                ..Default::default()
            },
            ..Default::default()
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());

        for path in ["/", "parent/"] {
            assert_eq!(op.stat(path).await?.mode(), EntryMode::DIR);
            let error = op.stat_with(path).version("version").await.unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Unsupported);
        }
        Ok(())
    }

    #[tokio::test]
    async fn simulate_recursive_list_with_start_after() -> Result<()> {
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                list_with_start_after: true,
                ..Default::default()
            },
            tree: Some(Arc::new(TreeState {
                tree: HashMap::from([
                    ("/".into(), vec![("a/".into(), true), ("b".into(), false)]),
                    (
                        "a/".into(),
                        vec![
                            ("a/z".into(), false),
                            ("a/a".into(), false),
                            ("a/m".into(), false),
                        ],
                    ),
                ]),
                ..Default::default()
            })),
        });
        let op =
            Operator::from_parts(OperationContext::default(), srv).layer(SimulateLayer::default());

        let entries = op.list_with("/").recursive(true).start_after("a/m").await?;
        let paths: Vec<_> = entries.iter().map(Entry::path).collect();
        assert_eq!(paths, ["a/z", "b"]);

        let entries = op.list_with("a").recursive(true).start_after("a/m").await?;
        let paths: Vec<_> = entries.iter().map(Entry::path).collect();
        assert_eq!(paths, ["a/z"]);
        Ok(())
    }

    #[tokio::test]
    async fn simulate_reader_uses_content_length_hint_for_suffix() -> Result<()> {
        let observed_range = Arc::new(Mutex::new(None));
        let (_, args, _) = options::ReadOptions {
            content_length_hint: Some(42),
            ..Default::default()
        }
        .into();
        let reader = SimulateReader::new(
            OperationContext::new(),
            Arc::new(MockService {
                capability: Capability::default(),
                ..Default::default()
            }),
            "test".to_string(),
            args,
            Box::new(MockReader {
                observed_range: observed_range.clone(),
            }),
            true,
        );

        oio::Read::read(&reader, BytesRange::suffix(10)).await?;

        assert_eq!(
            *observed_range.lock().expect("mutex must not poison"),
            Some(BytesRange::new(32, Some(10)))
        );

        Ok(())
    }

    /// Recursively list a two-directory tree in which `a/` fails once.
    ///
    /// Returns the files yielded, how many errors surfaced, and every path
    /// that reached `list`.
    async fn list_tree_with_one_fault(state: Arc<TreeState>) -> (Vec<String>, u32, Vec<String>) {
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(state.clone()),
        }) as Servicer;

        let mut lister = ServicerFlatLister::new(OperationContext::new(), srv, "/", OpList::new());

        // Drain it the way RetryLayer does: a temporary error is retried by
        // calling `next()` again on the same lister.
        let mut files = Vec::new();
        let mut errors = 0;
        loop {
            match oio::List::next(&mut lister).await {
                Ok(Some(entry)) => {
                    if entry.mode().is_file() {
                        files.push(entry.path().to_string());
                    }
                }
                Ok(None) => break,
                Err(err) => {
                    assert!(err.is_temporary(), "injected error is temporary: {err}");
                    errors += 1;
                    assert!(errors <= 2, "the fault is injected once, not repeatedly");
                }
            }
        }
        files.sort();

        let listed = state.listed.lock().expect("mutex must not poison").clone();
        (files, errors, listed)
    }

    fn tree_with_two_dirs() -> HashMap<String, Vec<(String, bool)>> {
        // `oio::Entry` normalizes an empty path to "/", so the root is listed
        // under that name.
        HashMap::from([
            (
                "/".to_string(),
                vec![("a/".to_string(), true), ("b/".to_string(), true)],
            ),
            ("a/".to_string(), vec![("a/one.txt".to_string(), false)]),
            ("b/".to_string(), vec![("b/two.txt".to_string(), false)]),
        ])
    }

    #[tokio::test]
    async fn servicer_flat_lister_preserves_a_directory_when_cancelled() {
        let state = Arc::new(TreeState {
            tree: tree_with_two_dirs(),
            pending_once_on: Some(("a/".to_string(), 1)),
            ..Default::default()
        });
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(state),
        }) as Servicer;
        let mut lister = ServicerFlatLister::new(OperationContext::new(), srv, "/", OpList::new());

        let mut next = Box::pin(oio::List::next(&mut lister));
        poll_fn(|cx| {
            assert!(
                next.as_mut().poll(cx).is_pending(),
                "the injected pending point must be reached"
            );
            Poll::Ready(())
        })
        .await;
        drop(next);

        let mut paths = Vec::new();
        while let Some(entry) = oio::List::next(&mut lister)
            .await
            .expect("listing must succeed")
        {
            paths.push(entry.path().to_string());
        }
        assert_eq!(paths, ["a/one.txt", "a/", "b/two.txt", "b/", "/"]);
    }

    #[tokio::test]
    async fn servicer_flat_lister_preserves_an_active_directory_when_cancelled() {
        let state = Arc::new(TreeState {
            tree: HashMap::from([
                ("/".to_string(), vec![("a/".to_string(), true)]),
                (
                    "a/".to_string(),
                    vec![("a/file".to_string(), false), ("a/sub/".to_string(), true)],
                ),
                (
                    "a/sub/".to_string(),
                    vec![("a/sub/file2".to_string(), false)],
                ),
            ]),
            pending_once_on: Some(("a/sub/".to_string(), 2)),
            ..Default::default()
        });
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(state),
        }) as Servicer;
        let mut lister = ServicerFlatLister::new(OperationContext::new(), srv, "/", OpList::new());

        let mut paths = Vec::new();
        while paths.last().map(String::as_str) != Some("a/sub/file2") {
            let entry = oio::List::next(&mut lister)
                .await
                .expect("listing must succeed")
                .expect("the nested file must be reached");
            paths.push(entry.path().to_string());
        }

        let mut next = Box::pin(oio::List::next(&mut lister));
        poll_fn(|cx| {
            assert!(
                next.as_mut().poll(cx).is_pending(),
                "the injected pending point must be reached"
            );
            Poll::Ready(())
        })
        .await;
        drop(next);

        while let Some(entry) = oio::List::next(&mut lister)
            .await
            .expect("listing must succeed")
        {
            paths.push(entry.path().to_string());
        }
        assert_eq!(paths, ["a/file", "a/sub/file2", "a/sub/", "a/", "/"]);
    }

    /// The directory is lost when its first `next()` fails.
    #[tokio::test]
    async fn servicer_flat_lister_retries_a_directory_whose_first_entry_failed() {
        let state = Arc::new(TreeState {
            tree: tree_with_two_dirs(),
            fail_first_next_on: Some("a/".to_string()),
            ..Default::default()
        });

        let (files, errors, listed) = list_tree_with_one_fault(state).await;

        assert_eq!(
            errors, 1,
            "the injected fault must surface; listed {listed:?}"
        );
        assert_eq!(
            files,
            vec!["a/one.txt".to_string(), "b/two.txt".to_string()],
            "the directory that failed must be listed again, not skipped"
        );
        assert_eq!(
            listed.iter().filter(|path| path.as_str() == "a/").count(),
            2,
            "a/ must be requested again after the temporary error; listed {listed:?}"
        );
    }

    /// The same loss happens one step earlier, when `list` itself fails.
    #[tokio::test]
    async fn servicer_flat_lister_retries_a_directory_whose_list_failed() {
        let state = Arc::new(TreeState {
            tree: tree_with_two_dirs(),
            fail_first_list_on: Some("a/".to_string()),
            ..Default::default()
        });

        let (files, errors, listed) = list_tree_with_one_fault(state).await;

        assert_eq!(
            errors, 1,
            "the injected fault must surface; listed {listed:?}"
        );
        assert_eq!(
            files,
            vec!["a/one.txt".to_string(), "b/two.txt".to_string()],
            "the directory that failed must be listed again, not skipped"
        );
        assert_eq!(
            listed.iter().filter(|path| path.as_str() == "a/").count(),
            2,
            "a/ must be requested again after the temporary error; listed {listed:?}"
        );
    }

    /// The same loss, but two levels down, where the restored `next_dir` has to
    /// coexist with a non-empty `active_lister` stack.
    ///
    /// The flat cases above fail a directory while only the root's lister is on
    /// the stack. Here `/` and `a/` are both open when `a/b/` fails, so a lost
    /// entry would be masked by the parent resuming instead: the listing would
    /// pop back to `/`, move on to `c/`, and return `Ok` without `deep.txt`.
    #[tokio::test]
    async fn servicer_flat_lister_retries_a_nested_directory() {
        let state = Arc::new(TreeState {
            tree: HashMap::from([
                (
                    "/".to_string(),
                    vec![("a/".to_string(), true), ("c/".to_string(), true)],
                ),
                ("a/".to_string(), vec![("a/b/".to_string(), true)]),
                (
                    "a/b/".to_string(),
                    vec![("a/b/deep.txt".to_string(), false)],
                ),
                ("c/".to_string(), vec![("c/two.txt".to_string(), false)]),
            ]),
            fail_first_next_on: Some("a/b/".to_string()),
            ..Default::default()
        });

        let (files, errors, listed) = list_tree_with_one_fault(state).await;

        assert_eq!(
            errors, 1,
            "the injected fault must surface; listed {listed:?}"
        );
        assert_eq!(
            files,
            vec!["a/b/deep.txt".to_string(), "c/two.txt".to_string()],
            "the nested directory must be listed again, not skipped by the parent"
        );
        assert_eq!(
            listed.iter().filter(|path| path.as_str() == "a/b/").count(),
            2,
            "a/b/ must be requested again after the temporary error; listed {listed:?}"
        );
    }

    /// A permanent error must still end the listing rather than lose entries
    /// silently, so the restored `next_dir` cannot mask a real failure.
    #[tokio::test]
    async fn servicer_flat_lister_reports_a_directory_that_keeps_failing() {
        let state = Arc::new(TreeState {
            tree: tree_with_two_dirs(),
            fail_first_next_on: Some("a/".to_string()),
            ..Default::default()
        });
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(state.clone()),
        }) as Servicer;

        let mut lister = ServicerFlatLister::new(OperationContext::new(), srv, "/", OpList::new());

        // Walk until the fault, then stop: a caller that does not retry must
        // see the error instead of a short, successful listing.
        let mut saw_error = false;
        for _ in 0..8 {
            if let Err(err) = oio::List::next(&mut lister).await {
                assert!(err.is_temporary());
                saw_error = true;
                break;
            }
        }
        assert!(
            saw_error,
            "the error must reach a caller that does not retry"
        );
    }

    /// A non-recursive listing can omit the self-entry of an empty directory.
    /// The flat lister must still yield that directory, in post-order.
    #[tokio::test]
    async fn servicer_flat_lister_yields_an_empty_subdirectory() -> Result<()> {
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(Arc::new(TreeState {
                tree: HashMap::from([(
                    "/".to_string(),
                    vec![
                        ("a/one.txt".to_string(), false),
                        ("empty/".to_string(), true),
                    ],
                )]),
                ..Default::default()
            })),
        }) as Servicer;
        let mut lister = ServicerFlatLister::new(OperationContext::new(), srv, "/", OpList::new());

        let mut paths = Vec::new();
        while let Some(entry) = oio::List::next(&mut lister).await? {
            paths.push(entry.path().to_string());
        }

        assert_eq!(paths, ["a/one.txt", "empty/", "/"]);
        Ok(())
    }

    /// A non-recursive listing can emit a self-entry for an empty directory.
    /// The flat lister must still yield that directory exactly once, in
    /// post-order.
    #[tokio::test]
    async fn servicer_flat_lister_does_not_duplicate_empty_subdirectory_self_entry() -> Result<()> {
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(Arc::new(TreeState {
                tree: HashMap::from([
                    (
                        "/".to_string(),
                        vec![
                            ("a/one.txt".to_string(), false),
                            ("empty/".to_string(), true),
                        ],
                    ),
                    ("empty/".to_string(), vec![("empty/".to_string(), true)]),
                ]),
                ..Default::default()
            })),
        }) as Servicer;
        let mut lister = ServicerFlatLister::new(OperationContext::new(), srv, "/", OpList::new());

        let mut paths = Vec::new();
        while let Some(entry) = oio::List::next(&mut lister).await? {
            paths.push(entry.path().to_string());
        }

        assert_eq!(paths, ["a/one.txt", "empty/", "/"]);
        Ok(())
    }

    /// An empty or missing top-level prefix must list as empty instead of
    /// yielding the prefix directory itself.
    #[tokio::test]
    async fn servicer_flat_lister_keeps_an_empty_top_level_prefix_empty() -> Result<()> {
        for prefix in ["missing/", "empty/"] {
            let srv = Arc::new(MockService {
                capability: Capability {
                    list: true,
                    ..Default::default()
                },
                tree: Some(Arc::new(TreeState {
                    tree: HashMap::from([("empty/".to_string(), vec![])]),
                    ..Default::default()
                })),
            }) as Servicer;
            let mut lister =
                ServicerFlatLister::new(OperationContext::new(), srv, prefix, OpList::new());

            assert!(
                oio::List::next(&mut lister).await?.is_none(),
                "the top-level prefix {prefix} must not be yielded"
            );
        }
        Ok(())
    }

    /// A caller-supplied path at or above the 64 KiB compact metadata block
    /// limit must not panic in `ServicerFlatLister::new` before reaching the
    /// underlying service.
    #[tokio::test]
    async fn servicer_flat_lister_handles_oversized_caller_path() -> Result<()> {
        let oversized_dir = format!("{}/", "a".repeat(u16::MAX as usize));
        let oversized_prefix = format!("{oversized_dir}child");

        // Empty listing for both an oversized directory and an oversized prefix
        // must complete cleanly without panicking.
        for path in [&oversized_dir, &oversized_prefix] {
            let state = Arc::new(TreeState::default());
            let srv = Arc::new(MockService {
                capability: Capability {
                    list: true,
                    ..Default::default()
                },
                tree: Some(state.clone()),
            }) as Servicer;
            let op =
                Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());

            let entries = op.list_with(path).recursive(true).await?;
            assert!(entries.is_empty());
            assert_eq!(
                state
                    .listed
                    .lock()
                    .expect("mutex must not poison")
                    .as_slice(),
                std::slice::from_ref(&oversized_dir)
            );
        }

        // A backend error for the oversized directory must propagate cleanly.
        let state = Arc::new(TreeState {
            fail_first_list_on: Some(oversized_dir.clone()),
            ..Default::default()
        });
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(state),
        }) as Servicer;
        let mut lister =
            ServicerFlatLister::new(OperationContext::new(), srv, &oversized_dir, OpList::new());
        let err = oio::List::next(&mut lister)
            .await
            .expect_err("backend listing error must propagate");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        // Even if a backend returns entries under an oversized directory path,
        // materializing the top-level directory must return an error instead of
        // panicking.
        let state = Arc::new(TreeState {
            tree: HashMap::from([(
                oversized_dir.clone(),
                vec![("child.txt".to_string(), false)],
            )]),
            ..Default::default()
        });
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                ..Default::default()
            },
            tree: Some(state),
        }) as Servicer;
        let mut lister =
            ServicerFlatLister::new(OperationContext::new(), srv, &oversized_dir, OpList::new());
        let err = oio::List::next(&mut lister)
            .await
            .expect_err("oversized directory entry must fail cleanly");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        Ok(())
    }

    /// Simulated recursive delete must remove an empty subdirectory before it
    /// removes the parent directory.
    #[tokio::test]
    async fn simulate_recursive_delete_removes_empty_subdirectories() -> Result<()> {
        let state = Arc::new(TreeState {
            tree: HashMap::from([
                ("/".to_string(), vec![("data/".to_string(), true)]),
                (
                    "data/".to_string(),
                    vec![
                        ("data/empty/".to_string(), true),
                        ("data/file.txt".to_string(), false),
                        ("data/nested/".to_string(), true),
                    ],
                ),
                (
                    "data/nested/".to_string(),
                    vec![("data/nested/deep.txt".to_string(), false)],
                ),
            ]),
            ..Default::default()
        });
        let srv = Arc::new(MockService {
            capability: Capability {
                list: true,
                delete: true,
                list_with_recursive: false,
                ..Default::default()
            },
            tree: Some(state.clone()),
        }) as Servicer;
        let op =
            Operator::from_parts(OperationContext::default(), srv).layer(SimulateLayer::default());

        op.delete_with("data/").recursive(true).await?;

        let deleted = state.deleted.lock().expect("mutex must not poison").clone();
        let index = |path: &str| {
            deleted
                .iter()
                .position(|deleted| deleted == path)
                .unwrap_or_else(|| panic!("{path} must be deleted; deleted {deleted:?}"))
        };
        assert!(index("data/empty/") < index("data/"));
        assert!(index("data/file.txt") < index("data/"));
        assert!(index("data/nested/deep.txt") < index("data/nested/"));
        assert!(index("data/nested/") < index("data/"));
        Ok(())
    }

    #[tokio::test]
    async fn simulate_reader_revalidates_suffix_when_object_size_changes() -> Result<()> {
        struct SizeChangingReader {
            contents: Arc<Mutex<std::collections::VecDeque<bytes::Bytes>>>,
            emit_metadata: bool,
        }

        impl oio::StreamRead for SizeChangingReader {
            async fn open(
                &self,
                range: BytesRange,
            ) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
                let data = self
                    .contents
                    .lock()
                    .expect("mutex must not poison")
                    .pop_front()
                    .expect("unexpected open call");
                let content_range = range.to_content_range(data.len())?;
                let slice = data.slice(content_range);
                let rp = if self.emit_metadata {
                    RpRead::new(MetadataBuilder::file(data.len() as u64).build())
                } else {
                    RpRead::default()
                };
                Ok((
                    rp,
                    Box::new(Buffer::from(slice)) as Box<dyn oio::ReadStreamDyn>,
                ))
            }
        }

        #[derive(Debug)]
        struct SizeChangingService {
            stat_lens: Arc<Mutex<std::collections::VecDeque<u64>>>,
            contents: Arc<Mutex<std::collections::VecDeque<bytes::Bytes>>>,
            emit_metadata: bool,
        }

        impl SizeChangingService {
            fn servicer(
                stat_lens: impl Into<std::collections::VecDeque<u64>>,
                contents: impl Into<std::collections::VecDeque<bytes::Bytes>>,
            ) -> Servicer {
                Arc::new(Self {
                    stat_lens: Arc::new(Mutex::new(stat_lens.into())),
                    contents: Arc::new(Mutex::new(contents.into())),
                    emit_metadata: true,
                })
            }

            fn without_metadata(
                stat_lens: impl Into<std::collections::VecDeque<u64>>,
                contents: impl Into<std::collections::VecDeque<bytes::Bytes>>,
            ) -> Servicer {
                Arc::new(Self {
                    stat_lens: Arc::new(Mutex::new(stat_lens.into())),
                    contents: Arc::new(Mutex::new(contents.into())),
                    emit_metadata: false,
                })
            }
        }

        impl Service for SizeChangingService {
            type Reader = oio::StreamReader<SizeChangingReader>;
            type Writer = ();
            type Lister = ();
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock")
            }

            fn capability(&self) -> Capability {
                Capability {
                    stat: true,
                    read: true,
                    read_with_suffix: false,
                    ..Default::default()
                }
            }

            async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
                let len = self
                    .stat_lens
                    .lock()
                    .expect("mutex must not poison")
                    .pop_front()
                    .expect("unexpected stat call");
                Ok(RpStat::new(MetadataBuilder::file(len).build()))
            }

            fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
                Ok(oio::StreamReader::new(SizeChangingReader {
                    contents: self.contents.clone(),
                    emit_metadata: self.emit_metadata,
                }))
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        let ctx = OperationContext::new();

        // 1. Growth (8 -> 12) for open() and read(): re-resolves Suffix(4) from 4..8 to 8..12.
        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [8],
            [
                bytes::Bytes::from_static(b"0123456789AB"),
                bytes::Bytes::from_static(b"0123456789AB"),
            ],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, mut stream) = reader.open_dyn(BytesRange::suffix(4)).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 12);
        assert_eq!(stream.read_all_dyn().await?.to_bytes(), &b"89AB"[..]);

        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [8],
            [
                bytes::Bytes::from_static(b"0123456789AB"),
                bytes::Bytes::from_static(b"0123456789AB"),
            ],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, buf) = reader.read_dyn(BytesRange::suffix(4)).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 12);
        assert_eq!(buf.to_bytes(), &b"89AB"[..]);

        // 2. Repeated growth (8 -> 12 -> 16) fails with temporary Conflict + context on open() and read().
        for use_read in [false, true] {
            let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
                [8],
                [
                    bytes::Bytes::from_static(b"0123456789AB"),
                    bytes::Bytes::from_static(b"0123456789ABCDEF"),
                ],
            ));
            let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
            let err = if use_read {
                reader
                    .read_dyn(BytesRange::suffix(4))
                    .await
                    .expect_err("repeated size change must fail")
            } else {
                reader
                    .open_dyn(BytesRange::suffix(4))
                    .await
                    .err()
                    .expect("repeated size change must fail")
            };
            assert_eq!(err.kind(), ErrorKind::Conflict);
            assert!(err.is_temporary());
            let msg = err.to_string();
            assert!(msg.contains("at read"), "missing operation in {msg}");
            assert!(msg.contains("path: f"), "missing path in {msg}");
            assert!(
                msg.contains("expected_size: 12"),
                "missing expected_size in {msg}"
            );
            assert!(
                msg.contains("actual_size: 16"),
                "missing actual_size in {msg}"
            );
        }

        // 3. Shrink (12 -> 6) for open() and read(): triggers RangeNotSatisfied, re-stats to 6, returns 2..6.
        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [12, 6],
            [
                bytes::Bytes::from_static(b"012345"),
                bytes::Bytes::from_static(b"012345"),
            ],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, mut stream) = reader.open_dyn(BytesRange::suffix(4)).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 6);
        assert_eq!(stream.read_all_dyn().await?.to_bytes(), &b"2345"[..]);

        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [12, 6],
            [
                bytes::Bytes::from_static(b"012345"),
                bytes::Bytes::from_static(b"012345"),
            ],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, buf) = reader.read_dyn(BytesRange::suffix(4)).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 6);
        assert_eq!(buf.to_bytes(), &b"2345"[..]);

        // 4. suffix >= stat_len growth (stat=8, actual=12, Suffix(100)) returns full 12 bytes.
        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [8],
            [
                bytes::Bytes::from_static(b"0123456789AB"),
                bytes::Bytes::from_static(b"0123456789AB"),
            ],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, mut stream) = reader.open_dyn(BytesRange::suffix(100)).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 12);
        assert_eq!(
            stream.read_all_dyn().await?.to_bytes(),
            &b"0123456789AB"[..]
        );

        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [8],
            [
                bytes::Bytes::from_static(b"0123456789AB"),
                bytes::Bytes::from_static(b"0123456789AB"),
            ],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, buf) = reader.read_dyn(BytesRange::suffix(100)).await?;
        assert_eq!(rp.metadata().unwrap().content_length(), 12);
        assert_eq!(buf.to_bytes(), &b"0123456789AB"[..]);

        // 5. Suffix(0) and empty object succeed with empty payload.
        let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
            [8],
            [bytes::Bytes::from_static(b"01234567")],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (_, mut stream) = reader.open_dyn(BytesRange::suffix(0)).await?;
        assert!(stream.read_all_dyn().await?.is_empty());

        let sim = SimulateLayer::default()
            .apply_service(SizeChangingService::servicer([0], [bytes::Bytes::new()]));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (_, buf) = reader.read_dyn(BytesRange::suffix(4)).await?;
        assert!(buf.is_empty());

        // 6. RpRead::default() (no metadata, e.g. PositionReader/Fs) does not false-error.
        let sim = SimulateLayer::default().apply_service(SizeChangingService::without_metadata(
            [8],
            [bytes::Bytes::from_static(b"01234567")],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, mut stream) = reader.open_dyn(BytesRange::suffix(4)).await?;
        assert!(rp.metadata().is_none());
        assert_eq!(stream.read_all_dyn().await?.to_bytes(), &b"4567"[..]);

        let sim = SimulateLayer::default().apply_service(SizeChangingService::without_metadata(
            [8],
            [bytes::Bytes::from_static(b"01234567")],
        ));
        let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
        let (rp, buf) = reader.read_dyn(BytesRange::suffix(4)).await?;
        assert!(rp.metadata().is_none());
        assert_eq!(buf.to_bytes(), &b"4567"[..]);

        // 7. RangeNotSatisfied with unchanged re-stat length passes through RangeNotSatisfied.
        for use_read in [false, true] {
            let sim = SimulateLayer::default().apply_service(SizeChangingService::servicer(
                [12, 12],
                [bytes::Bytes::from_static(b"012345")],
            ));
            let reader = sim.read_dyn(&ctx, "f", OpRead::new())?;
            let err = if use_read {
                reader
                    .read_dyn(BytesRange::suffix(4))
                    .await
                    .expect_err("unchanged re-stat length must pass through RangeNotSatisfied")
            } else {
                reader
                    .open_dyn(BytesRange::suffix(4))
                    .await
                    .err()
                    .expect("unchanged re-stat length must pass through RangeNotSatisfied")
            };
            assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
        }

        Ok(())
    }

    #[tokio::test]
    async fn servicer_flat_lister_stops_polling_lister_after_not_found() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, Clone)]
        enum PageStep {
            Entries(Vec<oio::Entry>, bool),
            Err(ErrorKind),
        }

        struct ScriptedPageList {
            steps: Arc<Mutex<std::collections::VecDeque<PageStep>>>,
            calls: Arc<AtomicUsize>,
        }

        impl oio::PageList for ScriptedPageList {
            async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
                let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(count <= 3, "lister was re-polled {count} times after error");

                let step = self
                    .steps
                    .lock()
                    .expect("mutex must not poison")
                    .pop_front()
                    .unwrap_or(PageStep::Err(ErrorKind::NotFound));
                match step {
                    PageStep::Entries(entries, done) => {
                        ctx.entries.extend(entries);
                        ctx.done = done;
                        Ok(())
                    }
                    PageStep::Err(kind) => {
                        // Leave `ctx.done = false`, matching real `PageList` implementations.
                        Err(Error::new(kind, "injected page error"))
                    }
                }
            }
        }

        #[derive(Debug)]
        struct PagedListService {
            pages: HashMap<String, (Vec<PageStep>, Arc<AtomicUsize>)>,
        }

        impl Service for PagedListService {
            type Reader = ();
            type Writer = ();
            type Lister = oio::PageLister<ScriptedPageList>;
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock")
            }

            fn capability(&self) -> Capability {
                Capability {
                    list: true,
                    list_with_recursive: false,
                    ..Default::default()
                }
            }

            fn list(&self, _: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
                let (steps, calls) = self
                    .pages
                    .get(path)
                    .cloned()
                    .unwrap_or_else(|| (vec![PageStep::Entries(vec![], true)], Arc::default()));
                Ok(oio::PageLister::new(ScriptedPageList {
                    steps: Arc::new(Mutex::new(steps.into())),
                    calls,
                }))
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        // 1. Discovered directory returns NotFound on its first async page.
        let sub_calls = Arc::new(AtomicUsize::new(0));
        let srv = Arc::new(PagedListService {
            pages: HashMap::from([
                (
                    "/".to_string(),
                    (
                        vec![PageStep::Entries(
                            vec![
                                oio::Entry::new("sub/", MetadataBuilder::dir().build()),
                                oio::Entry::new("b", MetadataBuilder::file(1).build()),
                            ],
                            true,
                        )],
                        Arc::default(),
                    ),
                ),
                (
                    "sub/".to_string(),
                    (vec![PageStep::Err(ErrorKind::NotFound)], sub_calls.clone()),
                ),
            ]),
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());
        let entries = op.list_with("/").recursive(true).await?;
        let paths: Vec<_> = entries.iter().map(Entry::path).collect();
        assert_eq!(paths, ["b", "/"]);
        assert_eq!(sub_calls.load(Ordering::SeqCst), 1);

        // 2. Active discovered directory yields page 1, then returns NotFound on page 2.
        let sub_calls = Arc::new(AtomicUsize::new(0));
        let srv = Arc::new(PagedListService {
            pages: HashMap::from([
                (
                    "/".to_string(),
                    (
                        vec![PageStep::Entries(
                            vec![
                                oio::Entry::new("sub/", MetadataBuilder::dir().build()),
                                oio::Entry::new("b", MetadataBuilder::file(1).build()),
                            ],
                            true,
                        )],
                        Arc::default(),
                    ),
                ),
                (
                    "sub/".to_string(),
                    (
                        vec![
                            PageStep::Entries(
                                vec![oio::Entry::new("sub/a", MetadataBuilder::file(1).build())],
                                false,
                            ),
                            PageStep::Err(ErrorKind::NotFound),
                        ],
                        sub_calls.clone(),
                    ),
                ),
            ]),
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());
        let entries = op.list_with("/").recursive(true).await?;
        let paths: Vec<_> = entries.iter().map(Entry::path).collect();
        assert_eq!(paths, ["sub/a", "b", "/"]);
        assert_eq!(sub_calls.load(Ordering::SeqCst), 2);

        // 3. Initial directory returns NotFound on its first async page.
        let missing_calls = Arc::new(AtomicUsize::new(0));
        let srv = Arc::new(PagedListService {
            pages: HashMap::from([(
                "missing/".to_string(),
                (
                    vec![PageStep::Err(ErrorKind::NotFound)],
                    missing_calls.clone(),
                ),
            )]),
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());
        let entries = op.list_with("missing/").recursive(true).await?;
        assert!(entries.is_empty());
        assert_eq!(missing_calls.load(Ordering::SeqCst), 1);

        Ok(())
    }

    #[tokio::test]
    async fn servicer_flat_lister_permission_denied_parity() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug, Clone)]
        enum DirBehavior {
            SyncDenied,
            AsyncFirstDenied,
            AsyncSecondDenied(Vec<oio::Entry>),
            Entries(Vec<oio::Entry>),
        }

        struct ScriptedLister {
            steps: std::collections::VecDeque<Result<Option<oio::Entry>>>,
            calls: Arc<AtomicUsize>,
        }

        impl oio::List for ScriptedLister {
            async fn next(&mut self) -> Result<Option<oio::Entry>> {
                let count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(count <= 4, "lister was polled {count} times");
                self.steps.pop_front().unwrap_or(Ok(None))
            }
        }

        #[derive(Debug)]
        struct PermissionService {
            dirs: HashMap<String, (DirBehavior, Arc<AtomicUsize>)>,
        }

        impl Service for PermissionService {
            type Reader = ();
            type Writer = ();
            type Lister = ScriptedLister;
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock")
            }

            fn capability(&self) -> Capability {
                Capability {
                    list: true,
                    list_with_recursive: false,
                    ..Default::default()
                }
            }

            fn list(&self, _: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
                let Some((behavior, calls)) = self.dirs.get(path).cloned() else {
                    return Ok(ScriptedLister {
                        steps: std::collections::VecDeque::new(),
                        calls: Arc::default(),
                    });
                };
                match behavior {
                    DirBehavior::SyncDenied => Err(Error::new(
                        ErrorKind::PermissionDenied,
                        "sync permission denied",
                    )),
                    DirBehavior::AsyncFirstDenied => Ok(ScriptedLister {
                        steps: std::collections::VecDeque::from([Err(Error::new(
                            ErrorKind::PermissionDenied,
                            "async permission denied on first poll",
                        ))]),
                        calls,
                    }),
                    DirBehavior::AsyncSecondDenied(entries) => {
                        let mut steps: std::collections::VecDeque<_> =
                            entries.into_iter().map(|e| Ok(Some(e))).collect();
                        steps.push_back(Err(Error::new(
                            ErrorKind::PermissionDenied,
                            "async permission denied on second poll",
                        )));
                        Ok(ScriptedLister { steps, calls })
                    }
                    DirBehavior::Entries(entries) => Ok(ScriptedLister {
                        steps: entries.into_iter().map(|e| Ok(Some(e))).collect(),
                        calls,
                    }),
                }
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        // Initial starting directory propagates PermissionDenied for sync, first-poll, and second-poll failures.
        for behavior in [
            DirBehavior::SyncDenied,
            DirBehavior::AsyncFirstDenied,
            DirBehavior::AsyncSecondDenied(vec![oio::Entry::new(
                "forbidden/first",
                MetadataBuilder::file(1).build(),
            )]),
        ] {
            let srv = Arc::new(PermissionService {
                dirs: HashMap::from([("forbidden/".to_string(), (behavior, Arc::default()))]),
            }) as Servicer;
            let op =
                Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());
            let err = op
                .list_with("forbidden/")
                .recursive(true)
                .await
                .expect_err("initial directory must propagate PermissionDenied");
            assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        }

        // Discovered subdirectory skips PermissionDenied for sync, first-poll, and second-poll failures.
        let cases = [
            (DirBehavior::SyncDenied, vec!["ok/file", "ok/", "/"], 0usize),
            (
                DirBehavior::AsyncFirstDenied,
                vec!["ok/file", "ok/", "/"],
                1usize,
            ),
            (
                DirBehavior::AsyncSecondDenied(vec![oio::Entry::new(
                    "forbidden/a",
                    MetadataBuilder::file(1).build(),
                )]),
                vec!["forbidden/a", "ok/file", "ok/", "/"],
                2usize,
            ),
        ];

        for (behavior, expected_paths, expected_polls) in cases {
            let calls = Arc::new(AtomicUsize::new(0));
            let srv = Arc::new(PermissionService {
                dirs: HashMap::from([
                    (
                        "/".to_string(),
                        (
                            DirBehavior::Entries(vec![
                                oio::Entry::new("forbidden/", MetadataBuilder::dir().build()),
                                oio::Entry::new("ok/", MetadataBuilder::dir().build()),
                            ]),
                            Arc::default(),
                        ),
                    ),
                    ("forbidden/".to_string(), (behavior, calls.clone())),
                    (
                        "ok/".to_string(),
                        (
                            DirBehavior::Entries(vec![oio::Entry::new(
                                "ok/file",
                                MetadataBuilder::file(1).build(),
                            )]),
                            Arc::default(),
                        ),
                    ),
                ]),
            }) as Servicer;
            let op =
                Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());
            let entries = op.list_with("/").recursive(true).await?;
            let paths: Vec<_> = entries.iter().map(Entry::path).collect();
            assert_eq!(paths, expected_paths);
            assert_eq!(calls.load(Ordering::SeqCst), expected_polls);
        }

        Ok(())
    }

    #[tokio::test]
    async fn simulate_reader_forwards_version_when_resolving_suffix() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct VersionedSuffixService {
            capability: Capability,
            stat_calls: Arc<AtomicUsize>,
            observed_stat_versions: Arc<Mutex<Vec<Option<String>>>>,
        }

        struct VersionedReader {
            data: bytes::Bytes,
        }

        impl oio::StreamRead for VersionedReader {
            async fn open(
                &self,
                range: BytesRange,
            ) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
                let content_range = range.to_content_range(self.data.len())?;
                Ok((
                    RpRead::new(MetadataBuilder::file(self.data.len() as u64).build()),
                    Box::new(Buffer::from(self.data.slice(content_range)))
                        as Box<dyn oio::ReadStreamDyn>,
                ))
            }
        }

        impl Service for VersionedSuffixService {
            type Reader = oio::StreamReader<VersionedReader>;
            type Writer = ();
            type Lister = ();
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock")
            }

            fn capability(&self) -> Capability {
                self.capability
            }

            async fn stat(&self, _: &OperationContext, _: &str, args: OpStat) -> Result<RpStat> {
                self.stat_calls.fetch_add(1, Ordering::SeqCst);
                self.observed_stat_versions
                    .lock()
                    .expect("mutex must not poison")
                    .push(args.version().map(str::to_owned));
                let len = match args.version() {
                    Some("v1") => 10,
                    _ => 4,
                };
                Ok(RpStat::new(MetadataBuilder::file(len).build()))
            }

            fn read(&self, _: &OperationContext, _: &str, args: OpRead) -> Result<Self::Reader> {
                let data = match args.version() {
                    Some("v1") => bytes::Bytes::from_static(b"0123456789"),
                    _ => bytes::Bytes::from_static(b"ABCD"),
                };
                Ok(oio::StreamReader::new(VersionedReader { data }))
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        let ctx = OperationContext::new();

        let stat_calls = Arc::new(AtomicUsize::new(0));
        let observed_versions = Arc::new(Mutex::new(Vec::new()));
        let srv = Arc::new(VersionedSuffixService {
            capability: Capability {
                read: true,
                read_with_version: true,
                read_with_suffix: false,
                stat: true,
                stat_with_version: true,
                ..Default::default()
            },
            stat_calls: stat_calls.clone(),
            observed_stat_versions: observed_versions.clone(),
        }) as Servicer;
        let sim = SimulateLayer::default().apply_service(srv);
        let (_, args, _) = options::ReadOptions {
            version: Some("v1".to_string()),
            ..Default::default()
        }
        .into();
        let reader = sim.read_dyn(&ctx, "f", args)?;
        let (_, buf) = reader.read_dyn(BytesRange::suffix(4)).await?;
        assert_eq!(buf.to_bytes(), &b"6789"[..]);
        assert_eq!(stat_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *observed_versions.lock().expect("mutex must not poison"),
            vec![Some("v1".to_string())]
        );

        Ok(())
    }

    #[tokio::test]
    async fn simulate_delete_recursive_capability_and_version_handling() -> Result<()> {
        type DeletedRecords = Arc<Mutex<Vec<(String, Option<String>)>>>;

        struct VersionedListAndDelete {
            capability: Capability,
            entries: Vec<oio::Entry>,
            deleted: DeletedRecords,
        }

        struct VecLister(std::collections::VecDeque<oio::Entry>);

        impl oio::List for VecLister {
            async fn next(&mut self) -> Result<Option<oio::Entry>> {
                Ok(self.0.pop_front())
            }
        }

        struct RecordingDeleter {
            deleted: DeletedRecords,
        }

        impl oio::Delete for RecordingDeleter {
            async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
                self.deleted
                    .lock()
                    .expect("mutex must not poison")
                    .push((path.to_string(), args.version().map(str::to_owned)));
                Ok(())
            }

            async fn close(&mut self) -> Result<()> {
                Ok(())
            }
        }

        impl Debug for VersionedListAndDelete {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("VersionedListAndDelete").finish()
            }
        }

        impl Service for VersionedListAndDelete {
            type Reader = ();
            type Writer = ();
            type Lister = VecLister;
            type Deleter = RecordingDeleter;
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock")
            }

            fn capability(&self) -> Capability {
                self.capability
            }

            fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
                Ok(VecLister(self.entries.clone().into()))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                Ok(RecordingDeleter {
                    deleted: self.deleted.clone(),
                })
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                Err(Error::new(ErrorKind::Unsupported, "unsupported"))
            }
        }

        let mut meta = MetadataBuilder::file(4);
        meta.version("g1");
        let versioned_entry = oio::Entry::new("dir/a", meta.build());

        // 1. Disabling list_recursive on a non-recursive service must not advertise delete_with_recursive.
        let deleted = Arc::new(Mutex::new(Vec::new()));
        let srv = Arc::new(VersionedListAndDelete {
            capability: Capability {
                list: true,
                list_with_recursive: false,
                delete: true,
                delete_with_recursive: false,
                ..Default::default()
            },
            entries: vec![versioned_entry.clone()],
            deleted: deleted.clone(),
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv)
            .layer(SimulateLayer::default().with_list_recursive(false));
        assert!(!op.info().capability().delete_with_recursive);
        let err = op
            .delete_with("dir/")
            .recursive(true)
            .await
            .expect_err("recursive delete without recursive list must fail");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(deleted.lock().expect("mutex must not poison").is_empty());

        // 2. Native list_with_recursive still allows simulated delete_with_recursive when list_recursive simulation is off.
        let srv = Arc::new(VersionedListAndDelete {
            capability: Capability {
                list: true,
                list_with_recursive: true,
                delete: true,
                delete_with_recursive: false,
                ..Default::default()
            },
            entries: vec![versioned_entry.clone()],
            deleted: Arc::default(),
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv)
            .layer(SimulateLayer::default().with_list_recursive(false));
        assert!(op.info().capability().delete_with_recursive);

        // Use the listed version unless the caller supplied one.
        let deleted = Arc::new(Mutex::new(Vec::new()));
        let srv = Arc::new(VersionedListAndDelete {
            capability: Capability {
                list: true,
                list_with_recursive: true,
                delete: true,
                delete_with_recursive: false,
                delete_with_version: true,
                ..Default::default()
            },
            entries: vec![versioned_entry],
            deleted: deleted.clone(),
        }) as Servicer;
        let op = Operator::from_parts(OperationContext::new(), srv).layer(SimulateLayer::default());
        op.delete_with("dir/").recursive(true).await?;
        op.delete_with("dir/")
            .recursive(true)
            .version("explicit-v")
            .await?;
        assert_eq!(
            *deleted.lock().expect("mutex must not poison"),
            vec![
                ("dir/a".to_string(), Some("g1".to_string())),
                ("dir/a".to_string(), Some("explicit-v".to_string())),
            ]
        );

        Ok(())
    }
}
