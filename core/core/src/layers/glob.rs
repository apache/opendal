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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use futures::future::Either;
use futures::future::select;

use crate::raw::oio::{self, List};
use crate::raw::*;
use crate::*;

/// Child directories that the walk lists ahead.
const LIST_AHEAD: usize = 8;
/// Entries that the walk reads from a listing at a time if the caller sets no
/// page size. S3 returns up to 1,000 keys per request.
const BATCH: usize = 1000;

/// Provide client-side glob support for services that support listing.
///
/// OpenDAL installs this layer by default. Services that advertise native glob
/// support receive list requests unchanged. Otherwise the layer uses the pattern
/// to skip unrelated directories. If the walk would list every directory below
/// a directory, as below `t/` for `t/**/*.parquet` or below `logs/` for
/// `logs/**`, and the service supports recursive listing, the layer filters one
/// recursive listing of that directory instead. With a cursor, versions, or
/// delete markers, it filters an underlying listing to preserve the service's
/// cursor and entries.
/// With an executor, the walk lists up to 8 child directories of the current
/// directory in the background and returns entries in the same order.
/// For a custom stack, install it before layers that resolve symbolic links
/// when completing entry metadata.
///
/// See [`ListOptions::glob`](options::ListOptions::glob) for the pattern syntax.
#[derive(Debug, Clone, Copy, Default)]
pub struct GlobLayer;

impl Layer for GlobLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        let capability = inner.capability();
        if !capability.list || capability.list_with_glob {
            return inner;
        }
        Arc::new(GlobService { inner })
    }
}

#[derive(Debug)]
struct GlobService {
    inner: Servicer,
}

impl Service for GlobService {
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
        let mut capability = self.inner.capability();
        capability.list_with_glob = true;
        capability
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

    fn list(&self, ctx: &OperationContext, path: &str, mut args: OpList) -> Result<Self::Lister> {
        let Some(glob) = args.glob() else {
            return self.inner.list(ctx, path, args);
        };
        let pattern = Arc::new(GlobPattern::new(glob)?);
        args.clear_glob();
        let base = if path.ends_with('/') {
            path
        } else {
            get_parent(path)
        };

        if args.start_after().is_some() || args.versions() || args.deleted() {
            // A recursive service listing can return versions of directory markers
            // that a delimiter listing would fold into a common prefix.
            let recursive = pattern.spans_directories();
            let start_after = args.start_after().map(str::to_owned);
            let implied = (recursive && !args.versions() && !args.deleted())
                .then(ImpliedDirectories::default);
            args.set_recursive(recursive);
            return Ok(Box::new(FilteredLister {
                inner: self.inner.list(ctx, path, args)?,
                pattern,
                base: base.to_owned(),
                root: base.to_owned(),
                start_after,
                implied,
                pending: None,
            }));
        }

        let mut directory = if base == "/" {
            String::new()
        } else {
            base.to_owned()
        };
        let mut position = 0;
        for component in pattern.literal_directories() {
            directory.push_str(component);
            directory.push('/');
            position += 1;
        }
        if directory.is_empty() {
            directory.push('/');
        }
        let prefix = if path == "/" { "" } else { path };
        let can_match = position == 0 || directory.starts_with(prefix);

        // A recursive listing reads every key below `root`. It costs no more
        // requests than the walk only if the walk lists every directory too.
        if can_match
            && self.inner.capability().list_with_recursive
            && pattern.walks_whole_subtree(position)
        {
            args.set_recursive(true);
            let root = if directory.starts_with(prefix) {
                directory
            } else {
                path.to_owned()
            };
            return Ok(Box::new(FilteredLister {
                inner: self.inner.list(ctx, &root, args)?,
                pattern,
                base: base.to_owned(),
                root,
                start_after: None,
                implied: Some(ImpliedDirectories::default()),
                pending: None,
            }));
        }

        args.set_recursive(false);
        let directories = if can_match {
            vec![Directory::new(directory, vec![position], false, false)]
        } else {
            Vec::new()
        };
        Ok(Box::new(GuidedLister {
            walk: Arc::new(Walk {
                ctx: ctx.clone(),
                inner: self.inner.clone(),
                pattern,
                batch: args.limit().unwrap_or(BATCH).max(1),
                args,
                base: base.to_owned(),
            }),
            prefix: prefix.to_owned(),
            directories,
            // Tokio runs background tasks on wasm32 only inside a `LocalSet`.
            background: cfg!(not(target_arch = "wasm32")),
        }))
    }
}

/// What a listing needs to read a directory.
struct Walk {
    ctx: OperationContext,
    inner: Servicer,
    pattern: Arc<GlobPattern>,
    args: OpList,
    base: String,
    /// Entries to read from a listing at a time.
    batch: usize,
}

impl Walk {
    /// Open the listing of `path`. A recursive listing is filtered.
    fn list(&self, path: &str, recursive: bool) -> Result<oio::Lister> {
        let mut args = self.args.clone();
        args.set_recursive(recursive);
        let lister = self.inner.list(&self.ctx, path, args)?;
        if !recursive {
            return Ok(lister);
        }
        Ok(Box::new(FilteredLister {
            inner: lister,
            pattern: self.pattern.clone(),
            base: self.base.clone(),
            root: path.to_owned(),
            start_after: None,
            implied: Some(ImpliedDirectories::default()),
            pending: None,
        }))
    }
}

struct Directory {
    path: String,
    positions: Vec<usize>,
    /// Found while listing a parent rather than requested by the caller.
    discovered: bool,
    /// Filter one recursive listing instead of walking the directory.
    recursive: bool,
    lister: Option<oio::Lister>,
    ahead: Option<Ahead>,
    /// An error to return after the queued entries.
    error: Option<Error>,
    done: bool,
    /// Entries to return in order. `true` enters the next directory in `children`.
    steps: VecDeque<(Option<oio::Entry>, bool)>,
    children: VecDeque<Directory>,
}

impl Directory {
    fn new(path: String, positions: Vec<usize>, discovered: bool, recursive: bool) -> Self {
        Self {
            path,
            positions,
            discovered,
            recursive,
            lister: None,
            ahead: None,
            error: None,
            done: false,
            steps: VecDeque::new(),
            children: VecDeque::new(),
        }
    }

    /// Queue `entry` if the walk returns or enters it.
    fn push(&mut self, walk: &Walk, prefix: &str, entry: oio::Entry) {
        if self.recursive {
            self.steps.push_back((Some(entry), false));
            return;
        }
        if !entry.path().starts_with(prefix) {
            return;
        }
        let Some(name) = relative_child(entry.path(), &self.path) else {
            return;
        };
        let name = name.trim_end_matches('/');
        if name.contains('/') {
            return;
        }
        let pattern = &walk.pattern;
        let positions = pattern.advance(&self.positions, name);
        let is_dir = entry.mode().is_dir();
        let matches = pattern.accepts(&positions, is_dir);
        // All viable positions travel with the same child. Overlapping
        // recursive matches therefore visit and emit each entry only once.
        let enter = is_dir && pattern.can_descend(&positions);
        if enter {
            // As in `list`, this costs no more requests than the walk.
            let recursive = walk.inner.capability().list_with_recursive
                && positions
                    .iter()
                    .any(|&position| pattern.walks_whole_subtree(position));
            let path = entry.path().to_owned();
            self.children
                .push_back(Directory::new(path, positions, true, recursive));
        }
        if enter || matches {
            self.steps.push_back((matches.then_some(entry), enter));
        }
    }
}

/// A background listing of the first entries of a directory.
struct Ahead {
    task: Task<Batch>,
    /// Set by the task or the walk, whichever starts the listing first.
    started: Arc<AtomicBool>,
}

/// Entries that a background listing read.
#[derive(Default)]
struct Batch {
    /// `None` if the listing ended or did not open.
    lister: Option<oio::Lister>,
    entries: Vec<oio::Entry>,
    /// Returned after `entries`.
    error: Option<Error>,
}

/// Read the first entries of the directory at `path` unless the walk started
/// the listing first.
async fn list_batch(
    walk: Arc<Walk>,
    path: String,
    recursive: bool,
    started: Arc<AtomicBool>,
) -> Batch {
    let mut batch = Batch::default();
    if started.swap(true, Ordering::AcqRel) {
        return batch;
    }
    let mut lister = match walk.list(&path, recursive) {
        Ok(lister) => lister,
        Err(err) => {
            batch.error = Some(err);
            return batch;
        }
    };
    while batch.entries.len() < walk.batch {
        let mut next = pin!(lister.next());
        // `TimeoutLayer` bounds the walk, not a background listing. Most entries
        // come from a fetched page, so start a timer only for a request.
        let next = match next.as_mut().now_or_never() {
            Some(next) => next,
            None => match walk.ctx.executor().timeout() {
                Some(timeout) => match select(next, timeout).await {
                    Either::Left((next, _)) => next,
                    Either::Right(_) => Err(Error::new(ErrorKind::Unexpected, "list timeout")
                        .with_context("path", &path)
                        .set_temporary()),
                },
                None => next.await,
            },
        };
        match next {
            Ok(Some(entry)) => batch.entries.push(entry),
            Ok(None) => return batch,
            Err(err) => {
                batch.error = Some(err);
                break;
            }
        }
    }
    batch.lister = Some(lister);
    batch
}

struct GuidedLister {
    walk: Arc<Walk>,
    prefix: String,
    directories: Vec<Directory>,
    /// Whether the executor accepts background listings.
    background: bool,
}

impl GuidedLister {
    /// List the next child directories of the current directory in the background.
    fn list_ahead(&mut self) {
        if !self.background {
            return;
        }
        let Some(directory) = self.directories.last_mut() else {
            return;
        };
        for child in directory.children.iter_mut().take(LIST_AHEAD) {
            if child.ahead.is_some() {
                continue;
            }
            let started = Arc::new(AtomicBool::new(false));
            let listing = list_batch(
                self.walk.clone(),
                child.path.clone(),
                child.recursive,
                started.clone(),
            );
            match self.walk.ctx.executor().execute(listing) {
                Ok(task) => child.ahead = Some(Ahead { task, started }),
                // Without an executor, the walk lists each directory when it enters it.
                Err(_) => {
                    self.background = false;
                    return;
                }
            }
        }
    }

    /// Queue the next entries of the current directory. Return `false` at the
    /// end of its listing.
    async fn fill(&mut self) -> Result<bool> {
        let walk = &self.walk;
        let directory = self.directories.last_mut().expect("directory is queued");
        if let Some(err) = directory.error.take() {
            return Err(err);
        }
        if let Some(ahead) = &mut directory.ahead {
            // A task may never start, for example on a current-thread runtime
            // that no thread drives. Then the walk lists the directory itself.
            if ahead.started.swap(true, Ordering::AcqRel) {
                let batch = (&mut ahead.task).await;
                directory.ahead = None;
                let Batch {
                    lister,
                    entries,
                    error,
                } = batch?;
                directory.done = lister.is_none() && error.is_none();
                directory.lister = lister;
                directory.error = error;
                for entry in entries {
                    directory.push(walk, &self.prefix, entry);
                }
                return Ok(true);
            }
            directory.ahead = None;
        }
        if directory.done {
            return Ok(false);
        }
        if directory.lister.is_none() {
            directory.lister = Some(walk.list(&directory.path, directory.recursive)?);
        }
        for _ in 0..walk.batch {
            let lister = directory.lister.as_mut().expect("listing is open");
            match lister.next().await {
                Ok(Some(entry)) => directory.push(walk, &self.prefix, entry),
                Ok(None) => {
                    directory.done = true;
                    break;
                }
                Err(err) => {
                    directory.error = Some(err);
                    break;
                }
            }
            // Only background listings need the entries after the next step.
            if !self.background && !directory.steps.is_empty() {
                break;
            }
        }
        Ok(true)
    }
}

impl List for GuidedLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            self.list_ahead();
            let Some(directory) = self.directories.last_mut() else {
                return Ok(None);
            };
            if let Some((entry, enter)) = directory.steps.pop_front() {
                if enter {
                    let child = directory.children.pop_front().expect("child is queued");
                    self.directories.push(child);
                }
                if entry.is_some() {
                    return Ok(entry);
                }
                continue;
            }
            match self.fill().await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(err)
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {}
                // Like recursive listings, skip discovered directories that cannot
                // be read. The starting directory still reports the error.
                Err(err)
                    if err.kind() == ErrorKind::PermissionDenied
                        && self.directories.last().is_some_and(|d| d.discovered) =>
                {
                    let path = &self.directories.last().expect("directory is queued").path;
                    log::warn!("glob listing skipped directory {path}: {err}");
                }
                Err(err) => return Err(err),
            }
            self.directories.pop();
        }
    }
}

struct FilteredLister {
    inner: oio::Lister,
    pattern: Arc<GlobPattern>,
    base: String,
    /// The listed path. As in the guided listing, a listed directory is not
    /// returned.
    root: String,
    start_after: Option<String>,
    implied: Option<ImpliedDirectories>,
    pending: Option<oio::Entry>,
}

/// Object stores return only keys. The guided listing also returns each
/// directory that exists only as a key prefix.
#[derive(Default)]
struct ImpliedDirectories {
    returned: HashSet<String>,
    /// A directory whose parents and itself are already checked.
    checked: String,
}

impl FilteredLister {
    /// Return the next unchecked parent of `entry` that matches, else `entry` if
    /// it matches. After a parent, `entry` waits in `pending`.
    fn take_next(&mut self, entry: oio::Entry) -> Option<oio::Entry> {
        let root = self.root.as_str();
        let accepts = |path: &str, is_dir: bool| {
            !(path == root && root.ends_with('/'))
                && relative_child(path, &self.base)
                    .is_some_and(|relative| self.pattern.matches(relative, is_dir))
        };
        let path = entry.path();
        let is_dir = entry.mode().is_dir();
        let Some(implied) = &mut self.implied else {
            return accepts(path, is_dir).then_some(entry);
        };

        let listed = if root == "/" { 0 } else { root.len() };
        let end = path.trim_end_matches('/').len();
        let start = shared_directory_len(&implied.checked, path).min(end);
        for (index, _) in path[start..end].match_indices('/') {
            let parent = &path[..=start + index];
            if parent.len() > listed
                && self
                    .start_after
                    .as_deref()
                    .is_none_or(|marker| parent > marker)
                && !implied.returned.contains(parent)
                && accepts(parent, true)
            {
                implied.returned.insert(parent.to_owned());
                implied.checked.clear();
                implied.checked.push_str(parent);
                let parent = oio::Entry::new(parent, MetadataBuilder::dir().build());
                self.pending = Some(entry);
                return Some(parent);
            }
        }

        let directory_len = if is_dir {
            path.len()
        } else {
            path.rfind('/').map_or(0, |index| index + 1)
        };
        implied.checked.clear();
        implied.checked.push_str(&path[..directory_len]);
        if !accepts(path, is_dir) || (is_dir && !implied.returned.insert(path.to_owned())) {
            return None;
        }
        Some(entry)
    }
}

/// Length of the longest shared prefix of `a` and `b` that ends with `/`.
fn shared_directory_len(a: &str, b: &str) -> usize {
    let shared = a.bytes().zip(b.bytes()).take_while(|(a, b)| a == b).count();
    a.as_bytes()[..shared]
        .iter()
        .rposition(|&byte| byte == b'/')
        .map_or(0, |index| index + 1)
}

impl List for FilteredLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let entry = match self.pending.take() {
                Some(entry) => entry,
                None => match self.inner.next().await? {
                    Some(entry) => entry,
                    None => return Ok(None),
                },
            };
            if let Some(entry) = self.take_next(entry) {
                return Ok(Some(entry));
            }
        }
    }
}

fn relative_child<'a>(path: &'a str, directory: &str) -> Option<&'a str> {
    let relative = if directory == "/" {
        path.trim_start_matches('/')
    } else {
        path.strip_prefix(directory)?
    };
    (!relative.is_empty()).then_some(relative)
}

#[cfg(test)]
mod tests;
