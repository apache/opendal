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

use std::collections::{HashSet, VecDeque};
#[cfg(not(target_arch = "wasm32"))]
use std::pin::pin;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use asyncband::semaphore::{OwnedSemaphorePermit, Semaphore};
#[cfg(not(target_arch = "wasm32"))]
use futures::FutureExt;
#[cfg(not(target_arch = "wasm32"))]
use futures::future::{Either, select};

use super::SimulateService;
use crate::raw::oio::{self, List};
use crate::raw::*;
use crate::*;

#[cfg(not(target_arch = "wasm32"))]
const LIST_CONCURRENCY: usize = 8;

pub(super) enum GlobLister {
    Guided(GuidedLister),
    Filtered(FilteredLister),
}

impl List for GlobLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        match self {
            Self::Guided(lister) => lister.next().await,
            Self::Filtered(lister) => lister.next().await,
        }
    }
}

pub(super) fn simulate_list(
    service: &SimulateService,
    ctx: &OperationContext,
    path: &str,
    mut args: OpList,
) -> Result<GlobLister> {
    let glob = args
        .glob()
        .expect("glob simulation requires a glob pattern");
    let pattern = Arc::new(GlobPattern::new(glob)?);
    args.clear_glob();
    let base = if path.ends_with('/') {
        path
    } else {
        get_parent(path)
    };
    let cap = service.srv.capability();

    if args.start_after().is_some() || args.versions() || args.deleted() {
        // A recursive service listing can return versions of directory markers
        // that a delimiter listing would fold into a common prefix.
        let recursive = pattern.spans_directories();
        if recursive && !cap.list_with_recursive && !service.config.list_recursive {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "recursive listing is required to simulate glob with start_after, versions, or deleted",
            ));
        }
        let start_after = args.start_after().map(str::to_owned);
        let implied =
            (recursive && !args.versions() && !args.deleted()).then(ImpliedDirectories::default);
        args.set_recursive(recursive);
        let inner = service
            .simulate_list_without_glob(ctx, path, args)?
            .into_oio_lister();
        return Ok(GlobLister::Filtered(FilteredLister {
            inner,
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
    if can_match && cap.list_with_recursive && pattern.walks_whole_subtree(position) {
        args.set_recursive(true);
        let root = if directory.starts_with(prefix) {
            directory
        } else {
            path.to_owned()
        };
        return Ok(GlobLister::Filtered(FilteredLister {
            inner: service.srv.list(ctx, &root, args)?,
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
    Ok(GlobLister::Guided(GuidedLister {
        walk: Arc::new(Walk {
            ctx: ctx.clone(),
            inner: service.srv.clone(),
            pattern,
            args,
            base: base.to_owned(),
        }),
        prefix: prefix.to_owned(),
        directories,
        next_directory: None,
        // The active directory has a reserved slot. Descent can always make
        // progress even while earlier levels have outstanding sibling reads.
        // On wasm32, background Tokio tasks would require a LocalSet.
        #[cfg(not(target_arch = "wasm32"))]
        prefetch_slots: Some(Arc::new(Semaphore::new(LIST_CONCURRENCY - 1))),
    }))
}

struct Walk {
    ctx: OperationContext,
    inner: Servicer,
    pattern: Arc<GlobPattern>,
    args: OpList,
    base: String,
}

impl Walk {
    fn list(&self, path: &str, recursive: bool) -> Result<oio::Lister> {
        let mut args = self.args.clone();
        args.set_recursive(recursive);
        let lister = self.inner.list(&self.ctx, path, args)?;
        if recursive {
            Ok(Box::new(FilteredLister {
                inner: lister,
                pattern: self.pattern.clone(),
                base: self.base.clone(),
                root: path.to_owned(),
                start_after: None,
                implied: Some(ImpliedDirectories::default()),
                pending: None,
            }))
        } else {
            Ok(lister)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
type ListedEntry = (oio::Lister, Result<Option<oio::Entry>>);

#[derive(Default)]
enum Listing {
    #[default]
    Unopened,
    Open(oio::Lister),
    #[cfg(not(target_arch = "wasm32"))]
    Prefetched {
        task: Task<Result<Option<ListedEntry>>>,
        started: Arc<AtomicBool>,
    },
}

enum Step {
    Yield(oio::Entry),
    Descend {
        child: Directory,
        entry: Option<oio::Entry>,
    },
}

struct Directory {
    path: String,
    positions: Vec<usize>,
    /// Found while listing a parent rather than requested by the caller.
    discovered: bool,
    /// Filter one recursive listing instead of walking the directory.
    recursive: bool,
    listing: Listing,
    steps: VecDeque<Step>,
}

impl Directory {
    fn new(path: String, positions: Vec<usize>, discovered: bool, recursive: bool) -> Self {
        Self {
            path,
            positions,
            discovered,
            recursive,
            listing: Listing::Unopened,
            steps: VecDeque::new(),
        }
    }

    /// Open the listing and get its first entry. A paged lister buffers the
    /// rest of the page itself. Reading another entry here could start a second
    /// request and delay a match that is already available.
    #[cfg(not(target_arch = "wasm32"))]
    fn prefetch(&mut self, walk: &Arc<Walk>, permit: OwnedSemaphorePermit) -> Result<()> {
        let walk = walk.clone();
        let path = self.path.clone();
        let recursive = self.recursive;
        let started = Arc::new(AtomicBool::new(false));
        let task_started = started.clone();
        let executor = walk.ctx.executor().clone();
        let task = executor.execute(async move {
            let _permit = permit;
            if task_started.swap(true, Ordering::AcqRel) {
                return Ok(None);
            }
            let mut lister = walk.list(&path, recursive)?;
            let entry = {
                let mut next = pin!(lister.next());
                // The outer TimeoutLayer covers the foreground walk. Use the
                // executor's timer for I/O started before the caller needs it.
                match next.as_mut().now_or_never() {
                    Some(entry) => entry,
                    None => match walk.ctx.executor().timeout() {
                        Some(timeout) => match select(next, timeout).await {
                            Either::Left((entry, _)) => entry,
                            Either::Right(_) => {
                                Err(Error::new(ErrorKind::Unexpected, "list timeout")
                                    .with_context("path", &path)
                                    .set_temporary())
                            }
                        },
                        None => next.await,
                    },
                }
            };
            Ok(Some((lister, entry)))
        })?;
        self.listing = Listing::Prefetched { task, started };
        Ok(())
    }

    async fn next_entry(&mut self, walk: &Walk) -> Result<Option<oio::Entry>> {
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(self.listing, Listing::Prefetched { .. }) {
            let Listing::Prefetched { task, started } = std::mem::take(&mut self.listing) else {
                unreachable!("directory has a prefetch task");
            };
            // If the executor has not polled the task, open the directory
            // here. Once we wait on it, cancellation drops the task so a
            // retry can reopen the directory from its first entry.
            if started.swap(true, Ordering::AcqRel) {
                let (lister, entry) = task.await??.expect("prefetch started the listing");
                self.listing = Listing::Open(lister);
                return entry;
            }
        }
        if matches!(self.listing, Listing::Unopened) {
            self.listing = Listing::Open(walk.list(&self.path, self.recursive)?);
        }
        let Listing::Open(lister) = &mut self.listing else {
            unreachable!("directory listing is open");
        };
        lister.next().await
    }

    fn step(&self, walk: &Walk, prefix: &str, entry: oio::Entry) -> Option<Step> {
        if self.recursive {
            return Some(Step::Yield(entry));
        }
        if !entry.path().starts_with(prefix) {
            return None;
        }
        let name = relative_child(entry.path(), &self.path)?.trim_end_matches('/');
        if name.contains('/') {
            return None;
        }
        let positions = walk.pattern.advance(&self.positions, name);
        let is_dir = entry.mode().is_dir();
        let matches = walk.pattern.accepts(&positions, is_dir);
        // All viable positions travel with the same child. Overlapping
        // recursive matches therefore visit and emit each entry only once.
        if is_dir && walk.pattern.can_descend(&positions) {
            let recursive = walk.inner.capability().list_with_recursive
                && positions
                    .iter()
                    .any(|&position| walk.pattern.walks_whole_subtree(position));
            let child = Self::new(entry.path().to_owned(), positions, true, recursive);
            return Some(Step::Descend {
                child,
                entry: matches.then_some(entry),
            });
        }
        matches.then_some(Step::Yield(entry))
    }

    async fn next_step(&mut self, walk: &Walk, prefix: &str) -> Result<Option<Step>> {
        if let Some(step) = self.steps.pop_front() {
            return Ok(Some(step));
        }
        while let Some(entry) = self.next_entry(walk).await? {
            if let Some(step) = self.step(walk, prefix, entry) {
                return Ok(Some(step));
            }
        }
        Ok(None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prefetch_siblings(
        &mut self,
        walk: &Arc<Walk>,
        prefix: &str,
        slots: &Arc<Semaphore>,
    ) -> Result<()> {
        // Descend without starting another parent request. Pausing such a
        // request would hold transport resources and escape foreground retries.
        for _ in self.steps.len()..LIST_CONCURRENCY - 1 {
            if matches!(
                self.steps.back(),
                Some(Step::Yield(_) | Step::Descend { entry: Some(_), .. })
            ) {
                break;
            }
            let Listing::Open(lister) = &mut self.listing else {
                break;
            };
            let Some(entry) = lister.next_buffered(oio::BufferedEntries) else {
                break;
            };
            self.steps.extend(self.step(walk, prefix, entry));
        }
        for step in &mut self.steps {
            let Step::Descend { child, entry: None } = step else {
                break;
            };
            if !matches!(child.listing, Listing::Unopened) {
                continue;
            }
            let Some(permit) = slots.clone().try_acquire_owned(1) else {
                break;
            };
            child.prefetch(walk, permit)?;
        }
        Ok(())
    }
}

pub(super) struct GuidedLister {
    walk: Arc<Walk>,
    prefix: String,
    directories: Vec<Directory>,
    // A matching directory must be yielded before starting its listing.
    next_directory: Option<Directory>,
    #[cfg(not(target_arch = "wasm32"))]
    prefetch_slots: Option<Arc<Semaphore>>,
}

impl List for GuidedLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(directory) = self.directories.last_mut() else {
                return Ok(None);
            };
            if let Some(child) = self.next_directory.take() {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(slots) = &self.prefetch_slots
                    && directory
                        .prefetch_siblings(&self.walk, &self.prefix, slots)
                        .is_err()
                {
                    // Without an executor, list each directory on entry.
                    self.prefetch_slots = None;
                }
                self.directories.push(child);
                continue;
            }
            match directory.next_step(&self.walk, &self.prefix).await {
                Ok(Some(Step::Yield(entry))) => return Ok(Some(entry)),
                Ok(Some(Step::Descend { child, entry })) => {
                    self.next_directory = Some(child);
                    if entry.is_some() {
                        return Ok(entry);
                    }
                    continue;
                }
                Ok(None) => {}
                Err(err)
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {}
                // Like recursive listings, skip discovered directories that cannot
                // be read. The starting directory still reports the error.
                Err(err) if err.kind() == ErrorKind::PermissionDenied && directory.discovered => {
                    log::warn!("glob listing skipped directory {}: {err}", directory.path);
                }
                Err(err) => return Err(err),
            }
            self.directories.pop();
        }
    }
}

pub(super) struct FilteredLister {
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
