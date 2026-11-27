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
use std::pin::pin;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use asyncband::semaphore::{OwnedSemaphorePermit, Semaphore};
use futures::FutureExt;
use futures::future::{Either, select};

use super::SimulateService;
use crate::raw::oio::{self, List};
use crate::raw::*;
use crate::*;

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
        slots: cfg!(not(target_arch = "wasm32"))
            .then(|| Arc::new(Semaphore::new(LIST_CONCURRENCY - 1))),
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

type ListedEntry = (oio::Lister, Result<Option<oio::Entry>>);

struct Prefetch {
    task: Task<Result<Option<ListedEntry>>>,
    started: Arc<AtomicBool>,
}

#[cfg(not(target_arch = "wasm32"))]
struct PendingRead {
    // Polling requires exclusive access to the directory.
    future: Mutex<BoxedStaticFuture<ListedEntry>>,
    _permit: OwnedSemaphorePermit,
}

enum Step {
    Yield(oio::Entry),
    Descend {
        child: Box<Directory>,
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
    lister: Option<oio::Lister>,
    prefetch: Option<Prefetch>,
    #[cfg(not(target_arch = "wasm32"))]
    pending_read: Option<PendingRead>,
    steps: VecDeque<Step>,
    /// End of the listing or an error to return after the buffered steps.
    end: Option<Result<()>>,
}

impl Directory {
    fn new(path: String, positions: Vec<usize>, discovered: bool, recursive: bool) -> Self {
        Self {
            path,
            positions,
            discovered,
            recursive,
            lister: None,
            prefetch: None,
            #[cfg(not(target_arch = "wasm32"))]
            pending_read: None,
            steps: VecDeque::new(),
            end: None,
        }
    }

    /// Open the listing and get its first entry. A paged lister buffers the
    /// rest of the page itself. Reading another entry here could start a second
    /// request and delay a match that is already available.
    fn prefetch(&mut self, walk: &Arc<Walk>, permit: Option<OwnedSemaphorePermit>) -> Result<()> {
        if self.prefetch.is_some() {
            return Ok(());
        }
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
        self.prefetch = Some(Prefetch { task, started });
        Ok(())
    }

    async fn next_entry(&mut self, walk: &Walk) -> Result<Option<oio::Entry>> {
        if let Some(prefetch) = &mut self.prefetch {
            // An executor that is not being driven may accept work without
            // starting it. Claim the listing here in that case.
            if prefetch.started.swap(true, Ordering::AcqRel) {
                let result = (&mut prefetch.task).await;
                self.prefetch = None;
                let (lister, entry) = result??.expect("prefetch started the listing");
                self.lister = Some(lister);
                return entry;
            }
            self.prefetch = None;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(pending) = &mut self.pending_read {
            let (lister, entry) = pending.future.get_mut().expect("exclusive access").await;
            self.pending_read = None;
            self.lister = Some(lister);
            return entry;
        }
        if self.lister.is_none() {
            self.lister = Some(walk.list(&self.path, self.recursive)?);
        }
        self.lister
            .as_mut()
            .expect("directory is open")
            .next()
            .await
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
            let child = Box::new(Self::new(
                entry.path().to_owned(),
                positions,
                true,
                recursive,
            ));
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
        if let Some(end) = self.end.take() {
            return end.map(|()| None);
        }
        while let Some(entry) = self.next_entry(walk).await? {
            if let Some(step) = self.step(walk, prefix, entry) {
                return Ok(Some(step));
            }
        }
        Ok(None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prefetch_siblings(
        &mut self,
        walk: &Arc<Walk>,
        prefix: &str,
        slots: &Arc<Semaphore>,
    ) -> Result<()> {
        // Look for siblings without waiting on the parent's next page before
        // descending. Keep a pending read and its slot until the walk resumes
        // this directory so that it never drops and reissues that request.
        for _ in self.steps.len()..LIST_CONCURRENCY - 1 {
            if self.pending_read.is_some() || self.end.is_some() {
                break;
            }
            if matches!(
                self.steps.back(),
                Some(Step::Yield(_) | Step::Descend { entry: Some(_), .. })
            ) {
                break;
            }
            let Some(permit) = slots.clone().try_acquire_owned(1) else {
                break;
            };
            let mut lister = self.lister.take().expect("parent directory is open");
            let mut future: BoxedStaticFuture<ListedEntry> = Box::pin(async move {
                let entry = lister.next().await;
                (lister, entry)
            });
            let Poll::Ready((lister, entry)) = futures::poll!(&mut future) else {
                self.pending_read = Some(PendingRead {
                    future: Mutex::new(future),
                    _permit: permit,
                });
                break;
            };
            self.lister = Some(lister);
            match entry {
                Ok(Some(entry)) => {
                    self.steps.extend(self.step(walk, prefix, entry));
                }
                end => self.end = Some(end.map(|_| ())),
            }
        }
        for step in &mut self.steps {
            let Step::Descend { child, entry: None } = step else {
                break;
            };
            if child.prefetch.is_some() {
                continue;
            }
            let Some(permit) = slots.clone().try_acquire_owned(1) else {
                break;
            };
            child.prefetch(walk, Some(permit))?;
        }
        Ok(())
    }
}

pub(super) struct GuidedLister {
    walk: Arc<Walk>,
    prefix: String,
    directories: Vec<Directory>,
    next_directory: Option<Box<Directory>>,
    slots: Option<Arc<Semaphore>>,
}

impl List for GuidedLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let Some(directory) = self.directories.last_mut() else {
                return Ok(None);
            };
            if let Some(mut child) = self.next_directory.take() {
                if let Some(_slots) = &self.slots {
                    let submitted = child.prefetch(&self.walk, None);
                    #[cfg(not(target_arch = "wasm32"))]
                    let submitted = match submitted {
                        Ok(()) => {
                            directory
                                .prefetch_siblings(&self.walk, &self.prefix, _slots)
                                .await
                        }
                        Err(err) => Err(err),
                    };
                    if submitted.is_err() {
                        // Without an executor, list each directory on entry.
                        self.slots = None;
                    }
                }
                self.directories.push(*child);
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
