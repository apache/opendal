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
use std::sync::Arc;

use super::SimulateService;
use crate::raw::oio::{self, List};
use crate::raw::*;
use crate::*;

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
        vec![Directory {
            path: directory,
            positions: vec![position],
            lister: None,
            discovered: false,
            recursive: false,
        }]
    } else {
        Vec::new()
    };
    Ok(GlobLister::Guided(GuidedLister {
        ctx: ctx.clone(),
        inner: service.srv.clone(),
        pattern,
        args,
        base: base.to_owned(),
        prefix: prefix.to_owned(),
        directories,
    }))
}

struct Directory {
    path: String,
    positions: Vec<usize>,
    lister: Option<oio::Lister>,
    /// Found while listing a parent rather than requested by the caller.
    discovered: bool,
    /// Filter one recursive listing instead of walking the directory.
    recursive: bool,
}

impl Directory {
    async fn next(
        &mut self,
        ctx: &OperationContext,
        inner: &Servicer,
        args: &OpList,
        pattern: &Arc<GlobPattern>,
        base: &str,
    ) -> Result<Option<oio::Entry>> {
        if self.lister.is_none() {
            let mut args = args.clone();
            args.set_recursive(self.recursive);
            let lister = inner.list(ctx, &self.path, args)?;
            self.lister = Some(if self.recursive {
                Box::new(FilteredLister {
                    inner: lister,
                    pattern: pattern.clone(),
                    base: base.to_owned(),
                    root: self.path.clone(),
                    start_after: None,
                    implied: Some(ImpliedDirectories::default()),
                    pending: None,
                })
            } else {
                lister
            });
        }
        self.lister
            .as_mut()
            .expect("directory listing is open")
            .next()
            .await
    }
}

pub(super) struct GuidedLister {
    ctx: OperationContext,
    inner: Servicer,
    pattern: Arc<GlobPattern>,
    args: OpList,
    base: String,
    prefix: String,
    directories: Vec<Directory>,
}

impl List for GuidedLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        while let Some(directory) = self.directories.last_mut() {
            let entry = match directory
                .next(
                    &self.ctx,
                    &self.inner,
                    &self.args,
                    &self.pattern,
                    &self.base,
                )
                .await
            {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    self.directories.pop();
                    continue;
                }
                Err(err)
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) =>
                {
                    self.directories.pop();
                    continue;
                }
                // Like recursive listings, skip discovered directories that cannot
                // be read. The starting directory still reports the error.
                Err(err) if err.kind() == ErrorKind::PermissionDenied && directory.discovered => {
                    log::warn!("glob listing skipped directory {}: {err}", directory.path);
                    self.directories.pop();
                    continue;
                }
                Err(err) => return Err(err),
            };
            if directory.recursive {
                return Ok(Some(entry));
            }
            if !entry.path().starts_with(&self.prefix) {
                continue;
            }
            let Some(name) = relative_child(entry.path(), &directory.path) else {
                continue;
            };
            let name = name.trim_end_matches('/');
            if name.contains('/') {
                continue;
            }
            let positions = self.pattern.advance(&directory.positions, name);
            let is_dir = entry.mode().is_dir();
            let matches = self.pattern.accepts(&positions, is_dir);
            // All viable positions travel with the same child. Overlapping
            // recursive matches therefore visit and emit each entry only once.
            if is_dir && self.pattern.can_descend(&positions) {
                // As in `list`, this costs no more requests than the walk.
                let recursive = self.inner.capability().list_with_recursive
                    && positions
                        .iter()
                        .any(|&position| self.pattern.walks_whole_subtree(position));
                self.directories.push(Directory {
                    path: entry.path().to_owned(),
                    positions,
                    lister: None,
                    discovered: true,
                    recursive,
                });
            }
            if matches {
                return Ok(Some(entry));
            }
        }
        Ok(None)
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
