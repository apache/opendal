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

use crate::raw::glob_matcher::GlobPattern;
use crate::raw::oio::{self, List};
use crate::raw::*;
use crate::*;

/// Provide client-side glob support for services that support listing.
///
/// OpenDAL installs this layer by default. Services that advertise native glob
/// support receive list requests unchanged. Otherwise the layer uses the pattern
/// to skip unrelated directories. If the walk would list every directory below
/// the leading literal directories of the pattern, as for `t/**/*.parquet`, and
/// the service supports recursive listing, the layer filters one recursive
/// listing instead. With a cursor, versions, or delete markers, it filters an
/// underlying listing to preserve the service's cursor and entries.
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
        let pattern = GlobPattern::new(glob)?;
        args.clear_glob();
        let base = if path.ends_with('/') {
            path
        } else {
            get_parent(path)
        };

        if args.start_after().is_some() || args.versions() || args.deleted() {
            // A recursive service listing can return versions of directory markers
            // that a delimiter listing would fold into a common prefix.
            args.set_recursive(pattern.spans_directories());
            return Ok(Box::new(FilteredLister {
                inner: self.inner.list(ctx, path, args)?,
                pattern,
                base: base.to_owned(),
                root: base.to_owned(),
                implied: None,
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
            }]
        } else {
            Vec::new()
        };
        Ok(Box::new(GuidedLister {
            ctx: ctx.clone(),
            inner: self.inner.clone(),
            pattern,
            args,
            prefix: prefix.to_owned(),
            directories,
        }))
    }
}

struct Directory {
    path: String,
    positions: Vec<usize>,
    lister: Option<oio::Lister>,
    /// Found while listing a parent rather than requested by the caller.
    discovered: bool,
}

impl Directory {
    async fn next(
        &mut self,
        ctx: &OperationContext,
        inner: &Servicer,
        args: &OpList,
    ) -> Result<Option<oio::Entry>> {
        if self.lister.is_none() {
            self.lister = Some(inner.list(ctx, &self.path, args.clone())?);
        }
        self.lister
            .as_mut()
            .expect("directory listing is open")
            .next()
            .await
    }
}

struct GuidedLister {
    ctx: OperationContext,
    inner: Servicer,
    pattern: GlobPattern,
    args: OpList,
    prefix: String,
    directories: Vec<Directory>,
}

impl List for GuidedLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        while let Some(directory) = self.directories.last_mut() {
            let entry = match directory.next(&self.ctx, &self.inner, &self.args).await {
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
                self.directories.push(Directory {
                    path: entry.path().to_owned(),
                    positions,
                    lister: None,
                    discovered: true,
                });
            }
            if matches {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
}

struct FilteredLister {
    inner: oio::Lister,
    pattern: GlobPattern,
    base: String,
    /// The listed path. As in the guided listing, a listed directory is not
    /// returned.
    root: String,
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
            if parent.len() > listed && !implied.returned.contains(parent) && accepts(parent, true)
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
