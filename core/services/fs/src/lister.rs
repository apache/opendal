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

use std::path::Path;
use std::sync::Arc;
use std::vec::IntoIter;

use opendal_core::EntryMode;
use opendal_core::ErrorKind;
use opendal_core::MetadataBuilder;
use opendal_core::Result;
use opendal_core::raw::*;
use tokio::fs::ReadDir;

use super::core::FsCore;

pub struct FsLister {
    core: Arc<FsCore>,
    prefix: String,
    recursive: bool,
    start_after: Option<String>,
    next_dir: Option<PendingDir>,
    active: Vec<DirectoryEntries>,
}

/// A directory waiting to be listed.
///
/// The initial directory comes from the caller and has not been validated yet.
/// `oio::Entry` rejects paths whose metadata exceeds the compact block limit,
/// so materialize it only after `fs_list` succeeds.
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

    fn into_entry(self) -> oio::Entry {
        match self {
            PendingDir::Initial(path) => oio::Entry::new(&path, MetadataBuilder::dir().build()),
            PendingDir::Discovered(entry) => entry,
        }
    }
}

enum DirectoryEntries {
    Streaming {
        reader: ReadDir,
        after_children: Option<oio::Entry>,
    },
    Sorted(IntoIter<oio::Entry>),
}

impl FsLister {
    pub(super) fn new(core: Arc<FsCore>, path: &str, args: OpList) -> Self {
        let directory = if path.ends_with('/') {
            path
        } else {
            get_parent(path)
        };
        let prefix = if path == "/" { "" } else { path };
        Self {
            core,
            prefix: prefix.to_string(),
            recursive: args.recursive(),
            start_after: args.start_after().map(str::to_owned),
            next_dir: Some(PendingDir::Initial(directory.to_string())),
            active: Vec::new(),
        }
    }
}

impl oio::List for FsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            if let Some(dir) = &self.next_dir {
                // A discovered child always has an active parent directory.
                let discovered_child = matches!(dir, PendingDir::Discovered(_));
                let mut reader = match self.core.fs_list(dir.path()).await {
                    Ok(Some(reader)) => reader,
                    Ok(None) => {
                        if discovered_child {
                            log::warn!(
                                "FS recursive listing skipped missing directory: {}",
                                dir.path()
                            );
                        }
                        self.next_dir = None;
                        continue;
                    }
                    Err(err)
                        if discovered_child
                            && matches!(
                                err.kind(),
                                ErrorKind::PermissionDenied | ErrorKind::NotFound
                            ) =>
                    {
                        log::warn!(
                            "FS recursive listing skipped directory {}: {err}",
                            dir.path()
                        );
                        self.next_dir = None;
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                let include_dir = dir.path().starts_with(&self.prefix)
                    && self
                        .start_after
                        .as_deref()
                        .is_none_or(|marker| dir.path() > marker);

                if let Some(marker) = &self.start_after {
                    // Keep next_dir until the buffer is complete, so an error or
                    // cancelled read leaves the entire directory available to retry.
                    let mut entries = Vec::new();
                    while let Some(entry) = next_entry(
                        &mut reader,
                        &self.core.root,
                        &self.prefix,
                        Some(marker),
                        self.recursive,
                    )
                    .await?
                    {
                        entries.push(entry);
                    }
                    let mut dir = self.next_dir.take().filter(|_| include_dir);
                    // Unlike other directory paths, "/" is not a prefix of its
                    // children. It can sort between entries in the root.
                    if dir.as_ref().is_some_and(|dir| dir.path() == "/") {
                        entries.extend(dir.take().map(PendingDir::into_entry));
                    }
                    entries.sort_unstable_by(|a, b| a.path().cmp(b.path()));
                    self.active
                        .push(DirectoryEntries::Sorted(entries.into_iter()));
                    // A non-root directory sorts before all its descendants.
                    if let Some(dir) = dir {
                        return Ok(Some(dir.into_entry()));
                    }
                } else {
                    let mut dir = self.next_dir.take().filter(|_| include_dir);
                    // Keep bottom-up ordering for ordinary recursive listings.
                    let after_children = if self.recursive {
                        dir.take().map(PendingDir::into_entry)
                    } else {
                        None
                    };
                    self.active.push(DirectoryEntries::Streaming {
                        reader,
                        after_children,
                    });
                    if let Some(dir) = dir {
                        return Ok(Some(dir.into_entry()));
                    }
                }
            }

            let Some(directory) = self.active.last_mut() else {
                return Ok(None);
            };
            let entry = match directory {
                DirectoryEntries::Streaming { reader, .. } => {
                    next_entry(reader, &self.core.root, &self.prefix, None, self.recursive).await?
                }
                DirectoryEntries::Sorted(entries) => entries.next(),
            };
            let Some(entry) = entry else {
                if let Some(DirectoryEntries::Streaming {
                    after_children: Some(dir),
                    ..
                }) = self.active.pop()
                {
                    return Ok(Some(dir));
                }
                continue;
            };
            if self.recursive && entry.mode().is_dir() && entry.path() != "/" {
                self.next_dir = Some(PendingDir::Discovered(entry));
            } else {
                return Ok(Some(entry));
            }
        }
    }
}

async fn next_entry(
    reader: &mut ReadDir,
    root: &Path,
    prefix: &str,
    start_after: Option<&str>,
    recursive: bool,
) -> Result<Option<oio::Entry>> {
    loop {
        let de = match reader.next_entry().await {
            Ok(Some(de)) => de,
            Ok(None) => return Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Directory can be removed during listing; stop gracefully.
                return Ok(None);
            }
            Err(e) => return Err(new_std_io_error(e)),
        };

        let entry_path = de.path();
        let raw = entry_path
            .strip_prefix(root)
            .expect("cannot fail because the prefix is iterated")
            .to_string_lossy();
        #[cfg(windows)]
        let raw = raw.replace('\\', "/");
        let rel_path = normalize_path(&raw);

        let ft = match de.file_type().await {
            Ok(ft) => ft,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Entry can be deleted between readdir and stat calls.
                continue;
            }
            Err(e) => return Err(new_std_io_error(e)),
        };
        let (path, mode) = if ft.is_dir() {
            // Make sure we are returning the correct path.
            (&format!("{rel_path}/"), EntryMode::DIR)
        } else if ft.is_file() {
            (&rel_path, EntryMode::FILE)
        } else {
            (&rel_path, EntryMode::Unknown)
        };

        if !path.starts_with(prefix) {
            continue;
        }
        // A directory containing the marker can still have later children.
        if let Some(marker) = start_after
            && path.as_str() <= marker
            && !(recursive && mode.is_dir() && marker.starts_with(path))
        {
            continue;
        }

        let de_metadata = match de.metadata().await {
            Ok(de_metadata) => de_metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Entry can be deleted between readdir and metadata calls.
                continue;
            }
            Err(e) => return Err(new_std_io_error(e)),
        };
        let last_modified = match de_metadata.modified() {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(e) => return Err(new_std_io_error(e)),
        };
        let mut metadata = match mode {
            EntryMode::FILE => MetadataBuilder::file(de_metadata.len()),
            EntryMode::DIR => MetadataBuilder::dir(),
            EntryMode::Unknown => MetadataBuilder::unknown(),
        };
        metadata.last_modified(Timestamp::try_from(last_modified)?);

        return Ok(Some(oio::Entry::new(path, metadata.build())));
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use opendal_core::EntryMode;
    use opendal_core::ErrorKind;
    use opendal_core::Operator;

    use crate::Fs;

    #[tokio::test]
    async fn list_file_with_backslash_in_name() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("has\\slash"), b"data").unwrap();
        let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();

        let entries = op.list("/").await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].metadata().mode(), EntryMode::DIR);
        assert_eq!(entries[1].metadata().mode(), EntryMode::FILE);
        assert!(
            !entries[1].path().ends_with('/'),
            "file path must not end with /: {}",
            entries[1].path()
        );
    }

    #[tokio::test]
    async fn glob_does_not_traverse_discovered_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();
        op.write("dir/photo.jpg", "data").await.unwrap();
        std::os::unix::fs::symlink("dir", root.path().join("alias")).unwrap();

        let entries = op.list_with("/").glob("**/*.jpg").await.unwrap();
        let paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
        assert_eq!(paths, ["dir/photo.jpg"]);
    }

    #[tokio::test]
    async fn list_recursively_skips_unreadable_child() {
        let root = tempfile::tempdir().unwrap();
        let locked = root.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::write(root.path().join("z.txt"), b"data").unwrap();
        let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();
        let permissions = fs::metadata(&locked).unwrap().permissions();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

        // Privileged processes can read the directory even with mode 000.
        match fs::read_dir(&locked) {
            Ok(_) => {
                fs::set_permissions(&locked, permissions).unwrap();
                return;
            }
            Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied),
        }

        let recursive = op.list_with("/").recursive(true).await;
        let resumed = op.list_with("/").recursive(true).start_after("a").await;
        let direct = op.list_with("locked/").recursive(true).await;
        let direct_resumed = op
            .list_with("locked/")
            .recursive(true)
            .start_after("a")
            .await;
        fs::set_permissions(&locked, permissions).unwrap();

        let recursive = recursive.unwrap();
        let paths: Vec<_> = recursive.iter().map(|entry| entry.path()).collect();
        assert_eq!(paths, ["z.txt", "/"]);
        let resumed = resumed.unwrap();
        let paths: Vec<_> = resumed.iter().map(|entry| entry.path()).collect();
        assert_eq!(paths, ["z.txt"]);
        assert_eq!(direct.unwrap_err().kind(), ErrorKind::PermissionDenied);
        assert_eq!(
            direct_resumed.unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
    }

    #[tokio::test]
    async fn glob_skips_unreadable_child() {
        let root = tempfile::tempdir().unwrap();
        let locked = root.path().join("data/locked");
        fs::create_dir_all(&locked).unwrap();
        fs::create_dir_all(root.path().join("data/open")).unwrap();
        fs::write(locked.join("a.jpg"), b"data").unwrap();
        fs::write(root.path().join("data/open/b.jpg"), b"data").unwrap();
        let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();
        let permissions = fs::metadata(&locked).unwrap().permissions();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

        // Privileged processes can read the directory even with mode 000.
        match fs::read_dir(&locked) {
            Ok(_) => {
                fs::set_permissions(&locked, permissions).unwrap();
                return;
            }
            Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied),
        }

        let any_depth = op.list_with("/").glob("data/**/*.jpg").await;
        let one_level = op.list_with("/").glob("data/*/*.jpg").await;
        let direct = op.list_with("data/locked/").glob("*.jpg").await;
        fs::set_permissions(&locked, permissions).unwrap();

        for entries in [any_depth.unwrap(), one_level.unwrap()] {
            let paths: Vec<_> = entries.iter().map(|entry| entry.path()).collect();
            assert_eq!(paths, ["data/open/b.jpg"]);
        }
        assert_eq!(direct.unwrap_err().kind(), ErrorKind::PermissionDenied);
    }
}

#[cfg(test)]
mod oversized_path {
    use opendal_core::Operator;

    use crate::Fs;

    #[tokio::test]
    async fn list_oversized_path_does_not_panic() {
        let root = tempfile::tempdir().unwrap();
        let op = Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap();

        // A path this long cannot fit in the compact metadata block, so listing
        // must fail before building an entry instead of after a successful lookup.
        let component = "a".repeat(65531);
        for path in [format!("{component}/"), format!("{component}/child")] {
            assert_oversized_list_result(op.list(&path).await);
            assert_oversized_list_result(op.list_with(&path).recursive(true).await);
            assert_oversized_list_result(op.list_with(&path).start_after("marker").await);
            assert_oversized_list_result(
                op.list_with(&path)
                    .recursive(true)
                    .start_after("marker")
                    .await,
            );
        }
    }

    #[cfg(unix)]
    fn assert_oversized_list_result(result: opendal_core::Result<Vec<opendal_core::Entry>>) {
        assert_eq!(
            result.unwrap_err().kind(),
            opendal_core::ErrorKind::Unexpected,
            "oversized path must fail before building an entry"
        );
    }

    #[cfg(not(unix))]
    fn assert_oversized_list_result(result: opendal_core::Result<Vec<opendal_core::Entry>>) {
        // Windows may reject the path at the OS level or report it as missing.
        // Either outcome is fine as long as listing does not panic.
        if let Ok(entries) = result {
            assert!(entries.is_empty());
        }
    }
}
