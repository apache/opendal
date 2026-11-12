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

use std::collections::VecDeque;
use std::path::Path;
use std::path::PathBuf;

use opendal_core::EntryMode;
use opendal_core::MetadataBuilder;
use opendal_core::Result;
use opendal_core::raw::*;

pub struct FsLister<P> {
    root: PathBuf,

    current_path: Option<String>,

    rd: P,

    start_after: Option<String>,
    recursive: bool,

    // Entries collected for start_after filtering, sorted by path.
    buffered: Option<VecDeque<oio::Entry>>,
}

impl<P> FsLister<P> {
    pub fn new(
        root: &Path,
        path: &str,
        rd: P,
        start_after: Option<String>,
        recursive: bool,
    ) -> Self {
        Self {
            root: root.to_owned(),
            current_path: Some(path.to_string()),
            rd,
            start_after,
            recursive,
            buffered: None,
        }
    }

    /// Whether an entry should be listed after the start_after marker.
    fn keep(&self, path: &str, mode: EntryMode) -> bool {
        let Some(marker) = self.start_after.as_deref() else {
            return true;
        };
        if path > marker {
            return true;
        }
        // The entry is at or before the marker. Keep a directory that may
        // hold later entries so recursive listing can still descend into it;
        // the walker lists subdirectories with a fresh list call.
        self.recursive && mode.is_dir() && marker.starts_with(path)
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<P> Sync for FsLister<P> {}

impl oio::List for FsLister<tokio::fs::ReadDir> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        // since list should return path itself, we return it first
        if let Some(path) = self.current_path.take() {
            // The listed dir itself is also subject to start_after: it is
            // dropped when it is at or before the marker, just like any
            // other entry.
            if self.keep(&path, EntryMode::DIR) {
                let e = oio::Entry::new(path.as_str(), MetadataBuilder::dir().build());
                return Ok(Some(e));
            }
        }

        // start_after needs entries in sorted order, so read the whole
        // directory once, filter and sort before yielding.
        if self.start_after.is_some() {
            if self.buffered.is_none() {
                let mut entries = Vec::new();
                while let Some(e) = self.next_raw().await? {
                    entries.push(e);
                }
                entries.sort_by(|a, b| a.path().cmp(b.path()));
                self.buffered = Some(entries.into());
            }
            return Ok(self
                .buffered
                .as_mut()
                .expect("buffer must be set")
                .pop_front());
        }

        self.next_raw().await
    }
}

impl FsLister<tokio::fs::ReadDir> {
    /// Read the next raw entry from the directory, applying the start_after filter.
    async fn next_raw(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            let de = match self.rd.next_entry().await {
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
                .strip_prefix(&self.root)
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

            if !self.keep(path, mode) {
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
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use opendal_core::raw::oio::List;

    #[tokio::test]
    async fn list_file_with_backslash_in_name() {
        let root = std::env::temp_dir().join("opendal-test-fs-lister-backslash");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        std::fs::write(root.join("has\\slash"), b"data").unwrap();

        let rd = tokio::fs::read_dir(&root).await.unwrap();
        let mut lister = FsLister::new(&root, "/", rd, None, false);

        let first = lister.next().await.unwrap().unwrap();
        assert_eq!(first.mode(), EntryMode::DIR);

        let file_entry = lister.next().await.unwrap().unwrap();
        assert_eq!(file_entry.mode(), EntryMode::FILE);
        assert!(
            !file_entry.path().ends_with('/'),
            "file path must not end with /: {}",
            file_entry.path()
        );

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn list_with_start_after() {
        let root = std::env::temp_dir().join("opendal-test-fs-lister-start-after");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        for name in ["a.txt", "b.txt", "c.txt"] {
            std::fs::write(root.join(name), b"data").unwrap();
        }
        std::fs::create_dir_all(root.join("d")).unwrap();
        std::fs::write(root.join("d/x"), b"data").unwrap();
        std::fs::write(root.join("d/y"), b"data").unwrap();

        // Entries at or before the marker are dropped, the rest come back sorted.
        // The listed dir itself is dropped too: it sorts before the marker.
        let rd = tokio::fs::read_dir(&root).await.unwrap();
        let mut lister = FsLister::new(&root, "/", rd, Some("a.txt".to_string()), false);
        let mut paths = vec![];
        while let Some(e) = lister.next().await.unwrap() {
            paths.push(e.path().to_string());
        }
        assert_eq!(paths, vec!["b.txt", "c.txt", "d/"]);

        // A marker inside a directory drops that directory for a flat list.
        let rd = tokio::fs::read_dir(&root).await.unwrap();
        let mut lister = FsLister::new(&root, "/", rd, Some("d/x".to_string()), false);
        let mut paths = vec![];
        while let Some(e) = lister.next().await.unwrap() {
            paths.push(e.path().to_string());
        }
        assert!(paths.is_empty());

        // The same marker keeps the directory for a recursive list, so the
        // walker can still descend into it.
        let rd = tokio::fs::read_dir(&root).await.unwrap();
        let mut lister = FsLister::new(&root, "/", rd, Some("d/x".to_string()), true);
        let mut paths = vec![];
        while let Some(e) = lister.next().await.unwrap() {
            paths.push(e.path().to_string());
        }
        assert_eq!(paths, vec!["d/"]);

        std::fs::remove_dir_all(&root).unwrap();
    }
}
