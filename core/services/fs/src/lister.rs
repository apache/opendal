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
use std::path::PathBuf;

use opendal_core::EntryMode;
use opendal_core::Metadata;
use opendal_core::Result;
use opendal_core::raw::*;

pub struct FsLister<P> {
    root: PathBuf,

    current_path: Option<String>,

    rd: P,
}

impl<P> FsLister<P> {
    pub fn new(root: &Path, path: &str, rd: P) -> Self {
        Self {
            root: root.to_owned(),
            current_path: Some(path.to_string()),
            rd,
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<P> Sync for FsLister<P> {}

impl oio::List for FsLister<tokio::fs::ReadDir> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        loop {
            // since list should return path itself, we return it first
            if let Some(path) = self.current_path.take() {
                let e = oio::Entry::new(path.as_str(), Metadata::new(EntryMode::DIR));
                return Ok(Some(e));
            }

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
            let rel_path = normalize_path(
                &entry_path
                    .strip_prefix(&self.root)
                    .expect("cannot fail because the prefix is iterated")
                    .to_string_lossy()
                    .replace('\\', "/"),
            );

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
            let metadata = Metadata::new(mode)
                .with_content_length(de_metadata.len())
                .with_last_modified(Timestamp::try_from(last_modified)?);

            return Ok(Some(oio::Entry::new(path, metadata)));
        }
    }
}
