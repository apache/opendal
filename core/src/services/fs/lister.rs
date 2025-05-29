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

use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;
use chrono::DateTime;
use std::path::Path;
use std::path::PathBuf;

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
        // since list should return path itself, we return it first
        if let Some(path) = self.current_path.take() {
            let meta = tokio::fs::metadata(self.root.join(&path))
                .await
                .map_err(new_std_io_error)?;
            let m = Metadata::new(EntryMode::DIR)
                .with_content_length(meta.len())
                .with_last_modified(
                    meta.modified()
                        .map(DateTime::from)
                        .map_err(new_std_io_error)?,
                );
            let e = oio::Entry::new(&path, m);
            return Ok(Some(e));
        }

        let Some(de) = self.rd.next_entry().await.map_err(new_std_io_error)? else {
            return Ok(None);
        };

        let entry_path = de.path();
        let rel_path = normalize_path(
            &entry_path
                .strip_prefix(&self.root)
                .expect("cannot fail because the prefix is iterated")
                .to_string_lossy()
                .replace('\\', "/"),
        );

        let ft = de.file_type().await.map_err(new_std_io_error)?;
        let (path, mode) = if ft.is_dir() {
            // Make sure we are returning the correct path.
            (&format!("{rel_path}/"), EntryMode::DIR)
        } else if ft.is_file() {
            (&rel_path, EntryMode::FILE)
        } else {
            (&rel_path, EntryMode::Unknown)
        };

        let meta = tokio::fs::metadata(self.root.join(&path))
            .await
            .map_err(new_std_io_error)?;
        let m = Metadata::new(mode)
            .with_content_length(meta.len())
            .with_last_modified(
                meta.modified()
                    .map(DateTime::from)
                    .map_err(new_std_io_error)?,
            );
        let entry = oio::Entry::new(path, m);
        Ok(Some(entry))
    }
}
