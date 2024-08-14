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
use crate::Metadata;
use crate::Result;
use crate::{EntryMode, Metakey};
use std::path::Path;
use std::path::PathBuf;

pub struct FsLister<P> {
    root: PathBuf,

    rd: P,

    op: OpList,
}

impl<P> FsLister<P> {
    pub fn new(root: &Path, rd: P, arg: OpList) -> Self {
        Self {
            root: root.to_owned(),
            rd,
            op: arg,
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<P> Sync for FsLister<P> {}

impl oio::List for FsLister<tokio::fs::ReadDir> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
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

        let default_meta = self.op.metakey() == Metakey::Mode;

        let metadata = if default_meta {
            let ft = de.file_type().await.map_err(new_std_io_error)?;
            if ft.is_file() {
                Metadata::new(EntryMode::FILE)
            } else if ft.is_dir() {
                Metadata::new(EntryMode::DIR)
            } else {
                Metadata::new(EntryMode::Unknown)
            }
        } else {
            let fs_meta = de.metadata().await.map_err(new_std_io_error)?;
            let mut meta = if fs_meta.file_type().is_file() {
                Metadata::new(EntryMode::FILE)
            } else if fs_meta.file_type().is_dir() {
                Metadata::new(EntryMode::DIR)
            } else {
                Metadata::new(EntryMode::Unknown)
            };
            meta.set_content_length(fs_meta.len());
            meta.set_last_modified(fs_meta.modified().map_err(new_std_io_error)?.into());
            meta
        };

        let p = if metadata.is_dir() {
            // Make sure we are returning the correct path.
            &format!("{rel_path}/")
        } else {
            &rel_path
        };

        Ok(Some(oio::Entry::new(p, metadata)))
    }
}

impl oio::BlockingList for FsLister<std::fs::ReadDir> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        let de = match self.rd.next() {
            Some(de) => de.map_err(new_std_io_error)?,
            None => return Ok(None),
        };

        let entry_path = de.path();
        let rel_path = normalize_path(
            &entry_path
                .strip_prefix(&self.root)
                .expect("cannot fail because the prefix is iterated")
                .to_string_lossy()
                .replace('\\', "/"),
        );

        let default_meta = self.op.metakey() == Metakey::Mode;

        let metadata = if default_meta {
            let ft = de.file_type().map_err(new_std_io_error)?;
            if ft.is_file() {
                Metadata::new(EntryMode::FILE)
            } else if ft.is_dir() {
                Metadata::new(EntryMode::DIR)
            } else {
                Metadata::new(EntryMode::Unknown)
            }
        } else {
            let fs_meta = de.metadata().map_err(new_std_io_error)?;
            let mut meta = if fs_meta.file_type().is_file() {
                Metadata::new(EntryMode::FILE)
            } else if fs_meta.file_type().is_dir() {
                Metadata::new(EntryMode::DIR)
            } else {
                Metadata::new(EntryMode::Unknown)
            };
            meta.set_content_length(fs_meta.len());
            meta.set_last_modified(fs_meta.modified().map_err(new_std_io_error)?.into());
            meta
        };

        let p = if metadata.is_dir() {
            // Make sure we are returning the correct path.
            &format!("{rel_path}/")
        } else {
            &rel_path
        };

        Ok(Some(oio::Entry::new(p, metadata)))
    }
}
