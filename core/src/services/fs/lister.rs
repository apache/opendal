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

use std::fs::FileType;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::FutureExt;

use crate::raw::oio::Entry;
use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct FsLister<P> {
    root: PathBuf,

    rd: P,
}

impl<P> FsLister<P> {
    pub fn new(root: &Path, rd: P) -> Self {
        Self {
            root: root.to_owned(),
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
        let Some(de) = self.rd.next_entry().await.map_err(new_std_io_error)? else {
            return Ok(None);
        };

        let ft = de.file_type().await.map_err(new_std_io_error)?;

        let entry_path = de.path();
        let rel_path = normalize_path(
            &entry_path
                .strip_prefix(&self.root)
                .expect("cannot fail because the prefix is iterated")
                .to_string_lossy()
                .replace('\\', "/"),
        );

        let d = if ft.is_file() {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
        } else if ft.is_dir() {
            // Make sure we are returning the correct path.
            oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
        } else {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
        };

        Ok(Some(d))
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

        // On Windows and most Unix platforms this function is free
        // (no extra system calls needed), but some Unix platforms may
        // require the equivalent call to symlink_metadata to learn about
        // the target file type.
        let file_type = de.file_type().map_err(new_std_io_error)?;

        let entry = if file_type.is_file() {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
        } else if file_type.is_dir() {
            // Make sure we are returning the correct path.
            oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
        } else {
            oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
        };

        Ok(Some(entry))
    }
}
