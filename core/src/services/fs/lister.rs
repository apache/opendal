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
use flagset::FlagSet;
use std::fmt::Debug;
use std::fs::FileType;
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

        let (metadata, is_dir) = if default_meta {
            let ft = de.file_type().await.map_err(new_std_io_error)?;
            get_metadata(self.op.metakey(), None, Some(ft))?
        } else {
            let fs_meta = de.metadata().await.map_err(new_std_io_error)?;
            get_metadata(self.op.metakey(), Some(fs_meta), None)?
        };

        let p = if is_dir {
            // Make sure we are returning the correct path.
            &format!("{rel_path}/")
        } else {
            &rel_path
        };

        Ok(Some(oio::Entry::new(&p, metadata)))
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

        let default_meta = self.op.metakey() == Metakey::Mode;

        let (metadata, is_dir) = if default_meta {
            let ft = de.file_type().map_err(new_std_io_error)?;
            get_metadata(self.op.metakey(), None, Some(ft))?
        } else {
            let fs_meta = de.metadata().map_err(new_std_io_error)?;
            get_metadata(self.op.metakey(), Some(fs_meta), None)?
        };

        let p = if is_dir {
            // Make sure we are returning the correct path.
            &format!("{rel_path}/")
        } else {
            &rel_path
        };

        Ok(Some(oio::Entry::new(&p, metadata)))
    }
}

fn get_metadata(
    metakey: FlagSet<Metakey>,
    fs_meta: Option<std::fs::Metadata>,
    file_type: Option<FileType>,
) -> Result<(Metadata, bool)> {
    let ft = if fs_meta.is_some() {
        fs_meta.as_ref().unwrap().file_type()
    } else if file_type.is_some() {
        file_type.unwrap()
    } else {
        panic!("At least one metadata should be provided!")
    };

    let mut meta = if ft.is_file() {
        Metadata::new(EntryMode::FILE)
    } else if ft.is_dir() {
        Metadata::new(EntryMode::DIR)
    } else {
        Metadata::new(EntryMode::Unknown)
    };

    if fs_meta.is_some() {
        let contains_metakey = |k| metakey.contains(k) || metakey.contains(Metakey::Complete);
        let fs_meta = fs_meta.unwrap();

        if contains_metakey(Metakey::ContentLength) {
            meta.set_content_length(fs_meta.len());
        }

        if contains_metakey(Metakey::LastModified) {
            meta.set_last_modified(fs_meta.modified().map_err(new_std_io_error)?.into());
        }
    }

    Ok((meta, ft.is_dir()))
}
