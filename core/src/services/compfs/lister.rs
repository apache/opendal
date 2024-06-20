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

use std::{fs::ReadDir, path::Path, sync::Arc};

use super::core::CompfsCore;
use crate::raw::*;
use crate::*;

#[derive(Debug)]
pub struct CompfsLister {
    core: Arc<CompfsCore>,
    read_dir: Option<ReadDir>,
}

impl CompfsLister {
    pub(super) fn new(core: Arc<CompfsCore>, read_dir: ReadDir) -> Self {
        Self {
            core,
            read_dir: Some(read_dir),
        }
    }
}

fn next_entry(read_dir: &mut ReadDir, root: &Path) -> std::io::Result<Option<oio::Entry>> {
    let Some(entry) = read_dir.next().transpose()? else {
        return Ok(None);
    };
    let path = entry.path();
    let rel_path = normalize_path(
        &path
            .strip_prefix(root)
            .expect("cannot fail because the prefix is iterated")
            .to_string_lossy()
            .replace('\\', "/"),
    );

    let file_type = entry.file_type()?;

    let entry = if file_type.is_file() {
        oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
    } else if file_type.is_dir() {
        oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
    } else {
        oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
    };

    Ok(Some(entry))
}

impl oio::List for CompfsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let Some(mut read_dir) = self.read_dir.take() else {
            return Ok(None);
        };
        let root = self.core.root.clone();
        let (entry, read_dir) = self
            .core
            .exec_blocking(move || {
                let entry = next_entry(&mut read_dir, &root).map_err(new_std_io_error);
                (entry, read_dir)
            })
            .await?;
        self.read_dir = Some(read_dir);
        entry
    }
}
