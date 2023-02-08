// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;

use super::error::parse_io_error;
use crate::raw::*;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Result;

pub struct DirPager {
    root: PathBuf,

    size: usize,
    rd: tokio::fs::ReadDir,
}

impl DirPager {
    pub fn new(root: &Path, rd: tokio::fs::ReadDir) -> Self {
        Self {
            root: root.to_owned(),
            // TODO: make this a configurable value.
            size: 256,
            rd,
        }
    }
}

#[async_trait]
impl ObjectPage for DirPager {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        let mut oes: Vec<ObjectEntry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next_entry().await.map_err(parse_io_error)? {
                Some(de) => de,
                None => break,
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
            let file_type = de.file_type().await.map_err(parse_io_error)?;

            let d = if file_type.is_file() {
                ObjectEntry::new(&rel_path, ObjectMetadata::new(ObjectMode::FILE))
            } else if file_type.is_dir() {
                // Make sure we are returning the correct path.
                ObjectEntry::new(
                    &format!("{rel_path}/"),
                    ObjectMetadata::new(ObjectMode::DIR).with_complete(),
                )
            } else {
                ObjectEntry::new(&rel_path, ObjectMetadata::new(ObjectMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}

pub struct BlockingDirPager {
    root: PathBuf,

    size: usize,
    rd: std::fs::ReadDir,
}

impl BlockingDirPager {
    pub fn new(root: &Path, rd: std::fs::ReadDir) -> Self {
        Self {
            root: root.to_owned(),
            // TODO: make this a configurable value.
            size: 256,
            rd,
        }
    }
}

impl BlockingObjectPage for BlockingDirPager {
    fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        let mut oes: Vec<ObjectEntry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de.map_err(parse_io_error)?,
                None => break,
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
            let file_type = de.file_type().map_err(parse_io_error)?;

            let d = if file_type.is_file() {
                ObjectEntry::new(&rel_path, ObjectMetadata::new(ObjectMode::FILE))
            } else if file_type.is_dir() {
                // Make sure we are returning the correct path.
                ObjectEntry::new(
                    &format!("{rel_path}/"),
                    ObjectMetadata::new(ObjectMode::DIR).with_complete(),
                )
            } else {
                ObjectEntry::new(&rel_path, ObjectMetadata::new(ObjectMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
