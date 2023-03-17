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

use async_trait::async_trait;

use super::error::parse_io_error;
use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct FsPager<P> {
    root: PathBuf,

    size: usize,
    rd: P,
}

impl<P> FsPager<P> {
    pub fn new(root: &Path, rd: P, limit: Option<usize>) -> Self {
        Self {
            root: root.to_owned(),
            size: limit.unwrap_or(1000),
            rd,
        }
    }
}

#[async_trait]
impl oio::Page for FsPager<tokio::fs::ReadDir> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut oes: Vec<oio::Entry> = Vec::with_capacity(self.size);

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
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
            } else if file_type.is_dir() {
                // Make sure we are returning the correct path.
                oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}

impl oio::BlockingPage for FsPager<std::fs::ReadDir> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut oes: Vec<oio::Entry> = Vec::with_capacity(self.size);

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
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
            } else if file_type.is_dir() {
                // Make sure we are returning the correct path.
                oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
