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

use opendal_core::raw::*;
use opendal_core::*;
use wasi::filesystem::preopens::get_directories;
use wasi::filesystem::types::{
    Descriptor, DescriptorFlags, DescriptorStat, DescriptorType, DirectoryEntryStream, OpenFlags,
    PathFlags,
};

use super::error::parse_wasi_error;

#[derive(Debug)]
pub struct WasiFsCore {
    /// The preopened directory descriptor we're operating within
    root_descriptor: Descriptor,
    /// The path within the preopened dir
    root_path: String,
}

impl WasiFsCore {
    pub fn new(root: &str) -> Result<Self> {
        let mut preopens = get_directories();

        if preopens.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "No preopened directories available from WASI runtime",
            ));
        }

        let (idx, preopen_path) = Self::find_preopened_dir_index(&preopens, root)?;
        let (descriptor, _) = preopens.swap_remove(idx);

        let root_path = if root.starts_with(&preopen_path) {
            root.strip_prefix(&preopen_path)
                .unwrap_or("")
                .trim_start_matches('/')
                .to_string()
        } else {
            String::new()
        };

        Ok(Self {
            root_descriptor: descriptor,
            root_path,
        })
    }

    fn find_preopened_dir_index(
        preopens: &[(Descriptor, String)],
        root: &str,
    ) -> Result<(usize, String)> {
        for (idx, (_, path)) in preopens.iter().enumerate() {
            if root.starts_with(path) || path == "/" || root == "/" || root.is_empty() {
                return Ok((idx, path.clone()));
            }
        }

        // Fall back to first preopened directory
        if preopens.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "No preopened directories",
            ));
        }

        Ok((0, preopens[0].1.clone()))
    }

    fn build_path(&self, path: &str) -> String {
        let path = path.trim_start_matches('/').trim_end_matches('/');
        if self.root_path.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.root_path.clone()
        } else {
            format!("{}/{}", self.root_path, path)
        }
    }

    pub fn stat(&self, path: &str) -> Result<Metadata> {
        let abs_path = self.build_path(path);

        let stat = if abs_path.is_empty() {
            self.root_descriptor.stat().map_err(parse_wasi_error)?
        } else {
            self.root_descriptor
                .stat_at(PathFlags::empty(), &abs_path)
                .map_err(parse_wasi_error)?
        };

        Ok(Self::convert_stat(stat))
    }

    fn convert_stat(stat: DescriptorStat) -> Metadata {
        let mode = match stat.type_ {
            DescriptorType::Directory => EntryMode::DIR,
            DescriptorType::RegularFile => EntryMode::FILE,
            _ => EntryMode::Unknown,
        };

        let mut metadata = Metadata::new(mode).with_content_length(stat.size);

        if let Some(mtime) = stat.data_modification_timestamp {
            // Convert WASI timestamp (seconds + nanoseconds) to Timestamp
            if let Ok(ts) = Timestamp::new(mtime.seconds as i64, mtime.nanoseconds as i32) {
                metadata = metadata.with_last_modified(ts);
            }
        }

        metadata
    }

    pub fn create_dir(&self, path: &str) -> Result<()> {
        let abs_path = self.build_path(path);
        self.root_descriptor
            .create_directory_at(&abs_path)
            .map_err(parse_wasi_error)
    }

    pub fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        desc_flags: DescriptorFlags,
    ) -> Result<Descriptor> {
        let abs_path = self.build_path(path);
        self.root_descriptor
            .open_at(PathFlags::empty(), &abs_path, flags, desc_flags)
            .map_err(parse_wasi_error)
    }

    pub fn read_dir(&self, path: &str) -> Result<DirectoryEntryStream> {
        let abs_path = self.build_path(path);

        let dir_desc = if abs_path.is_empty() {
            // For the root, we need to open the same directory again
            // since we can't clone the descriptor
            self.root_descriptor
                .read_directory()
                .map_err(parse_wasi_error)?;
            return self
                .root_descriptor
                .read_directory()
                .map_err(parse_wasi_error);
        } else {
            self.root_descriptor
                .open_at(
                    PathFlags::empty(),
                    &abs_path,
                    OpenFlags::DIRECTORY,
                    DescriptorFlags::empty(),
                )
                .map_err(parse_wasi_error)?
        };

        dir_desc.read_directory().map_err(parse_wasi_error)
    }

    pub fn delete_file(&self, path: &str) -> Result<()> {
        let abs_path = self.build_path(path);
        self.root_descriptor
            .unlink_file_at(&abs_path)
            .map_err(parse_wasi_error)
    }

    pub fn delete_dir(&self, path: &str) -> Result<()> {
        let abs_path = self.build_path(path);
        self.root_descriptor
            .remove_directory_at(&abs_path)
            .map_err(parse_wasi_error)
    }

    pub fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from_path = self.build_path(from);
        let to_path = self.build_path(to);
        self.root_descriptor
            .rename_at(&from_path, &self.root_descriptor, &to_path)
            .map_err(parse_wasi_error)
    }
}
