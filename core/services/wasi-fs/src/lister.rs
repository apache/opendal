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

use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;
use wasi::filesystem::types::{DescriptorType, DirectoryEntryStream};

use super::core::WasiFsCore;
use super::error::parse_wasi_error;

pub struct WasiFsLister {
    stream: DirectoryEntryStream,
    path: String,
    returned_self: bool,
}

impl WasiFsLister {
    pub fn new(core: Arc<WasiFsCore>, path: &str) -> Result<Self> {
        let stream = core.read_dir(path)?;

        Ok(Self {
            stream,
            path: path.to_string(),
            returned_self: false,
        })
    }
}

/// # Safety
///
/// WasiFsLister only accesses WASI resources which are single-threaded in WASM.
unsafe impl Sync for WasiFsLister {}

impl oio::List for WasiFsLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        if !self.returned_self {
            self.returned_self = true;
            let entry = oio::Entry::new(&self.path, Metadata::new(EntryMode::DIR));
            return Ok(Some(entry));
        }

        // Use a loop to skip . and .. entries instead of recursion
        loop {
            match self.stream.read_directory_entry() {
                Ok(Some(entry)) => {
                    let name = entry.name;

                    // Skip . and .. entries
                    if name == "." || name == ".." {
                        continue;
                    }

                    let mode = match entry.type_ {
                        DescriptorType::Directory => EntryMode::DIR,
                        DescriptorType::RegularFile => EntryMode::FILE,
                        _ => EntryMode::Unknown,
                    };

                    let path = if self.path.ends_with('/') || self.path.is_empty() {
                        format!("{}{}", self.path, name)
                    } else {
                        format!("{}/{}", self.path, name)
                    };

                    let path = if mode == EntryMode::DIR {
                        format!("{}/", path)
                    } else {
                        path
                    };

                    return Ok(Some(oio::Entry::new(&path, Metadata::new(mode))));
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(parse_wasi_error(e)),
            }
        }
    }
}
