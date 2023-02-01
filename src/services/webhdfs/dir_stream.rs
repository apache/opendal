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

use std::collections::VecDeque;

use async_trait::async_trait;
use log::debug;

use super::backend::FileStatus;
use crate::raw::ObjectEntry;
use crate::raw::ObjectPage;
use crate::ObjectMetadata;
use crate::Result;

pub(super) struct DirStream {
    path: String,
    statuses: Vec<FileStatus>,
}

impl DirStream {
    pub fn new(path: &str, statuses: Vec<FileStatus>) -> Self {
        Self {
            path: path.to_string(),
            statuses,
        }
    }
}

#[async_trait]
impl ObjectPage for DirStream {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        if self.statuses.is_empty() {
            return Ok(None);
        }
        let mut entries = VecDeque::new();
        while let Some(status) = self.statuses.pop() {
            let file_name = status.path_suffix.clone();
            let mut path = format!("{}/{}", &self.path, file_name);
            let meta: ObjectMetadata = status.try_into()?;
            if meta.mode().is_dir() {
                path = format!("{path}/");
            }
            let entry = ObjectEntry::new(&path, meta);
            entries.push_front(entry);
        }
        Ok(Some(entries.into()))
    }
}
