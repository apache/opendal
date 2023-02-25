// Copyright 2022 Datafuse Labs
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

use async_trait::async_trait;

use crate::raw::*;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Result;

pub struct DirStream {
    root: String,

    size: usize,
    rd: hdrs::Readdir,
}

impl DirStream {
    pub fn new(root: &str, rd: hdrs::Readdir, limit: Option<usize>) -> Self {
        Self {
            root: root.to_string(),

            size: limit.unwrap_or(1000),
            rd,
        }
    }
}

#[async_trait]
impl output::Page for DirStream {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let mut oes: Vec<output::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de,
                None => break,
            };

            let path = build_rel_path(&self.root, de.path());

            let d = if de.is_file() {
                let meta = ObjectMetadata::new(ObjectMode::FILE)
                    .with_content_length(de.len())
                    .with_last_modified(time::OffsetDateTime::from(de.modified()));
                output::Entry::new(&path, meta)
            } else if de.is_dir() {
                // Make sure we are returning the correct path.
                output::Entry::new(&format!("{path}/"), ObjectMetadata::new(ObjectMode::DIR))
            } else {
                output::Entry::new(&path, ObjectMetadata::new(ObjectMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}

impl output::BlockingPage for DirStream {
    fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let mut oes: Vec<output::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de,
                None => break,
            };

            let path = build_rel_path(&self.root, de.path());

            let d = if de.is_file() {
                let meta = ObjectMetadata::new(ObjectMode::FILE)
                    .with_content_length(de.len())
                    .with_last_modified(time::OffsetDateTime::from(de.modified()));
                output::Entry::new(&path, meta)
            } else if de.is_dir() {
                // Make sure we are returning the correct path.
                output::Entry::new(&format!("{path}/"), ObjectMetadata::new(ObjectMode::DIR))
            } else {
                output::Entry::new(&path, ObjectMetadata::new(ObjectMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
