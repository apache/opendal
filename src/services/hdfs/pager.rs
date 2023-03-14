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

use async_trait::async_trait;

use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct HdfsPager {
    root: String,

    size: usize,
    rd: hdrs::Readdir,
}

impl HdfsPager {
    pub fn new(root: &str, rd: hdrs::Readdir, limit: Option<usize>) -> Self {
        Self {
            root: root.to_string(),

            size: limit.unwrap_or(1000),
            rd,
        }
    }
}

#[async_trait]
impl oio::Page for HdfsPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut oes: Vec<oio::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de,
                None => break,
            };

            let path = build_rel_path(&self.root, de.path());

            let d = if de.is_file() {
                let meta = Metadata::new(EntryMode::FILE)
                    .with_content_length(de.len())
                    .with_last_modified(time::OffsetDateTime::from(de.modified()));
                oio::Entry::new(&path, meta)
            } else if de.is_dir() {
                // Make sure we are returning the correct path.
                oio::Entry::new(&format!("{path}/"), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&path, Metadata::new(EntryMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}

impl oio::BlockingPage for HdfsPager {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut oes: Vec<oio::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de,
                None => break,
            };

            let path = build_rel_path(&self.root, de.path());

            let d = if de.is_file() {
                let meta = Metadata::new(EntryMode::FILE)
                    .with_content_length(de.len())
                    .with_last_modified(time::OffsetDateTime::from(de.modified()));
                oio::Entry::new(&path, meta)
            } else if de.is_dir() {
                // Make sure we are returning the correct path.
                oio::Entry::new(&format!("{path}/"), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&path, Metadata::new(EntryMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
